//! Integrated authentication, as the .NET client's `UseDefaultCredentials` and `Credentials`:
//! HTTP `Negotiate` (Kerberos or NTLM) and `NTLM` challenge-response on negotiate, the HTTP
//! transports and the WebSocket upgrade, following `SocketsHttpHandler`
//! (`AuthenticationHelper.NtAuth`):
//!
//! - a request goes out without credentials; on a 401 offering `Negotiate` (preferred) or `NTLM`
//!   it is sent again with `Authorization: <scheme> <token>` until the server stops challenging,
//!   on the same connection, or on a new one when the server closes it (`Connection: close`); a
//!   challenge whose body is too large to drain fails the request, as the connection cannot be
//!   reused;
//! - a token the server sends with its final, successful response (Kerberos mutual
//!   authentication) is validated, and a server that fails it fails the request with 401;
//! - a security context that fails while answering a challenge ends the exchange with that 401;
//! - every response's cookies are kept, and every leg sends the current ones;
//! - over TLS the tokens are bound to the server's certificate (RFC 5929 `tls-server-end-point`),
//!   for servers that require Extended Protection.
//!
//! Windows authenticates through SSPI; other native targets answer with NTLMv2 for explicit
//! credentials.

// Header and certificate parsing must not panic on server input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub(crate) mod ntlm;
#[cfg(windows)]
pub(crate) mod sspi;

use std::{future::Future, sync::Arc};

use base64::{engine::general_purpose, Engine};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use log::{debug, warn};

use crate::error::Error;

/// How many challenge legs one request may take (NTLM takes two, Kerberos one).
const MAX_LEGS: usize = 8;

/// The most of a challenge response's body drained to keep its connection, as
/// `SocketsHttpHandler.MaxResponseDrainSize` (1 MiB); a larger one fails the request.
const MAX_DRAIN: usize = 1024 * 1024;

/// The HTTP authentication schemes of integrated authentication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthScheme {
    /// SPNEGO: Kerberos, or NTLM when Kerberos is unavailable.
    Negotiate,
    Ntlm,
}

impl AuthScheme {
    fn name(self) -> &'static str {
        match self {
            AuthScheme::Negotiate => "Negotiate",
            AuthScheme::Ntlm => "NTLM",
        }
    }
}

/// Explicit credentials (`with_credentials`). `Debug` never shows the password.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Explicit {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) domain: String,
}

impl std::fmt::Debug for Explicit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Explicit")
            .field("user", &self.user)
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

/// Which credentials integrated authentication uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IntegratedCredentials {
    /// The current account (`with_default_credentials`).
    Default,
    /// `with_credentials`.
    Explicit(Explicit),
}

/// One authentication exchange: each step turns the server's challenge (none at first) into the
/// next token, or `None` once nothing more is needed. Steps may block (a Kerberos ticket request),
/// so they run on a blocking thread.
pub(crate) trait SecurityContext: Send {
    fn step(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error>;
}

/// Starts authentication exchanges for one client. `binding` is the connection's RFC 5929
/// `tls-server-end-point` channel binding (TLS only). Creating a context may block (a DNS
/// lookup of the canonical host name), so it runs on a blocking thread.
pub(crate) trait SecurityProvider: Send + Sync + std::fmt::Debug {
    fn context(
        &self,
        scheme: AuthScheme,
        host: &str,
        binding: Option<&[u8]>,
    ) -> Result<Box<dyn SecurityContext>, Error>;
}

/// SSPI with the current account or explicit credentials (Windows).
#[cfg(windows)]
#[derive(Debug)]
struct SspiProvider(IntegratedCredentials);

#[cfg(windows)]
impl SecurityProvider for SspiProvider {
    fn context(
        &self,
        scheme: AuthScheme,
        host: &str,
        binding: Option<&[u8]>,
    ) -> Result<Box<dyn SecurityContext>, Error> {
        let explicit = match &self.0 {
            IntegratedCredentials::Default => None,
            IntegratedCredentials::Explicit(explicit) => Some(explicit),
        };
        Ok(Box::new(sspi::SspiContext::new(
            scheme,
            &sspi::canonical_host(host),
            explicit,
            binding,
        )?))
    }
}

/// NTLMv2 with explicit credentials (other native targets). Both schemes carry raw NTLM tokens,
/// which `Negotiate` acceptors take.
#[cfg_attr(windows, allow(dead_code))]
#[derive(Debug)]
struct NtlmProvider(Explicit);

impl SecurityProvider for NtlmProvider {
    fn context(
        &self,
        _scheme: AuthScheme,
        host: &str,
        binding: Option<&[u8]>,
    ) -> Result<Box<dyn SecurityContext>, Error> {
        Ok(Box::new(ntlm::NtlmContext::new(
            self.0.clone(),
            format!("HTTP/{host}"),
            binding.map(<[u8]>::to_vec),
        )))
    }
}

/// The provider for `credentials` on this platform. Without SSPI the current account's
/// credentials are not available, which fails `new` with `Configuration`.
pub(crate) fn provider(
    credentials: &IntegratedCredentials,
) -> Result<Arc<dyn SecurityProvider>, Error> {
    #[cfg(windows)]
    {
        Ok(Arc::new(SspiProvider(credentials.clone())))
    }
    #[cfg(not(windows))]
    match credentials {
        IntegratedCredentials::Explicit(explicit) => Ok(Arc::new(NtlmProvider(explicit.clone()))),
        IntegratedCredentials::Default => Err(Error::configuration(
            "with_default_credentials",
            "the current account's credentials are available only on Windows (SSPI); use \
             with_credentials(user, password, domain) for NTLM",
        )),
    }
}

/// The integrated-authentication schemes a response offers, with their tokens.
pub(crate) fn offered(headers: &HeaderMap) -> Vec<(AuthScheme, Option<Vec<u8>>)> {
    headers
        .get_all(http::header::WWW_AUTHENTICATE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|challenge| {
            let mut parts = challenge.trim().splitn(2, ' ');
            let scheme = match parts.next()?.to_ascii_lowercase().as_str() {
                "negotiate" => AuthScheme::Negotiate,
                "ntlm" => AuthScheme::Ntlm,
                _ => return None,
            };
            let token = parts
                .next()
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .and_then(|token| general_purpose::STANDARD.decode(token).ok());
            Some((scheme, token))
        })
        .collect()
}

/// The token `scheme` carries in `headers`, if any.
fn scheme_token(headers: &HeaderMap, scheme: AuthScheme) -> Option<Vec<u8>> {
    offered(headers)
        .into_iter()
        .find(|(offer, _)| *offer == scheme)
        .and_then(|(_, token)| token)
}

/// The RFC 5929 `tls-server-end-point` channel binding of a TLS connection to the server whose
/// certificate is `certificate` (DER): the application data of the channel bindings. The hash is
/// the certificate's signature hash, SHA-256 for MD5, SHA-1 and anything else.
#[cfg_attr(not(rayx_tls), allow(dead_code))]
pub(crate) fn tls_server_end_point(certificate: &[u8]) -> Vec<u8> {
    use sha2::Digest;

    // DER of ecdsa-with-SHA384/512 and sha384/512WithRSAEncryption.
    const SHA384: [&[u8]; 2] = [
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c],
    ];
    const SHA512: [&[u8]; 2] = [
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d],
    ];
    let algorithm = signature_algorithm(certificate);
    let hash: Vec<u8> = match algorithm {
        Some(oid) if SHA384.contains(&oid) => sha2::Sha384::digest(certificate).to_vec(),
        Some(oid) if SHA512.contains(&oid) => sha2::Sha512::digest(certificate).to_vec(),
        _ => sha2::Sha256::digest(certificate).to_vec(),
    };
    [b"tls-server-end-point:".as_slice(), &hash].concat()
}

/// One DER element of `bytes`: (tag, content, rest).
#[cfg_attr(not(rayx_tls), allow(dead_code))]
fn der_element(bytes: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = bytes.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (length, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 {
            return None;
        }
        let (length_bytes, rest) = (rest.get(..count)?, rest.get(count..)?);
        let length = length_bytes
            .iter()
            .fold(0usize, |length, byte| (length << 8) | usize::from(*byte));
        (length, rest)
    };
    Some((tag, rest.get(..length)?, rest.get(length..)?))
}

/// The OID (DER content) of an X.509 certificate's `signatureAlgorithm`.
#[cfg_attr(not(rayx_tls), allow(dead_code))]
fn signature_algorithm(certificate: &[u8]) -> Option<&[u8]> {
    let (_, certificate, _) = der_element(certificate)?;
    let (_, _, rest) = der_element(certificate)?; // tbsCertificate
    let (_, algorithm, _) = der_element(rest)?;
    let (tag, oid, _) = der_element(algorithm)?;
    (tag == 0x06).then_some(oid)
}

/// Why an authenticated exchange failed: the connection (as the caller maps its errors), opening
/// a new connection for a later leg, or the authentication itself.
#[derive(Debug)]
pub(crate) enum ExchangeFailure {
    Http(hyper::Error),
    Connect(Error),
    Authentication(Error),
}

impl From<Error> for ExchangeFailure {
    fn from(error: Error) -> Self {
        ExchangeFailure::Authentication(error)
    }
}

/// A connection a request goes out on: hyper's sender, and its channel binding (TLS only).
pub(crate) struct Connection<B> {
    pub(crate) sender: SendRequest<B>,
    pub(crate) binding: Option<Vec<u8>>,
}

/// Runs `work` on a blocking thread: SSPI calls may wait on the network (a Kerberos ticket).
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        Error::authentication(None, format!("integrated authentication stopped: {error}"))
    })
}

/// Reads a challenge's body so its connection can carry the next leg; `false` when it is larger
/// than [`MAX_DRAIN`] or the read failed.
async fn drained(response: Response<Incoming>) -> bool {
    Limited::new(response.into_body(), MAX_DRAIN)
        .collect()
        .await
        .is_ok()
}

/// Whether the server closes the connection after `response`.
fn closes(response: &Response<Incoming>) -> bool {
    response
        .headers()
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("close"))
}

/// Sends the request `build` makes on `connection`, answering integrated-authentication
/// challenges when `provider` is set. `build` receives the `Authorization` value of a challenge
/// leg (`None` for the first request) and is called for every leg, so each leg carries the current
/// cookies. `observe` sees every response's headers (its cookies). `reconnect` opens a new
/// connection when the server closed the current one after a challenge. The response returned is
/// the first that is not an integrated-authentication challenge, or the 401 the exchange ended
/// with.
pub(crate) async fn send<B, R, F>(
    connection: &mut Connection<B>,
    provider: Option<&Arc<dyn SecurityProvider>>,
    host: &str,
    mut reconnect: R,
    mut observe: impl FnMut(&HeaderMap),
    mut build: impl FnMut(Option<String>) -> Result<Request<B>, Error>,
) -> Result<Response<Incoming>, ExchangeFailure>
where
    B: hyper::body::Body + 'static,
    R: FnMut() -> F,
    F: Future<Output = Result<Connection<B>, Error>>,
{
    let response = connection
        .sender
        .send_request(build(None)?)
        .await
        .map_err(ExchangeFailure::Http)?;
    observe(response.headers());
    let Some(provider) = provider else {
        return Ok(response);
    };
    if response.status() != StatusCode::UNAUTHORIZED {
        return Ok(response);
    }
    let offers = offered(response.headers());
    let Some(scheme) = [AuthScheme::Negotiate, AuthScheme::Ntlm]
        .into_iter()
        .find(|scheme| offers.iter().any(|(offer, _)| offer == scheme))
    else {
        return Ok(response);
    };
    let (provider, target, binding) = (
        provider.clone(),
        host.to_string(),
        connection.binding.clone(),
    );
    let mut context =
        match blocking(move || provider.context(scheme, &target, binding.as_deref())).await? {
            Ok(context) => context,
            Err(error) => {
                // As `SocketsHttpHandler`, a context that cannot start ends the exchange with
                // the 401.
                warn!("Integrated authentication could not start: {error}");
                return Ok(response);
            }
        };
    let mut challenge: Option<Vec<u8>> = None;
    let mut response = response;
    for _ in 0..MAX_LEGS {
        let (returned, outcome) = blocking(move || {
            let outcome = context.step(challenge.as_deref());
            (context, outcome)
        })
        .await?;
        context = returned;
        let token = match outcome {
            Ok(Some(token)) => token,
            Ok(None) => return Ok(response),
            Err(error) => {
                warn!("Integrated authentication failed to answer the challenge: {error}");
                return Ok(response);
            }
        };
        // The next leg goes out on a new connection when the server closes this one, else on this
        // one once the challenge's body is read; a body too large to drain fails the request, as
        // `SocketsHttpHandler` does.
        if closes(&response) {
            debug!("Continuing integrated authentication on a new connection");
            *connection = reconnect().await.map_err(ExchangeFailure::Connect)?;
        } else if !drained(response).await {
            return Err(ExchangeFailure::Authentication(Error::authentication(
                None,
                "integrated authentication failed because the connection could not be reused",
            )));
        } else {
            connection
                .sender
                .ready()
                .await
                .map_err(ExchangeFailure::Http)?;
        }
        let authorization = format!(
            "{} {}",
            scheme.name(),
            general_purpose::STANDARD.encode(token)
        );
        response = connection
            .sender
            .send_request(build(Some(authorization))?)
            .await
            .map_err(ExchangeFailure::Http)?;
        observe(response.headers());
        if response.status() != StatusCode::UNAUTHORIZED {
            // Kerberos mutual authentication: the success carries the server's final token.
            if let Some(tail) = scheme_token(response.headers(), scheme) {
                let (_, outcome) = blocking(move || {
                    let outcome = context.step(Some(&tail));
                    (context, outcome)
                })
                .await?;
                if let Err(error) = outcome {
                    return Err(ExchangeFailure::Authentication(Error::authentication(
                        Some(401),
                        format!("the server's integrated-authentication response could not be verified: {error}"),
                    )));
                }
            }
            return Ok(response);
        }
        challenge = scheme_token(response.headers(), scheme);
        if challenge.is_none() {
            // The server rejected the credentials: the 401 reaches the caller.
            return Ok(response);
        }
    }
    Ok(response)
}

#[cfg(test)]
mod integrated_auth_exchange {
    use std::sync::{Arc, Mutex};

    use super::{offered, AuthScheme, Connection, SecurityContext, SecurityProvider};

    #[test]
    fn integrated_auth_reads_the_offered_schemes_and_tokens() {
        let mut headers = http::HeaderMap::new();
        headers.append(http::header::WWW_AUTHENTICATE, "Negotiate".parse().unwrap());
        headers.append(
            http::header::WWW_AUTHENTICATE,
            "NTLM dGVzdA==".parse().unwrap(),
        );
        headers.append(
            http::header::WWW_AUTHENTICATE,
            "Bearer realm=\"x\"".parse().unwrap(),
        );
        assert_eq!(
            offered(&headers),
            vec![
                (AuthScheme::Negotiate, None),
                (AuthScheme::Ntlm, Some(b"test".to_vec()))
            ]
        );
    }

    /// How a scripted context behaves.
    #[derive(Clone, Copy, Debug, Default)]
    enum Script {
        /// Answers each challenge with a numbered token, `legs` times.
        #[default]
        Tokens,
        /// Fails on the second step (the first challenge).
        FailOnChallenge,
        /// Rejects the server's final token.
        RejectTail,
    }

    /// A provider that records the challenges and bindings it saw.
    #[derive(Debug, Default)]
    struct ScriptedProvider {
        challenges: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
        bindings: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
        legs: usize,
        script: Script,
    }

    struct ScriptedContext {
        challenges: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
        remaining: usize,
        script: Script,
    }

    impl SecurityContext for ScriptedContext {
        fn step(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>, crate::Error> {
            self.challenges
                .lock()
                .unwrap()
                .push(challenge.map(<[u8]>::to_vec));
            match self.script {
                Script::FailOnChallenge if challenge.is_some() => {
                    return Err(crate::Error::authentication(None, "context failed"))
                }
                Script::RejectTail if challenge == Some(b"tail") => {
                    return Err(crate::Error::authentication(None, "bad server token"))
                }
                _ => {}
            }
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            Ok(Some(format!("token-{}", self.remaining).into_bytes()))
        }
    }

    impl SecurityProvider for ScriptedProvider {
        fn context(
            &self,
            _scheme: AuthScheme,
            _host: &str,
            binding: Option<&[u8]>,
        ) -> Result<Box<dyn SecurityContext>, crate::Error> {
            self.bindings
                .lock()
                .unwrap()
                .push(binding.map(<[u8]>::to_vec));
            Ok(Box::new(ScriptedContext {
                challenges: self.challenges.clone(),
                remaining: self.legs,
                script: self.script,
            }))
        }
    }

    /// One scripted response: status, extra header lines, and whether the server then closes.
    type Reply = (u16, &'static str, bool);

    /// A loopback HTTP/1.1 server answering requests by script across connections; records each
    /// request's `Authorization` and `Cookie` headers and the connection it came on.
    async fn scripted_server(
        script: Vec<Reply>,
    ) -> (
        u16,
        tokio::task::JoinHandle<Vec<(usize, Option<String>, Option<String>)>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let mut seen = Vec::new();
            let mut replies = script.into_iter();
            let mut connection = 0;
            'connections: while let Ok((mut stream, _)) = listener.accept().await {
                connection += 1;
                loop {
                    let mut head = Vec::new();
                    while !head.ends_with(b"\r\n\r\n") {
                        let mut byte = [0u8; 1];
                        if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                            continue 'connections;
                        }
                        head.push(byte[0]);
                    }
                    let text = String::from_utf8(head).unwrap();
                    let header = |name: &str| {
                        text.lines()
                            .find_map(|line| line.strip_prefix(&format!("{name}: ")))
                            .map(str::to_string)
                    };
                    seen.push((connection, header("authorization"), header("cookie")));
                    let Some((status, headers, close)) = replies.next() else {
                        return seen;
                    };
                    let connection_header = if close { "Connection: close\r\n" } else { "" };
                    // `X-Large` asks for a body larger than the drain limit.
                    let body = if headers.contains("X-Large") {
                        "x".repeat(super::MAX_DRAIN + 1)
                    } else {
                        "ok".to_string()
                    };
                    let response = format!(
                        "HTTP/1.1 {status} X\r\n{headers}{connection_header}Content-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if stream.write_all(response.as_bytes()).await.is_err() {
                        return seen;
                    }
                    if close {
                        let _ = stream.shutdown().await;
                        continue 'connections;
                    }
                    if replies.len() == 0 {
                        return seen;
                    }
                }
            }
            seen
        });
        (port, task)
    }

    async fn connect(port: u16) -> Connection<http_body_util::Empty<bytes::Bytes>> {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let (sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
                .await
                .unwrap();
        tokio::spawn(connection);
        Connection {
            sender,
            binding: Some(b"tls-server-end-point:binding".to_vec()),
        }
    }

    async fn exchange(
        port: u16,
        provider: Option<Arc<dyn SecurityProvider>>,
        jar: &crate::CookieJar,
    ) -> Result<http::StatusCode, super::ExchangeFailure> {
        use http_body_util::Empty;
        let url = format!("http://127.0.0.1:{port}/hub");
        let mut connection = connect(port).await;
        let response = super::send(
            &mut connection,
            provider.as_ref(),
            "127.0.0.1",
            || connect(port).map(Ok),
            |headers| jar.store_response(&url, headers),
            |authorization| {
                let mut request =
                    http::Request::get("/hub").header(http::header::HOST, "127.0.0.1");
                if let Some(cookies) = jar.cookie_header(&url) {
                    request = request.header(http::header::COOKIE, cookies);
                }
                if let Some(authorization) = authorization {
                    request = request.header(http::header::AUTHORIZATION, authorization);
                }
                Ok(request.body(Empty::<bytes::Bytes>::new()).unwrap())
            },
        )
        .await?;
        Ok(response.status())
    }

    use futures::FutureExt;

    fn provider(legs: usize, script: Script) -> Arc<ScriptedProvider> {
        Arc::new(ScriptedProvider {
            legs,
            script,
            ..ScriptedProvider::default()
        })
    }

    #[tokio::test]
    async fn integrated_auth_answers_a_two_leg_challenge_on_one_connection_with_cookies() {
        let (port, server) = scripted_server(vec![
            (
                401,
                "WWW-Authenticate: Negotiate\r\nSet-Cookie: affinity=1; Path=/\r\n",
                false,
            ),
            (401, "WWW-Authenticate: Negotiate Y2hhbGxlbmdl\r\n", false),
            (200, "", false),
        ])
        .await;
        let scripted = provider(2, Script::Tokens);
        let (challenges, bindings) = (scripted.challenges.clone(), scripted.bindings.clone());
        let jar = crate::CookieJar::new();
        assert_eq!(exchange(port, Some(scripted), &jar).await.unwrap(), 200);
        assert_eq!(
            server.await.unwrap(),
            vec![
                (1, None, None),
                (
                    1,
                    Some("Negotiate dG9rZW4tMQ==".to_string()),
                    Some("affinity=1".to_string())
                ),
                (
                    1,
                    Some("Negotiate dG9rZW4tMA==".to_string()),
                    Some("affinity=1".to_string())
                ),
            ],
            "no credentials first, then each token with the cookie the 401 set, on one connection"
        );
        assert_eq!(
            *challenges.lock().unwrap(),
            vec![None, Some(b"challenge".to_vec())]
        );
        assert_eq!(
            *bindings.lock().unwrap(),
            vec![Some(b"tls-server-end-point:binding".to_vec())],
            "the context is bound to the connection's channel"
        );
    }

    #[tokio::test]
    async fn integrated_auth_continues_on_a_new_connection_when_the_server_closes() {
        let (port, server) = scripted_server(vec![
            (401, "WWW-Authenticate: Negotiate\r\n", true),
            (200, "", false),
        ])
        .await;
        let jar = crate::CookieJar::new();
        assert_eq!(
            exchange(port, Some(provider(1, Script::Tokens)), &jar)
                .await
                .unwrap(),
            200
        );
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, 1);
        assert_eq!(seen[1].0, 2, "the next leg went out on a new connection");
        assert!(seen[1].1.is_some());
    }

    #[tokio::test]
    async fn integrated_auth_validates_the_servers_final_token() {
        // Accepted tail.
        let (port, _server) = scripted_server(vec![
            (401, "WWW-Authenticate: Negotiate\r\n", false),
            (200, "WWW-Authenticate: Negotiate dGFpbA==\r\n", false),
        ])
        .await;
        let scripted = provider(1, Script::Tokens);
        let challenges = scripted.challenges.clone();
        let jar = crate::CookieJar::new();
        assert_eq!(exchange(port, Some(scripted), &jar).await.unwrap(), 200);
        assert_eq!(
            challenges.lock().unwrap().last().unwrap().as_deref(),
            Some(&b"tail"[..])
        );

        // A tail the context rejects fails the request with 401.
        let (port, _server) = scripted_server(vec![
            (401, "WWW-Authenticate: Negotiate\r\n", false),
            (200, "WWW-Authenticate: Negotiate dGFpbA==\r\n", false),
        ])
        .await;
        let failure = exchange(port, Some(provider(1, Script::RejectTail)), &jar)
            .await
            .unwrap_err();
        let super::ExchangeFailure::Authentication(error) = failure else {
            panic!("an authentication failure");
        };
        assert_eq!(error.kind(), crate::ErrorKind::Authentication);
        assert_eq!(error.status(), Some(401));
    }

    #[tokio::test]
    async fn integrated_auth_returns_the_401_on_rejection_or_context_failure() {
        let jar = crate::CookieJar::new();
        // The server rejects the credentials: the final 401 reaches the caller.
        let (port, server) = scripted_server(vec![
            (401, "WWW-Authenticate: NTLM\r\n", false),
            (401, "WWW-Authenticate: NTLM\r\n", false),
        ])
        .await;
        assert_eq!(
            exchange(port, Some(provider(2, Script::Tokens)), &jar)
                .await
                .unwrap(),
            401
        );
        assert_eq!(server.await.unwrap().len(), 2);

        // The context fails on the challenge: the challenge's 401 reaches the caller.
        let (port, server) = scripted_server(vec![
            (401, "WWW-Authenticate: Negotiate\r\n", false),
            (401, "WWW-Authenticate: Negotiate Y2g=\r\n", false),
        ])
        .await;
        assert_eq!(
            exchange(port, Some(provider(2, Script::FailOnChallenge)), &jar)
                .await
                .unwrap(),
            401
        );
        assert_eq!(server.await.unwrap().len(), 2);

        // A challenge too large to drain fails the request: the connection cannot be reused.
        let (port, server) = scripted_server(vec![(
            401,
            "WWW-Authenticate: Negotiate\r\nX-Large: 1\r\n",
            false,
        )])
        .await;
        let failure = exchange(port, Some(provider(2, Script::Tokens)), &jar)
            .await
            .unwrap_err();
        let super::ExchangeFailure::Authentication(error) = failure else {
            panic!("an authentication failure");
        };
        assert!(error.to_string().contains("could not be reused"), "{error}");
        assert_eq!(server.await.unwrap().len(), 1);

        // A Bearer challenge is not integrated authentication: nothing is resent.
        let (port, server) =
            scripted_server(vec![(401, "WWW-Authenticate: Bearer\r\n", false)]).await;
        let scripted = provider(2, Script::Tokens);
        let challenges = scripted.challenges.clone();
        assert_eq!(exchange(port, Some(scripted), &jar).await.unwrap(), 401);
        assert_eq!(server.await.unwrap().len(), 1);
        assert!(challenges.lock().unwrap().is_empty());

        // Without integrated credentials a challenge is not answered.
        let (port, server) =
            scripted_server(vec![(401, "WWW-Authenticate: Negotiate\r\n", false)]).await;
        assert_eq!(exchange(port, None, &jar).await.unwrap(), 401);
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[test]
    fn integrated_auth_binds_to_the_certificate_with_its_signature_hash() {
        use sha2::Digest;
        for (algorithm, expected) in [
            (&rcgen::PKCS_ECDSA_P256_SHA256, 32),
            (&rcgen::PKCS_ECDSA_P384_SHA384, 48),
        ] {
            let key = rcgen::KeyPair::generate_for(algorithm).unwrap();
            let certificate = rcgen::CertificateParams::new(vec!["localhost".to_string()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
            let der = certificate.der().to_vec();
            let binding = super::tls_server_end_point(&der);
            assert!(binding.starts_with(b"tls-server-end-point:"));
            let hash = &binding[b"tls-server-end-point:".len()..];
            assert_eq!(hash.len(), expected);
            let digest = if expected == 48 {
                sha2::Sha384::digest(&der).to_vec()
            } else {
                sha2::Sha256::digest(&der).to_vec()
            };
            assert_eq!(hash, digest.as_slice());
        }
        // Not a certificate: SHA-256 of the bytes.
        assert_eq!(
            super::tls_server_end_point(b"junk").len(),
            b"tls-server-end-point:".len() + 32
        );
    }

    #[test]
    fn integrated_auth_debug_never_shows_the_password() {
        let explicit = super::Explicit {
            user: "user".into(),
            password: "s3cret".into(),
            domain: "CORP".into(),
        };
        let debug = format!("{:?}", super::IntegratedCredentials::Explicit(explicit));
        assert!(!debug.contains("s3cret"), "{debug}");
    }

    #[cfg(not(windows))]
    #[test]
    fn integrated_auth_default_credentials_need_windows() {
        let error = super::provider(&super::IntegratedCredentials::Default).unwrap_err();
        assert_eq!(error.setting(), Some("with_default_credentials"));
    }
}
