use std::time::Duration;

use crate::client::DisconnectionHandler;
use crate::communication::liveness::LivenessConfig;
use crate::communication::reconnection::ReconnectionConfig;
use crate::protocol::hub_protocol::HubProtocolKind;
use crate::StatefulReconnectConfig;

/// Returned by native `connect` when neither a runtime handle nor an ambient runtime exists.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) const NO_RUNTIME: &str = "rayx-signalr needs a Tokio runtime for native connections: \
call connect from within a Tokio runtime, or pass one with \
ConnectionConfiguration::with_runtime_handle(tokio::runtime::Handle)";

/// The configuration error of a native connection without a Tokio runtime.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn no_runtime() -> crate::Error {
    crate::Error::configuration("with_runtime_handle", NO_RUNTIME)
}

#[derive(Clone)]
pub(crate) enum Authentication {
    None,
    Basic {
        user: String,
        password: Option<String>,
    },
    Bearer {
        token: String,
    },
    /// `with_access_token_factory`: asked for a fresh Bearer token per request (`resolve`).
    Factory(AccessTokenFactory),
}

/// Logged when a browser client is configured with Basic credentials: as with the .NET client,
/// they go with every `fetch` request (negotiate, Long Polling, Server-Sent Events sends), but a
/// browser WebSocket or `EventSource` cannot carry an `Authorization` header.
pub(crate) const BROWSER_BASIC_WARNING: &str = "Basic credentials are sent only with fetch \
requests (negotiate, Long Polling, Server-Sent Events sends): browsers cannot set the \
Authorization header of a WebSocket or an EventSource, so the server must authenticate those \
otherwise; use authenticate_bearer or with_access_token_factory, which browsers send there as \
access_token";

/// Logs [`BROWSER_BASIC_WARNING`] when a browser (`browser`) is configured with Basic credentials;
/// shared by both targets so the decision is tested natively. Returns whether it warned.
pub(crate) fn warn_browser_basic(authentication: &Authentication, browser: bool) -> bool {
    let warn = browser && matches!(authentication, Authentication::Basic { .. });
    if warn {
        log::warn!("{BROWSER_BASIC_WARNING}");
    }
    warn
}

/// The access-token factory of `with_access_token_factory`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type AccessTokenFactory = std::sync::Arc<
    dyn Fn() -> futures::future::BoxFuture<'static, Result<String, String>> + Send + Sync,
>;
#[cfg(target_arch = "wasm32")]
pub(crate) type AccessTokenFactory =
    std::rc::Rc<dyn Fn() -> futures::future::LocalBoxFuture<'static, Result<String, String>>>;

pub struct ConnectionConfiguration {
    _secure: bool,
    _domain: String,
    _hub: String,
    _port: Option<i32>,
    _authentication: Authentication,
    _access_token_factory: Option<AccessTokenFactory>,
    #[cfg(not(target_arch = "wasm32"))]
    _disconnection: Option<Box<dyn DisconnectionHandler + Send + Sync>>,
    #[cfg(target_arch = "wasm32")]
    _disconnection: Option<Box<dyn DisconnectionHandler>>,
    _reconnection: ReconnectionConfig,
    _protocol: HubProtocolKind,
    _stateful_reconnect: Option<StatefulReconnectConfig>,
    _skip_negotiation: bool,
    _transports: crate::Transports,
    _liveness: LivenessConfig,
    _headers: Vec<(String, String)>,
    _invocation_headers: Option<InvocationHeaders>,
    _maximum_message_size: Option<usize>,
    #[cfg(not(target_arch = "wasm32"))]
    _tls: crate::communication::tls::TlsSettings,
    #[cfg(not(target_arch = "wasm32"))]
    _runtime: Option<tokio::runtime::Handle>,
    /// The proxy of native connections, or the error of an invalid `with_proxy` URL.
    #[cfg(not(target_arch = "wasm32"))]
    _proxy: Result<crate::communication::proxy::ProxyConfig, crate::Error>,
    /// The cookie jar of every native request and upgrade (`with_cookie_jar`).
    #[cfg(not(target_arch = "wasm32"))]
    _cookies: crate::CookieJar,
    /// Integrated authentication (`with_default_credentials`, `with_credentials`).
    #[cfg(not(target_arch = "wasm32"))]
    _integrated: Option<crate::communication::integrated_auth::IntegratedCredentials>,
}

impl Authentication {
    /// The `Authorization` header value for negotiate requests and native WebSocket upgrades.
    pub(crate) fn header_value(&self) -> Option<String> {
        use base64::{engine::general_purpose, Engine};
        match self {
            Authentication::None => None,
            Authentication::Basic { user, password } => {
                let credentials = match password {
                    Some(password) => format!("{user}:{password}"),
                    None => format!("{user}:"),
                };
                Some(format!(
                    "Basic {}",
                    general_purpose::STANDARD.encode(credentials)
                ))
            }
            Authentication::Bearer { token } => Some(format!("Bearer {token}")),
            // Resolved per request (`resolve`) before any header is built.
            Authentication::Factory(_) => None,
        }
    }

    /// The credentials for one request: a factory is asked for a fresh token, sent as Bearer, as
    /// the .NET client asks its `AccessTokenProvider`; its error fails the request with the
    /// factory's message, redacted.
    pub(crate) async fn resolve(&self) -> Result<Authentication, crate::Error> {
        match self {
            Authentication::Factory(factory) => factory()
                .await
                .map(|token| Authentication::Bearer { token })
                .map_err(|message| {
                    crate::Error::authentication(
                        None,
                        format!(
                            "access token factory failed: {}",
                            crate::error::redact(&message)
                        ),
                    )
                }),
            other => Ok(other.clone()),
        }
    }
}

/// The invocation-header hook of `with_invocation_headers`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type InvocationHeaders =
    std::sync::Arc<dyn Fn(&str) -> Vec<(String, String)> + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub(crate) type InvocationHeaders = std::rc::Rc<dyn Fn(&str) -> Vec<(String, String)>>;

#[derive(Clone)]
pub(crate) struct ConnectionRecipe {
    secure: bool,
    domain: String,
    hub: String,
    port: Option<i32>,
    authentication: Authentication,
    protocol: HubProtocolKind,
    stateful_reconnect: Option<StatefulReconnectConfig>,
    skip_negotiation: bool,
    /// The transports the connection may use (`with_transports`).
    transports: crate::Transports,
    liveness: LivenessConfig,
    /// Application headers for every HTTP request and the native WebSocket upgrade.
    headers: Vec<(String, String)>,
    /// Headers for Invocation and StreamInvocation messages, per target.
    invocation_headers: Option<InvocationHeaders>,
    /// The largest inbound hub message accepted (`None`: unbounded).
    maximum_message_size: Option<usize>,
    /// Credentials for the transport when negotiation replaced them (a redirect access token).
    transport_authentication: Option<Authentication>,
    /// The resolved credentials of the negotiate request that produced this connection (an
    /// access-token factory's token), which HTTP transports keep using, as the .NET client's
    /// `AccessTokenHttpMessageHandler` caches the token negotiate fetched.
    negotiated_authentication: Option<Authentication>,
    #[cfg(not(target_arch = "wasm32"))]
    tls: crate::communication::tls::TlsContext,
    /// The proxy of every native connection.
    #[cfg(not(target_arch = "wasm32"))]
    proxy: crate::communication::proxy::ProxyConfig,
    /// The cookies of every native request and upgrade, shared by every connection of the client.
    #[cfg(not(target_arch = "wasm32"))]
    cookies: crate::CookieJar,
    /// Answers integrated-authentication challenges (`with_default_credentials`,
    /// `with_credentials`).
    #[cfg(not(target_arch = "wasm32"))]
    integrated: Option<std::sync::Arc<dyn crate::communication::integrated_auth::SecurityProvider>>,
    /// The Tokio runtime that owns this connection's I/O, timers and tasks.
    #[cfg(not(target_arch = "wasm32"))]
    runtime: Option<tokio::runtime::Handle>,
}

impl ConnectionRecipe {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn runtime(&self) -> Option<tokio::runtime::Handle> {
        self.runtime.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// The headers the invocation-header hook gives a message to `target`.
    pub(crate) fn invocation_headers(
        &self,
        target: &str,
    ) -> std::collections::BTreeMap<String, String> {
        self.invocation_headers
            .as_ref()
            .map(|hook| hook(target).into_iter().collect())
            .unwrap_or_default()
    }

    /// The largest inbound hub message accepted (`None`: unbounded).
    pub(crate) fn maximum_message_size(&self) -> Option<usize> {
        self.maximum_message_size
    }

    /// The application headers (`with_header`), validated when the client was built.
    pub(crate) fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub(crate) fn skips_negotiation(&self) -> bool {
        self.skip_negotiation
    }

    pub(crate) fn transports(&self) -> crate::Transports {
        self.transports
    }

    pub(crate) fn liveness(&self) -> LivenessConfig {
        self.liveness
    }

    pub(crate) fn with_transport_authentication(mut self, authentication: Authentication) -> Self {
        self.transport_authentication = Some(authentication);
        self
    }

    /// The `Authorization` value for the WebSocket: negotiated credentials, else configured ones.
    #[cfg(all(not(target_arch = "wasm32"), test))]
    pub(crate) fn transport_authorization(&self) -> Option<String> {
        self.transport_authentication
            .as_ref()
            .unwrap_or(&self.authentication)
            .header_value()
    }

    /// Records the resolved credentials the negotiate request used.
    pub(crate) fn with_negotiated_authentication(mut self, authentication: Authentication) -> Self {
        self.negotiated_authentication = Some(authentication);
        self
    }

    /// The `Authorization` value an HTTP transport (Long Polling, Server-Sent Events) starts with: a negotiate redirect's
    /// token or the configured credentials, else the token negotiate fetched from an access-token
    /// factory (the factory is asked again only after a 401, as the .NET client's
    /// `AccessTokenHttpMessageHandler`).
    pub(crate) fn http_transport_authorization(&self) -> Option<String> {
        self.transport_authentication
            .as_ref()
            .or(self.negotiated_authentication.as_ref())
            .unwrap_or(&self.authentication)
            .header_value()
    }

    /// Whether an access-token factory provides the credentials (a 401 is then retried once with
    /// a fresh token).
    pub(crate) fn has_access_token_factory(&self) -> bool {
        self.transport_authentication.is_none()
            && matches!(self.authentication, Authentication::Factory(_))
    }

    /// The `Authorization` value for a transport being started (a native WebSocket, or a Long
    /// Polling request): a negotiate redirect's token, else the configured credentials, asking an
    /// access-token factory for a fresh token, as the .NET client asks its `AccessTokenProvider`.
    pub(crate) async fn current_transport_authorization(
        &self,
    ) -> Result<Option<String>, crate::Error> {
        match &self.transport_authentication {
            Some(negotiated) => Ok(negotiated.header_value()),
            None => Ok(self.authentication.resolve().await?.header_value()),
        }
    }

    /// A fresh token for a browser WebSocket's `access_token` query when an access-token factory
    /// is configured and no negotiate redirect issued the transport's token.
    #[cfg(any(target_arch = "wasm32", test))]
    pub(crate) async fn fresh_transport_token(&self) -> Result<Option<String>, crate::Error> {
        if self.transport_authentication.is_some()
            || !matches!(self.authentication, Authentication::Factory(_))
        {
            return Ok(None);
        }
        match self.authentication.resolve().await? {
            Authentication::Bearer { token } => Ok(Some(token)),
            _ => Ok(None),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_secure(&self) -> bool {
        self.secure
    }

    /// The proxy of every native connection.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn proxy(&self) -> &crate::communication::proxy::ProxyConfig {
        &self.proxy
    }

    /// The client's cookie jar.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn cookies(&self) -> &crate::CookieJar {
        &self.cookies
    }

    /// The integrated-authentication provider, when the client has one.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn integrated(
        &self,
    ) -> Option<&std::sync::Arc<dyn crate::communication::integrated_auth::SecurityProvider>> {
        self.integrated.as_ref()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_integrated(
        mut self,
        provider: Option<
            std::sync::Arc<dyn crate::communication::integrated_auth::SecurityProvider>,
        >,
    ) -> Self {
        self.integrated = provider;
        self
    }

    /// The connection's TLS state, shared by negotiate, the WebSocket and reconnects.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn tls(&self) -> &crate::communication::tls::TlsContext {
        &self.tls
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_tls(mut self, tls: crate::communication::tls::TlsContext) -> Self {
        self.tls = tls;
        self
    }

    pub(crate) fn web_url(&self) -> String {
        format!("{}://{}/{}", self.http_schema(), self.domain(), self.hub)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn socket_url(&self) -> String {
        format!("{}://{}/{}", self.socket_schema(), self.domain(), self.hub)
    }

    pub(crate) fn authentication(&self) -> Authentication {
        self.authentication.clone()
    }

    pub(crate) fn protocol(&self) -> HubProtocolKind {
        self.protocol
    }

    /// The recipe of a fallback negotiation for `transport`: stateful reconnect is requested only
    /// for WebSockets, as the .NET client stops requesting it before a fallback to another
    /// transport.
    pub(crate) fn for_transport(&self, transport: crate::TransportKind) -> ConnectionRecipe {
        let mut recipe = self.clone();
        if transport != crate::TransportKind::WebSockets {
            recipe.stateful_reconnect = None;
        }
        recipe
    }

    pub(crate) fn stateful_reconnect(&self) -> Option<StatefulReconnectConfig> {
        self.stateful_reconnect
    }

    #[cfg(test)]
    pub(crate) fn negotiate_url(&self) -> String {
        crate::communication::negotiation::negotiate_url(
            &self.web_url(),
            self.stateful_reconnect.is_some(),
        )
    }

    pub(crate) fn display_endpoint(&self) -> String {
        self.web_url()
    }

    fn http_schema(&self) -> &'static str {
        if self.secure {
            "https"
        } else {
            "http"
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn socket_schema(&self) -> &'static str {
        if self.secure {
            "wss"
        } else {
            "ws"
        }
    }

    fn domain(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{}", self.domain, port),
            None => self.domain.clone(),
        }
    }
}

impl ConnectionConfiguration {
    pub(crate) fn new(domain: String, hub: String) -> Self {
        ConnectionConfiguration {
            _authentication: Authentication::None,
            _access_token_factory: None,
            _domain: domain,
            _secure: true,
            _hub: hub,
            _port: None,
            _disconnection: None,
            _reconnection: ReconnectionConfig::default(),
            _protocol: HubProtocolKind::default(),
            _stateful_reconnect: None,
            _skip_negotiation: false,
            _transports: crate::Transports::all(),
            _headers: Vec::new(),
            _invocation_headers: None,
            _maximum_message_size: None,
            _liveness: LivenessConfig::default(),
            #[cfg(not(target_arch = "wasm32"))]
            _tls: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            _runtime: None,
            #[cfg(not(target_arch = "wasm32"))]
            _proxy: Ok(Default::default()),
            #[cfg(not(target_arch = "wasm32"))]
            _cookies: crate::CookieJar::new(),
            #[cfg(not(target_arch = "wasm32"))]
            _integrated: None,
        }
    }

    /// Sets the port for the connection.
    ///
    /// # Arguments
    ///
    /// * `port` - An integer specifying the port number to use for the connection.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.with_port(5220);
    /// }).await.unwrap();
    /// ```
    pub fn with_port(&mut self, port: i32) -> &ConnectionConfiguration {
        self._port = Some(port);

        self
    }

    /// Sets the hub name for the connection.
    ///
    /// # Arguments
    ///
    /// * `hub` - A `String` specifying the name of the hub to connect to.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.with_hub("myHub".to_string());
    /// }).await.unwrap();
    /// ```
    pub fn with_hub(&mut self, hub: String) -> &ConnectionConfiguration {
        self._hub = hub;

        self
    }

    /// Configures the connection to use a secure (HTTPS) protocol.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.secure();
    /// }).await.unwrap();
    /// ```
    pub fn secure(&mut self) -> &ConnectionConfiguration {
        self._secure = true;

        self
    }

    /// Configures the connection to use an unsecure (HTTP) protocol.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.unsecure();
    /// }).await.unwrap();
    /// ```
    pub fn unsecure(&mut self) -> &ConnectionConfiguration {
        self._secure = false;

        self
    }

    /// Configures the connection to use basic authentication.
    ///
    /// # Arguments
    ///
    /// * `user` - A `String` specifying the username for authentication.
    /// * `password` - An `Option<String>` specifying the password for authentication. If `None`, no password is used.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.authenticate_basic("username".to_string(), Some("password".to_string()));
    /// }).await.unwrap();
    /// ```
    pub fn authenticate_basic(
        &mut self,
        user: String,
        password: Option<String>,
    ) -> &ConnectionConfiguration {
        self._authentication = Authentication::Basic { user, password };

        self
    }

    /// Configures the connection to use bearer token authentication.
    ///
    /// # Arguments
    ///
    /// * `token` - A `String` specifying the bearer token for authentication.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.authenticate_bearer("your_bearer_token".to_string());
    /// }).await.unwrap();
    /// ```
    pub fn authenticate_bearer(&mut self, token: String) -> &ConnectionConfiguration {
        self._authentication = Authentication::Bearer { token };

        self
    }

    /// Authenticates with Bearer tokens from `factory`, as the official .NET client's
    /// `HttpConnectionOptions.AccessTokenProvider`: it is called before every negotiate (the
    /// initial connect, every reconnect that negotiates) and again whenever a WebSocket is opened,
    /// including every stateful resume, so a token can be refreshed between them. The token is
    /// sent as `Authorization: Bearer` (natively on negotiate and the WebSocket upgrade; browsers
    /// send it on negotiate and as the `access_token` query of the WebSocket). A token from a
    /// negotiate redirect is used instead. An `Err` fails that attempt with
    /// [`ErrorKind::Authentication`](crate::ErrorKind::Authentication) carrying the message
    /// (redacted). Combining it with [`authenticate_basic`](Self::authenticate_basic) or
    /// [`authenticate_bearer`](Self::authenticate_bearer) fails
    /// [`SignalRClient::new`](crate::SignalRClient::new) with
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration).
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    ///     c.with_access_token_factory(|| async { Ok::<_, String>("a fresh token".to_string()) });
    /// })?;
    /// client.start().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_access_token_factory<F, Fut>(&mut self, factory: F) -> &ConnectionConfiguration
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        self._access_token_factory = Some(std::sync::Arc::new(move || Box::pin(factory())));
        self
    }

    /// Authenticates with Bearer tokens from `factory`, as the official .NET client's
    /// `HttpConnectionOptions.AccessTokenProvider`: it is called before every negotiate and again
    /// whenever a WebSocket is opened, including every stateful resume. Browsers send the token
    /// with negotiate and as the `access_token` query of the WebSocket; a token from a negotiate
    /// redirect is used instead. An `Err` fails that attempt with
    /// [`ErrorKind::Authentication`](crate::ErrorKind::Authentication); combining it with Basic or
    /// Bearer credentials fails [`SignalRClient::new`](crate::SignalRClient::new).
    #[cfg(target_arch = "wasm32")]
    pub fn with_access_token_factory<F, Fut>(&mut self, factory: F) -> &ConnectionConfiguration
    where
        F: Fn() -> Fut + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + 'static,
    {
        self._access_token_factory = Some(std::rc::Rc::new(move || Box::pin(factory())));
        self
    }

    /// Sets how long the server may hold a Long Polling poll, 100 s by default (the JavaScript
    /// client's poll timeout); each poll is bounded by it plus the connect timeout, and a poll that
    /// exceeds the bound is reissued, as the .NET client reissues a poll its `HttpClient` timed
    /// out. Like the liveness durations, every value is accepted.
    pub fn with_long_polling_timeout(&mut self, timeout: Duration) -> &ConnectionConfiguration {
        self._liveness.long_polling_timeout = timeout;
        self
    }

    /// Restricts the transports the connection may use, as the .NET client's
    /// `HttpConnectionOptions.Transports`: every transport by default. Negotiation tries the ones
    /// the server offers, in the server's order, keeping those in this set that carry the hub
    /// protocol's transfer format (Server-Sent Events carry text only, so never MessagePack); a
    /// transport that fails to start is followed by the next one with a fresh negotiation.
    /// [`skip_negotiation`](Self::skip_negotiation) needs `Transports::WEB_SOCKETS` alone,
    /// stateful reconnect needs WebSockets in the set, and the MessagePack protocol cannot use
    /// Server-Sent Events alone; otherwise [`SignalRClient::new`](crate::SignalRClient::new) fails
    /// with [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming the setting
    /// (`with_transports` for Server-Sent Events with MessagePack).
    pub fn with_transports(&mut self, transports: crate::Transports) -> &ConnectionConfiguration {
        self._transports = transports;
        self
    }

    /// Fails for transport sets that cannot work: `skip_negotiation` without WebSockets alone,
    /// stateful reconnect without WebSockets, as the .NET client rejects them.
    pub(crate) fn validate_transports(&self) -> Result<(), crate::Error> {
        use crate::communication::transport::TransportKind;
        if self._skip_negotiation && !self._transports.is_only(TransportKind::WebSockets) {
            return Err(crate::Error::configuration(
                "skip_negotiation",
                "negotiation can only be skipped with the WebSockets transport alone: add \
                 with_transports(Transports::WEB_SOCKETS)",
            ));
        }
        if self._stateful_reconnect.is_some()
            && !self._transports.contains(TransportKind::WebSockets)
        {
            return Err(crate::Error::configuration(
                "with_stateful_reconnect",
                "stateful reconnect needs the WebSockets transport",
            ));
        }
        // Server-Sent Events carry text only; with other transports in the set selection skips
        // them instead.
        if self._transports.is_only(TransportKind::ServerSentEvents)
            && self._protocol.transfer_format() == "Binary"
        {
            return Err(crate::Error::configuration(
                "with_transports",
                "Server-Sent Events carry text only, so they cannot carry the MessagePack hub \
                 protocol: allow another transport or use JSON",
            ));
        }
        Ok(())
    }

    /// Fails when an access-token factory is combined with static credentials.
    pub(crate) fn validate_authentication(&self) -> Result<(), crate::Error> {
        if self._access_token_factory.is_some()
            && !matches!(self._authentication, Authentication::None)
        {
            return Err(crate::Error::configuration(
                "with_access_token_factory",
                "an access token factory cannot be combined with authenticate_basic or \
                 authenticate_bearer",
            ));
        }
        Ok(())
    }

    /// Sets a disconnection handler for the connection.
    ///
    /// # Arguments
    ///
    /// * `handler` - An implementation of the `DisconnectionHandler` trait to handle disconnection events.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    /// # Examples
    /// ```ignore
    /// let client = SignalRClient::connect_with("localhost", "test", |c| {
    ///     c.with_disconnection_handler(MyDisconnectionHandler{});
    /// }).await.unwrap();
    /// ```
    pub fn with_disconnection_handler<
        Handler: DisconnectionHandler + crate::platform::MaybeSendSync + 'static,
    >(
        &mut self,
        handler: Handler,
    ) -> &ConnectionConfiguration {
        self._disconnection = Some(Box::new(handler));

        self
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn get_disconnection_handler(
        &mut self,
    ) -> Option<Box<dyn DisconnectionHandler + Send + Sync>> {
        self._disconnection.take()
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn get_disconnection_handler(&mut self) -> Option<Box<dyn DisconnectionHandler>> {
        self._disconnection.take()
    }

    pub(crate) fn get_reconnection_config(&self) -> ReconnectionConfig {
        self._reconnection.clone()
    }

    /// Sets the reconnection policy for the connection.
    ///
    /// # Arguments
    ///
    /// * `config` - A `ReconnectionConfig` specifying the policy.
    ///
    /// # Returns
    ///
    /// * `&ConnectionConfiguration` - Returns a reference to the updated connection configuration.
    pub fn with_reconnection_policy(
        &mut self,
        config: ReconnectionConfig,
    ) -> &ConnectionConfiguration {
        self._reconnection = config;
        self
    }

    /// Connects the WebSocket directly to `ws(s)://host[:port]/hub` without a negotiate request,
    /// as the official clients' `skipNegotiation` does; a bearer token is sent in the
    /// `Authorization` header (browsers, which cannot set WebSocket headers, send it as
    /// `access_token`). The server must allow WebSockets-only connections. Ordinary reconnects
    /// connect directly too. As in the .NET client, it disables
    /// [`with_stateful_reconnect`](Self::with_stateful_reconnect), which needs the connection
    /// token negotiation issues; a warning is logged.
    pub fn skip_negotiation(&mut self) -> &ConnectionConfiguration {
        self._skip_negotiation = true;
        self
    }

    /// Sends a Ping after this long without outbound messages, so the server (whose
    /// `ClientTimeoutInterval` defaults to 30 s) keeps the connection. Defaults to 15 s.
    /// Every value is accepted, as the official .NET client accepts it: zero acts as one second
    /// (the .NET client's timer tick), and a value not below the server timeout is logged as a
    /// warning when the client is built.
    pub fn with_keep_alive_interval(&mut self, interval: Duration) -> &ConnectionConfiguration {
        self._liveness.keep_alive_interval = interval;
        self
    }

    /// Treats the transport as dropped after this long without any inbound message (the server
    /// pings every `KeepAliveInterval`, 15 s by default); the reconnect path then runs as for any
    /// dropped transport. Defaults to 30 s. Every value is accepted: zero acts as one second, and
    /// a value not above the keep-alive interval is logged as a warning (the connection is then
    /// considered lost whenever the server is idle).
    pub fn with_server_timeout(&mut self, timeout: Duration) -> &ConnectionConfiguration {
        self._liveness.server_timeout = timeout;
        self
    }

    /// Fails `connect` (and ends a reconnect attempt) when the server does not answer the
    /// handshake within this time. Defaults to 15 s. Zero is accepted (with a warning): every
    /// handshake then times out.
    pub fn with_handshake_timeout(&mut self, timeout: Duration) -> &ConnectionConfiguration {
        self._liveness.handshake_timeout = timeout;
        self
    }

    /// Bounds connection setup: each negotiate request, and opening the transport (natively the
    /// TCP connection, TLS and the WebSocket upgrade; in browsers opening the WebSocket). Browsers
    /// abort a negotiate `fetch` that exceeds it. Applies to `connect` and to every reconnect
    /// attempt. Defaults to 120 s, the official .NET client's bound. Zero is accepted (with a
    /// warning): every attempt then times out.
    ///
    /// A setup that exceeds the bound fails with [`ErrorKind::Timeout`](crate::ErrorKind::Timeout)
    /// naming [`TimeoutOperation::Negotiate`](crate::TimeoutOperation::Negotiate) or
    /// [`TimeoutOperation::Connect`](crate::TimeoutOperation::Connect).
    pub fn with_connect_timeout(&mut self, timeout: Duration) -> &ConnectionConfiguration {
        self._liveness.connect_timeout = timeout;
        self
    }

    /// Bounds how long [`stop`](crate::SignalRClient::stop) waits for the transport to close after
    /// the client sent its Close message, as the official .NET client's `CloseTimeout`. Defaults
    /// to 5 s; when it elapses the transport is dropped.
    pub fn with_close_timeout(&mut self, timeout: Duration) -> &ConnectionConfiguration {
        self._liveness.close_timeout = timeout;
        self
    }

    /// Adds a header to every HTTP request (negotiate and the HTTP transports) and natively to the
    /// WebSocket upgrade, as the official .NET client's `HttpConnectionOptions.Headers` does.
    /// Browsers cannot set WebSocket headers: there the header reaches negotiate only, and
    /// building the client logs a warning. As in the .NET client, the header replaces a default
    /// of the same name (an empty `User-Agent` removes it), while `X-Requested-With` and the
    /// credentials' `Authorization` always win. An invalid name or value, or a header the
    /// transport owns (`Host`, `Content-Length`, `Connection`, `Upgrade`, `Transfer-Encoding`,
    /// `TE`, `Trailer`, `Keep-Alive`, `Proxy-Connection`, `Sec-WebSocket-*`), fails
    /// [`SignalRClient::new`](crate::SignalRClient::new) before any I/O with
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming `with_header`.
    pub fn with_header(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> &ConnectionConfiguration {
        self._headers.push((name.into(), value.into()));
        self
    }

    /// Adds headers to every Invocation and StreamInvocation message (`invoke`, `send` and
    /// `enumerate`), as the official .NET client adds the W3C `traceparent` and `tracestate`
    /// headers of the current activity. The hook receives the hub method's name and runs on the
    /// calling task each time a message is built. ASP.NET Core uses `traceparent` and `tracestate`
    /// to parent the activity of the hub method's invocation (hub code sees it as
    /// `Activity.Current`); it does not expose other invocation headers to hub methods.
    /// Headers with the same name keep the last value.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(configuration: &mut rayx_signalr::ConnectionConfiguration) {
    /// configuration.with_invocation_headers(|_target| {
    ///     vec![(
    ///         "traceparent".to_string(),
    ///         "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
    ///     )]
    /// });
    /// # }
    /// ```
    pub fn with_invocation_headers(
        &mut self,
        hook: impl Fn(&str) -> Vec<(String, String)> + crate::platform::MaybeSendSync + 'static,
    ) -> &ConnectionConfiguration {
        #[cfg(not(target_arch = "wasm32"))]
        let hook: InvocationHeaders = std::sync::Arc::new(hook);
        #[cfg(target_arch = "wasm32")]
        let hook: InvocationHeaders = std::rc::Rc::new(hook);
        self._invocation_headers = Some(hook);
        self
    }

    /// Bounds the size of an inbound hub message (a JSON record or a MessagePack message, which may
    /// span several WebSocket messages); a larger one ends the transport with
    /// [`ErrorKind::Protocol`](crate::ErrorKind::Protocol) and the reconnection policy runs. Unbounded
    /// by default, as in the official .NET client.
    pub fn with_maximum_message_size(&mut self, bytes: usize) -> &ConnectionConfiguration {
        self._maximum_message_size = Some(bytes);
        self
    }

    /// Enables ASP.NET Core stateful reconnect for this connection.
    pub fn with_stateful_reconnect(
        &mut self,
        config: StatefulReconnectConfig,
    ) -> &ConnectionConfiguration {
        self._stateful_reconnect = Some(config);
        self
    }

    /// Configures the connection to use the MessagePack hub protocol instead of JSON.
    /// Requires the `messagepack` cargo feature to be enabled.
    #[cfg(feature = "messagepack")]
    pub fn with_messagepack_protocol(&mut self) -> &ConnectionConfiguration {
        self._protocol = HubProtocolKind::MessagePack;
        self
    }

    pub(crate) fn connection_recipe(&self) -> ConnectionRecipe {
        ConnectionRecipe {
            secure: self._secure,
            domain: self._domain.clone(),
            hub: self._hub.clone(),
            port: self._port,
            authentication: match &self._access_token_factory {
                Some(factory) => Authentication::Factory(factory.clone()),
                None => self._authentication.clone(),
            },
            protocol: self._protocol,
            stateful_reconnect: self._stateful_reconnect,
            skip_negotiation: self._skip_negotiation,
            transports: self._transports,
            liveness: self._liveness,
            headers: self._headers.clone(),
            invocation_headers: self._invocation_headers.clone(),
            maximum_message_size: self._maximum_message_size,
            transport_authentication: None,
            negotiated_authentication: None,
            #[cfg(not(target_arch = "wasm32"))]
            tls: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            runtime: None,
            #[cfg(not(target_arch = "wasm32"))]
            proxy: self._proxy.clone().unwrap_or_default(),
            #[cfg(not(target_arch = "wasm32"))]
            cookies: self._cookies.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            integrated: None,
        }
    }

    /// The integrated-authentication provider of `with_default_credentials`/`with_credentials`;
    /// fails where the credentials are not available (the current account outside Windows).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn integrated_provider(
        &self,
    ) -> Result<
        Option<std::sync::Arc<dyn crate::communication::integrated_auth::SecurityProvider>>,
        crate::Error,
    > {
        self._integrated
            .as_ref()
            .map(crate::communication::integrated_auth::provider)
            .transpose()
    }

    pub(crate) fn liveness(&self) -> LivenessConfig {
        self._liveness
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn tls_settings(&self) -> &crate::communication::tls::TlsSettings {
        &self._tls
    }

    /// The runtime for this connection: the configured handle, else the ambient runtime at
    /// `connect`, else an error that explains both options. Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn resolve_runtime(&self) -> Result<tokio::runtime::Handle, crate::Error> {
        self._runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok())
            .ok_or_else(no_runtime)
    }

    /// Binds this connection to a Tokio runtime. Native only.
    ///
    /// Socket I/O, timers (reconnect delays, stateful backpressure) and the connection's tasks
    /// run on this runtime, and the client's futures only wait on runtime-agnostic primitives, so
    /// they can be awaited from any executor (for example a UI framework's executor or
    /// `futures::executor::block_on` on a plain thread). Without this, `connect` binds the
    /// connection to the ambient Tokio runtime it is called from.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_runtime_handle(
        &mut self,
        runtime: tokio::runtime::Handle,
    ) -> &ConnectionConfiguration {
        self._runtime = Some(runtime);
        self
    }

    /// Trusts additional root certificates (PEM, one or more `CERTIFICATE` blocks) for secure
    /// endpoints of this connection, in addition to the selected verifier's roots. Native only.
    ///
    /// Invalid PEM, or PEM without a certificate, makes `connect` fail before any I/O. With the
    /// platform verifier the roots extend the operating system's trust store, except on Android,
    /// where `rustls-platform-verifier` cannot add roots: a connection with extra roots there uses
    /// webpki verification over the extra roots (plus the bundled Mozilla roots when the
    /// `tls-webpki-roots` feature is enabled).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_additional_root_certificates_pem(
        &mut self,
        pem: impl Into<Vec<u8>>,
    ) -> &ConnectionConfiguration {
        self._tls.extra_roots_pem = Some(pem.into());
        self
    }

    /// Presents this client certificate whenever a server asks for one, as the .NET client's
    /// `HttpConnectionOptions.ClientCertificates`: on every native negotiate, WebSocket, Long
    /// Polling and Server-Sent Events TLS connection. `chain_pem` holds the certificate and any
    /// intermediates (`CERTIFICATE` blocks, the leaf first); `key_pem` its private key (PKCS#8,
    /// PKCS#1 or SEC1 PEM). Invalid PEM or a key that does not match the certificate makes
    /// [`SignalRClient::new`](crate::SignalRClient::new) fail with
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming
    /// `with_client_certificate`; errors never contain the key. Native only: browsers choose
    /// client certificates from their own store.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_client_certificate(
        &mut self,
        chain_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> &ConnectionConfiguration {
        self._tls.client_certificate = Some(crate::communication::tls::ClientCertificatePem {
            chain: chain_pem.into(),
            key: key_pem.into(),
        });
        self
    }

    /// Verifies secure endpoints of this connection against the bundled Mozilla roots instead of
    /// the operating system's trust store. Requires the `tls-webpki-roots` feature. Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_webpki_roots(&mut self) -> &ConnectionConfiguration {
        self._tls.use_webpki_roots = true;
        self
    }

    /// Connects through this HTTP proxy, `http://[user:password@]host[:port]` (port 80 by
    /// default; user and password may be percent-encoded), instead of the system's proxy
    /// settings. Every native connection (negotiate, WebSocket, Long Polling, Server-Sent Events)
    /// opens an HTTP `CONNECT` tunnel through it, with Basic `Proxy-Authorization` when the URL
    /// carries credentials, and runs TLS inside the tunnel for secure endpoints, so the proxy
    /// never sees the traffic. Credentials never appear in logs or errors. An invalid URL fails
    /// [`SignalRClient::new`](crate::SignalRClient::new) with
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming `with_proxy`. Native
    /// only: browsers use the browser's proxy settings.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_proxy(&mut self, url: impl AsRef<str>) -> &ConnectionConfiguration {
        self._proxy = crate::communication::proxy::ProxyUrl::parse(url.as_ref())
            .map(crate::communication::proxy::ProxyConfig::Explicit);
        self
    }

    /// Uses the system's proxy settings, the default, as the .NET client's default proxy: the
    /// `HTTPS_PROXY` (secure endpoints) or `HTTP_PROXY` (plain ones) environment variable, else
    /// `ALL_PROXY`, read in lower or upper case when each connection is opened, except for the
    /// hosts `NO_PROXY` lists (curl's rules: host names with their subdomains, IP addresses, CIDR
    /// ranges, optional ports, `*` for every host). Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_system_proxy(&mut self) -> &ConnectionConfiguration {
        self._proxy = Ok(crate::communication::proxy::ProxyConfig::System);
        self
    }

    /// Authenticates with the current Windows account, as the .NET client's
    /// `HttpConnectionOptions.UseDefaultCredentials`: when the server answers a native negotiate
    /// request, WebSocket upgrade, Long Polling or Server-Sent Events request with a 401 offering
    /// `Negotiate` (Kerberos or NTLM) or `NTLM`, the request is sent again on the same connection
    /// with the account's tokens until the server accepts or rejects them. Windows only (SSPI):
    /// elsewhere [`SignalRClient::new`](crate::SignalRClient::new) fails with
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming
    /// `with_default_credentials`. Browsers authenticate with the browser's own integrated
    /// authentication.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_default_credentials(&mut self) -> &ConnectionConfiguration {
        self._integrated =
            Some(crate::communication::integrated_auth::IntegratedCredentials::Default);
        self
    }

    /// Authenticates with these credentials, as the .NET client's
    /// `HttpConnectionOptions.Credentials` (`NetworkCredential(user, password, domain)`): the
    /// same challenge-response as
    /// [`with_default_credentials`](Self::with_default_credentials), through SSPI on Windows and
    /// NTLMv2 on other native targets. `domain` may be empty. The password never appears in logs,
    /// errors or `Debug` output. Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_credentials(
        &mut self,
        user: impl Into<String>,
        password: impl Into<String>,
        domain: impl Into<String>,
    ) -> &ConnectionConfiguration {
        self._integrated = Some(
            crate::communication::integrated_auth::IntegratedCredentials::Explicit(
                crate::communication::integrated_auth::Explicit {
                    user: user.into(),
                    password: password.into(),
                    domain: domain.into(),
                },
            ),
        );
        self
    }

    /// Uses `jar` for this client's cookies instead of a jar of its own, as the .NET client's
    /// `HttpConnectionOptions.Cookies`. Every native negotiate request, WebSocket upgrade and
    /// Long Polling or Server-Sent Events request stores the `Set-Cookie` headers it receives and
    /// sends the cookies that match its URL, across reconnects, so a load balancer's affinity
    /// cookie keeps the connection on one server. Share a jar between clients, or keep a clone to
    /// read or seed it. Native only: browsers manage cookies themselves.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_cookie_jar(&mut self, jar: crate::CookieJar) -> &ConnectionConfiguration {
        self._cookies = jar;
        self
    }

    /// Connects directly, ignoring the system's proxy settings. Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn without_proxy(&mut self) -> &ConnectionConfiguration {
        self._proxy = Ok(crate::communication::proxy::ProxyConfig::Disabled);
        self
    }

    /// Fails when `with_proxy` was given an invalid URL.
    pub(crate) fn validate_proxy(&self) -> Result<(), crate::Error> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(error) = &self._proxy {
            return Err(error.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod stateful_configuration_tests {
    use super::ConnectionConfiguration;
    use crate::StatefulReconnectConfig;

    #[test]
    fn stateful_negotiate_query_is_opt_in_only() {
        let mut configuration = ConnectionConfiguration::new("localhost".into(), "hub".into());
        assert!(!configuration
            .connection_recipe()
            .negotiate_url()
            .contains("useStatefulReconnect"));
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        assert!(configuration
            .connection_recipe()
            .negotiate_url()
            .ends_with("&useStatefulReconnect=true"));
    }
}

/// Browser Basic credentials (R4.C2): sent with negotiate as in the .NET client, with a warning
/// that names the Bearer alternatives; native clients do not warn.
#[cfg(test)]
mod browser_basic {
    use super::{warn_browser_basic, Authentication, BROWSER_BASIC_WARNING};

    fn basic() -> Authentication {
        Authentication::Basic {
            user: "user".into(),
            password: Some("password".into()),
        }
    }

    #[test]
    fn browser_basic_credentials_warn_in_browsers_only() {
        let (warned, records) = crate::test_logging::capture(|| warn_browser_basic(&basic(), true));
        assert!(warned);
        let warnings: Vec<_> = records
            .iter()
            .filter(|record| record.level == log::Level::Warn)
            .collect();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].message, BROWSER_BASIC_WARNING);
        assert!(warnings[0].message.contains("with_access_token_factory"));
        assert!(warnings[0].message.contains("authenticate_bearer"));
        assert!(!warnings[0].message.contains("password"), "no credential");

        let (warned, records) =
            crate::test_logging::capture(|| warn_browser_basic(&basic(), false));
        assert!(
            !warned,
            "native clients send Basic on the WebSocket upgrade too"
        );
        assert!(records.is_empty());
    }

    #[test]
    fn browser_basic_other_credentials_never_warn() {
        for authentication in [
            Authentication::None,
            Authentication::Bearer {
                token: "token".into(),
            },
        ] {
            let (warned, records) =
                crate::test_logging::capture(|| warn_browser_basic(&authentication, true));
            assert!(!warned);
            assert!(records.is_empty());
        }
    }
}
