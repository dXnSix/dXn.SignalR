//! Proxy auto-config (`with_proxy_auto_config`, feature `proxy-pac`): a PAC script, loaded from
//! an `http`, `https` or `file` URL once per `start` (directly, never through a proxy, under the
//! connect timeout), decides per connection with `FindProxyForURL(url, host)` which proxies to
//! try, in order. Scripts run in the pure-Rust Boa engine on a blocking thread, bounded in time and
//! loop iterations, with the standard helper functions and no network or file access beyond the
//! DNS lookups of `dnsResolve`, `isResolvable` and `isInNet`.

// PAC code must not panic on script or server input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use boa_engine::{js_string, Context, JsArgs, JsResult, JsString, JsValue, NativeFunction, Source};

use super::proxy::{target_url, ProxyUrl, Route};
use crate::{error::Error, platform::LockExt};

/// How long one evaluation of `FindProxyForURL` may run.
const EVALUATION_LIMIT: Duration = Duration::from_secs(5);
/// Loop iterations a script may run, so a runaway evaluation ends its thread.
const LOOP_LIMIT: u64 = 1_000_000;
/// The largest PAC script accepted.
const SCRIPT_LIMIT: usize = 1024 * 1024;

/// The setting PAC configuration errors name.
const SETTING: &str = "with_proxy_auto_config";

/// A PAC script's URL and the script loaded for the current `start`.
#[derive(Clone)]
pub(crate) struct PacConfig {
    url: String,
    script: Arc<Mutex<Option<Arc<str>>>>,
}

impl PartialEq for PacConfig {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
    }
}

impl Eq for PacConfig {}

impl fmt::Debug for PacConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PacConfig({})", crate::error::redact(&self.url))
    }
}

impl PacConfig {
    /// A PAC configuration for `url` (`http`, `https` or `file`).
    pub(crate) fn new(url: &str) -> Result<Self, Error> {
        let scheme = url
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase());
        if !matches!(scheme.as_deref(), Some("http" | "https" | "file")) {
            return Err(Error::configuration(
                SETTING,
                "the PAC script URL must be an http, https or file URL",
            ));
        }
        Ok(Self {
            url: url.to_string(),
            script: Arc::default(),
        })
    }

    /// Forgets the loaded script, so the next connection loads it again (each `start`).
    pub(crate) fn begin_connect(&self) {
        *self.script.locked() = None;
    }

    /// The script, loaded on first use after [`begin_connect`](Self::begin_connect).
    async fn script(&self, tls: &super::tls::TlsContext) -> Result<Arc<str>, Error> {
        if let Some(script) = self.script.locked().clone() {
            return Ok(script);
        }
        let loaded: Arc<str> = load(&self.url, tls).await?.into();
        Ok(self.script.locked().get_or_insert(loaded).clone())
    }

    /// The routes the script chooses for a connection to `host:port`.
    pub(crate) async fn routes(
        &self,
        tls: &super::tls::TlsContext,
        secure: bool,
        host: &str,
        port: u16,
    ) -> Result<Vec<Route>, Error> {
        let script = self.script(tls).await?;
        routes_of_script(script, secure, host, port).await
    }
}

/// The routes `script` chooses for a connection to `host:port`.
pub(crate) async fn routes_of_script(
    script: impl AsRef<str> + Send + 'static,
    secure: bool,
    host: &str,
    port: u16,
) -> Result<Vec<Route>, Error> {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let url = target_url(secure, &host, port);
    let evaluation = tokio::task::spawn_blocking(move || evaluate(script.as_ref(), &url, &host));
    let result = tokio::time::timeout(EVALUATION_LIMIT + DNS_LIMIT, evaluation)
        .await
        .map_err(|_| {
            Error::proxy(
                None,
                format!("the PAC script's FindProxyForURL ran longer than {EVALUATION_LIMIT:?}"),
            )
        })?
        .map_err(|error| Error::proxy(None, format!("the PAC script stopped: {error}")))??;
    Ok(chosen_routes(&result))
}

/// The routes of a PAC result, logged as parsed: a proxy's credentials (`user:pass@` in an
/// entry) are masked, never logged as the script wrote them.
fn chosen_routes(result: &str) -> Vec<Route> {
    let routes = parse_result(result);
    log::debug!("The PAC script chose {routes:?}");
    routes
}

fn load_failure(message: impl fmt::Display) -> Error {
    Error::proxy(
        None,
        format!("the PAC script could not be loaded: {message}"),
    )
}

/// Loads the script at `url`: a file, or an HTTP(S) `GET` sent directly.
async fn load(url: &str, tls: &super::tls::TlsContext) -> Result<String, Error> {
    let bytes = if let Some(path) = file_path(url) {
        tokio::task::spawn_blocking(move || std::fs::read(path))
            .await
            .map_err(load_failure)?
            .map_err(load_failure)?
    } else {
        fetch(url, tls).await?
    };
    if bytes.len() > SCRIPT_LIMIT {
        return Err(load_failure(format!(
            "it is larger than {SCRIPT_LIMIT} bytes"
        )));
    }
    String::from_utf8(bytes).map_err(|_| load_failure("it is not UTF-8 text"))
}

/// The local path of a `file://` URL (`file:///etc/proxy.pac`, `file:///C:/proxy.pac`).
fn file_path(url: &str) -> Option<std::path::PathBuf> {
    let (scheme, rest) = url.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("file") {
        return None;
    }
    let path = rest.strip_prefix("localhost").unwrap_or(rest);
    // `/C:/...` is a Windows drive path.
    let path = match path.as_bytes() {
        [b'/', drive, b':', ..] if drive.is_ascii_alphabetic() => path.get(1..).unwrap_or(path),
        _ => path,
    };
    Some(std::path::PathBuf::from(percent_decoded(path)))
}

fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        let escaped = (byte == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(value) => {
                decoded.push(value);
                index += 3;
            }
            None => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// `GET`s the script over HTTP(S), directly (a PAC script is never loaded through a proxy).
async fn fetch(url: &str, tls: &super::tls::TlsContext) -> Result<Vec<u8>, Error> {
    use http_body_util::{BodyExt, Empty, Limited};
    use hyper::body::Bytes;

    let uri: http::Uri = url.parse().map_err(load_failure)?;
    let secure = uri.scheme_str() == Some("https");
    let authority = uri
        .authority()
        .ok_or_else(|| load_failure("the URL has no host"))?
        .clone();
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = authority
        .port_u16()
        .unwrap_or(if secure { 443 } else { 80 });
    let request = http::Request::get(uri.path_and_query().map_or("/", |path| path.as_str()))
        .header(http::header::HOST, authority.as_str())
        .header(http::header::USER_AGENT, super::headers::user_agent())
        .body(Empty::<Bytes>::new())
        .map_err(load_failure)?;
    let tcp = super::negotiate_http::connect_tcp(host, port)
        .await
        .map_err(load_failure)?;
    let response = if secure {
        send(
            super::negotiate_http::tls_stream(tls, host, tcp).await?,
            request,
        )
        .await?
    } else {
        send(tcp, request).await?
    };
    if !response.status().is_success() {
        return Err(load_failure(format!(
            "the server answered {}",
            response.status()
        )));
    }
    Ok(Limited::new(response.into_body(), SCRIPT_LIMIT + 1)
        .collect()
        .await
        .map_err(|_| {
            load_failure(format!(
                "it is larger than {SCRIPT_LIMIT} bytes or was cut off"
            ))
        })?
        .to_bytes()
        .to_vec())
}

async fn send<S>(
    stream: S,
    request: http::Request<http_body_util::Empty<hyper::body::Bytes>>,
) -> Result<http::Response<hyper::body::Incoming>, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(load_failure)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sender.send_request(request).await.map_err(load_failure)
}

/// The routes of a `FindProxyForURL` result (`PROXY host:port; SOCKS5 host:port; DIRECT`):
/// `PROXY` and `HTTP` are HTTP proxies, `HTTPS` an HTTPS proxy, `SOCKS` SOCKS4 and `SOCKS4`/
/// `SOCKS5` their versions; unknown or malformed entries are skipped, and a result without a
/// usable entry connects directly.
pub(crate) fn parse_result(result: &str) -> Vec<Route> {
    let mut routes: Vec<Route> = result
        .split(';')
        .filter_map(|entry| {
            let mut parts = entry.split_whitespace();
            let kind = parts.next()?.to_ascii_uppercase();
            if kind == "DIRECT" {
                return Some(Route::Direct);
            }
            let scheme = match kind.as_str() {
                "PROXY" | "HTTP" => "http",
                "HTTPS" => "https",
                "SOCKS" | "SOCKS4" => "socks4",
                "SOCKS5" => "socks5",
                _ => {
                    log::warn!("The PAC result names the unknown route type {kind}; it is skipped");
                    return None;
                }
            };
            let address = parts.next()?;
            match ProxyUrl::parse(&format!("{scheme}://{address}")) {
                Ok(proxy) => Some(Route::Proxy(proxy)),
                Err(_) => {
                    log::warn!("The PAC result names an unusable {kind} proxy; it is skipped");
                    None
                }
            }
        })
        .collect();
    if routes.is_empty() {
        routes.push(Route::Direct);
    }
    routes
}

/// The standard PAC helper functions that need no host access, and the date and time ranges.
const PRELUDE: &str = r#"
function isPlainHostName(host) { return String(host).indexOf('.') < 0; }
function dnsDomainIs(host, domain) {
  host = String(host).toLowerCase(); domain = String(domain).toLowerCase();
  return host.length >= domain.length && host.substring(host.length - domain.length) === domain;
}
function localHostOrDomainIs(host, hostdom) {
  host = String(host).toLowerCase(); hostdom = String(hostdom).toLowerCase();
  return host === hostdom || (host.indexOf('.') < 0 && hostdom.indexOf(host + '.') === 0);
}
function dnsResolve(host) { return __rayxResolve(String(host)); }
function isResolvable(host) { return __rayxResolve(String(host)) !== null; }
function myIpAddress() { return __rayxMyIpAddress(); }
function dnsDomainLevels(host) { return String(host).split('.').length - 1; }
function __rayxAddress(ip) {
  var parts = String(ip).split('.');
  if (parts.length !== 4) { return null; }
  var value = 0;
  for (var i = 0; i < 4; i++) {
    var part = Number(parts[i]);
    if (!(part >= 0 && part <= 255) || parts[i] === '') { return null; }
    value = value * 256 + part;
  }
  return value;
}
function isInNet(host, pattern, mask) {
  var address = __rayxAddress(host) === null ? dnsResolve(host) : String(host);
  if (address === null) { return false; }
  var a = __rayxAddress(address), p = __rayxAddress(pattern), m = __rayxAddress(mask);
  if (a === null || p === null || m === null) { return false; }
  for (var bit = 2147483648; bit >= 1; bit /= 2) {
    var maskBit = Math.floor(m / bit) % 2;
    if (maskBit === 1 && Math.floor(a / bit) % 2 !== Math.floor(p / bit) % 2) { return false; }
  }
  return true;
}
function shExpMatch(text, pattern) {
  var expression = '^' + String(pattern)
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.') + '$';
  return new RegExp(expression).test(String(text));
}
var __rayxDays = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
var __rayxMonths = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
function __rayxArguments(list) {
  var values = Array.prototype.slice.call(list);
  var gmt = values.length > 0 && String(values[values.length - 1]).toUpperCase() === 'GMT';
  if (gmt) { values.pop(); }
  return { values: values, gmt: gmt, now: __rayxNow() };
}
function __rayxNow() { return new Date(); }
function __rayxInRange(current, start, end) {
  return start <= end ? current >= start && current <= end : current >= start || current <= end;
}
function weekdayRange() {
  var call = __rayxArguments(arguments);
  var today = call.gmt ? call.now.getUTCDay() : call.now.getDay();
  var first = __rayxDays.indexOf(String(call.values[0]).toUpperCase());
  if (first < 0) { return false; }
  if (call.values.length < 2) { return today === first; }
  var last = __rayxDays.indexOf(String(call.values[1]).toUpperCase());
  return last >= 0 && __rayxInRange(today, first, last);
}
function dateRange() {
  var call = __rayxArguments(arguments), now = call.now;
  var today = {
    d: call.gmt ? now.getUTCDate() : now.getDate(),
    m: call.gmt ? now.getUTCMonth() : now.getMonth(),
    y: call.gmt ? now.getUTCFullYear() : now.getFullYear()
  };
  function kind(value) {
    if (__rayxMonths.indexOf(String(value).toUpperCase()) >= 0) { return 'm'; }
    return Number(value) > 31 ? 'y' : 'd';
  }
  function number(value) {
    var month = __rayxMonths.indexOf(String(value).toUpperCase());
    return month >= 0 ? month : Number(value);
  }
  var values = call.values;
  if (values.length === 1) { return today[kind(values[0])] === number(values[0]); }
  if (values.length === 0 || values.length % 2 !== 0) { return false; }
  var half = values.length / 2, kinds = {};
  for (var i = 0; i < half; i++) { kinds[kind(values[i])] = true; }
  function key(part) {
    var fields = { d: today.d, m: today.m, y: today.y };
    for (var i = 0; i < part.length; i++) { fields[kind(part[i])] = number(part[i]); }
    return (kinds.y ? fields.y : 0) * 10000 + (kinds.m ? fields.m : 0) * 100 + (kinds.d ? fields.d : 0);
  }
  return __rayxInRange(key([]), key(values.slice(0, half)), key(values.slice(half)));
}
function timeRange() {
  var call = __rayxArguments(arguments), now = call.now, values = call.values;
  var hours = call.gmt ? now.getUTCHours() : now.getHours();
  var minutes = call.gmt ? now.getUTCMinutes() : now.getMinutes();
  var seconds = call.gmt ? now.getUTCSeconds() : now.getSeconds();
  if (values.length === 1) { return hours === Number(values[0]); }
  if (values.length === 0 || values.length % 2 !== 0 || values.length > 6) { return false; }
  var half = values.length / 2;
  function seconds_of(part) {
    return Number(part[0]) * 3600 + (half > 1 ? Number(part[1]) * 60 : 0) + (half > 2 ? Number(part[2]) : 0);
  }
  var current = hours * 3600 + (half > 1 ? minutes * 60 : 0) + (half > 2 ? seconds : 0);
  return __rayxInRange(current, seconds_of(values.slice(0, half)), seconds_of(values.slice(half)));
}
"#;

/// How long a helper's DNS lookup may take.
const DNS_LIMIT: Duration = Duration::from_secs(2);

/// How many helper DNS lookups may be in flight in the process (each on its own thread, which
/// a slow system resolver can keep past its evaluation).
const CONCURRENT_LOOKUPS: usize = 16;

static LOOKUPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A DNS lookup: the host's first IPv4 address.
type Lookup = fn(&str) -> Option<std::net::Ipv4Addr>;

fn system_lookup(host: &str) -> Option<std::net::Ipv4Addr> {
    use std::net::ToSocketAddrs;
    (host, 0)
        .to_socket_addrs()
        .ok()?
        .find_map(|address| match address.ip() {
            std::net::IpAddr::V4(address) => Some(address),
            std::net::IpAddr::V6(_) => None,
        })
}

/// The DNS state of the evaluation running on this thread: its deadline, its lookup and the
/// answers so far, so each host is resolved at most once per evaluation.
struct Resolver {
    deadline: Instant,
    lookup: Lookup,
    answers: std::collections::HashMap<String, Option<std::net::Ipv4Addr>>,
}

thread_local! {
    static RESOLVER: std::cell::RefCell<Option<Resolver>> = const { std::cell::RefCell::new(None) };
}

/// Clears this thread's [`Resolver`] when the evaluation ends.
struct ResolverScope;

impl ResolverScope {
    fn enter(deadline: Instant, lookup: Lookup) -> Self {
        RESOLVER.with(|resolver| {
            *resolver.borrow_mut() = Some(Resolver {
                deadline,
                lookup,
                answers: std::collections::HashMap::new(),
            });
        });
        Self
    }
}

impl Drop for ResolverScope {
    fn drop(&mut self) {
        RESOLVER.with(|resolver| *resolver.borrow_mut() = None);
    }
}

/// Resolves `host` for the evaluation on this thread: a cached answer, else a lookup that waits
/// at most [`DNS_LIMIT`] and never past the evaluation's deadline (`None` then, as when no more
/// lookup threads may start).
fn resolve_host(host: &str) -> Option<std::net::Ipv4Addr> {
    use std::sync::atomic::Ordering;
    enum State {
        Cached(Option<std::net::Ipv4Addr>),
        Lookup(Instant, Lookup),
    }
    let state = RESOLVER.with(|resolver| {
        resolver
            .borrow()
            .as_ref()
            .map(|resolver| match resolver.answers.get(host) {
                Some(answer) => State::Cached(*answer),
                None => State::Lookup(resolver.deadline, resolver.lookup),
            })
    })?;
    let (deadline, lookup) = match state {
        State::Cached(answer) => return answer,
        State::Lookup(deadline, lookup) => (deadline, lookup),
    };
    let wait = DNS_LIMIT.min(deadline.saturating_duration_since(Instant::now()));
    let answer = if wait.is_zero() {
        None
    } else if LOOKUPS.fetch_add(1, Ordering::SeqCst) >= CONCURRENT_LOOKUPS {
        LOOKUPS.fetch_sub(1, Ordering::SeqCst);
        None
    } else {
        let (sender, receiver) = std::sync::mpsc::channel();
        let name = host.to_string();
        std::thread::spawn(move || {
            let _ = sender.send(lookup(&name));
            LOOKUPS.fetch_sub(1, Ordering::SeqCst);
        });
        receiver.recv_timeout(wait).ok().flatten()
    };
    RESOLVER.with(|resolver| {
        if let Some(resolver) = resolver.borrow_mut().as_mut() {
            resolver.answers.insert(host.to_string(), answer);
        }
    });
    answer
}

/// `dnsResolve`: the host's first IPv4 address, or `null` (also when the lookup takes longer than
/// [`DNS_LIMIT`] or the evaluation's deadline; the lookup's thread then ends with the system
/// resolver's own timeout).
fn resolve(_: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let host = args
        .get_or_undefined(0)
        .to_string(context)?
        .to_std_string_escaped();
    Ok(resolve_host(&host).map_or(JsValue::null(), |address| {
        JsValue::from(JsString::from(address.to_string()))
    }))
}

/// `myIpAddress`: the local address of the default route, else `127.0.0.1`. No packet is sent.
fn my_ip_address(_: &JsValue, _: &[JsValue], _: &mut Context) -> JsResult<JsValue> {
    let address = std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect("198.51.100.1:80")?;
            socket.local_addr()
        })
        .map_or_else(
            |_| "127.0.0.1".to_string(),
            |address| address.ip().to_string(),
        );
    Ok(JsValue::from(JsString::from(address)))
}

fn script_failure(error: impl fmt::Display) -> Error {
    Error::proxy(None, format!("the PAC script failed: {error}"))
}

/// Runs `script`'s `FindProxyForURL(url, host)` and returns its result.
pub(crate) fn evaluate(script: &str, url: &str, host: &str) -> Result<String, Error> {
    evaluate_with(script, url, host, None)
}

/// [`evaluate`], with `now` (milliseconds since the epoch) as the date and time ranges' clock.
fn evaluate_with(script: &str, url: &str, host: &str, now: Option<f64>) -> Result<String, Error> {
    evaluate_with_lookup(script, url, host, now, system_lookup)
}

/// [`evaluate_with`] with `lookup` as the DNS helpers' resolver.
fn evaluate_with_lookup(
    script: &str,
    url: &str,
    host: &str,
    now: Option<f64>,
    lookup: Lookup,
) -> Result<String, Error> {
    let deadline = Instant::now() + EVALUATION_LIMIT;
    let _resolver = ResolverScope::enter(deadline, lookup);
    let _slot = EvaluationSlot::take(deadline)?;
    let mut context = Context::default();
    context
        .runtime_limits_mut()
        .set_loop_iteration_limit(LOOP_LIMIT);
    context
        .register_global_callable(
            js_string!("__rayxResolve"),
            1,
            NativeFunction::from_fn_ptr(resolve),
        )
        .map_err(script_failure)?;
    context
        .register_global_callable(
            js_string!("__rayxMyIpAddress"),
            0,
            NativeFunction::from_fn_ptr(my_ip_address),
        )
        .map_err(script_failure)?;
    run(&mut context, PRELUDE, deadline)?;
    if let Some(now) = now {
        run(
            &mut context,
            &format!("function __rayxNow() {{ return new Date({now}); }}"),
            deadline,
        )?;
    }
    run(&mut context, script, deadline)?;
    let global = context.global_object();
    for (name, value) in [("__rayxUrl", url), ("__rayxHost", host)] {
        global
            .set(
                JsString::from(name),
                JsValue::from(JsString::from(value)),
                false,
                &mut context,
            )
            .map_err(script_failure)?;
    }
    let result = run(
        &mut context,
        "typeof FindProxyForURL === 'function' \
         ? String(FindProxyForURL(__rayxUrl, __rayxHost)) \
         : (function () { throw new Error('it defines no FindProxyForURL function'); })()",
        deadline,
    )?;
    Ok(result
        .to_string(&mut context)
        .map_err(script_failure)?
        .to_std_string_escaped())
}

/// Instructions a script runs between two checks of the evaluation's deadline.
const BUDGET: u32 = 10_000;

/// Runs `source` in `context`, checking `deadline` between instruction budgets (calls included),
/// so a runaway script ends with its thread. A builtin call (such as a regular expression) runs
/// to its end before the next check.
fn run(context: &mut Context, source: &str, deadline: Instant) -> Result<JsValue, Error> {
    use std::{
        future::Future,
        task::{Context as TaskContext, Poll},
    };

    let script = boa_engine::Script::parse(Source::from_bytes(source), None, context)
        .map_err(script_failure)?;
    let evaluation = script.evaluate_async_with_budget(context, BUDGET);
    let mut evaluation = std::pin::pin!(evaluation);
    let waker = futures::task::noop_waker();
    let mut task = TaskContext::from_waker(&waker);
    loop {
        if let Poll::Ready(result) = evaluation.as_mut().poll(&mut task) {
            return result.map_err(script_failure);
        }
        if Instant::now() >= deadline {
            return Err(Error::proxy(
                None,
                format!("the PAC script ran longer than {EVALUATION_LIMIT:?}"),
            ));
        }
    }
}

/// How many scripts may run at once; a further evaluation waits for a slot until its deadline,
/// so a builtin that keeps a thread busy cannot multiply threads.
const CONCURRENT_EVALUATIONS: usize = 8;

/// One of the [`CONCURRENT_EVALUATIONS`] evaluation slots, released when dropped.
struct EvaluationSlot;

fn slots() -> &'static (Mutex<usize>, std::sync::Condvar) {
    static SLOTS: std::sync::OnceLock<(Mutex<usize>, std::sync::Condvar)> =
        std::sync::OnceLock::new();
    SLOTS.get_or_init(|| (Mutex::new(0), std::sync::Condvar::new()))
}

impl EvaluationSlot {
    /// Waits for a slot until `deadline`.
    fn take(deadline: Instant) -> Result<Self, Error> {
        let (running, freed) = slots();
        let mut running = running.locked();
        while *running >= CONCURRENT_EVALUATIONS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::proxy(
                    None,
                    format!("{CONCURRENT_EVALUATIONS} PAC scripts were already running until the deadline"),
                ));
            }
            running = match freed.wait_timeout(running, remaining) {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        *running += 1;
        Ok(Self)
    }
}

impl Drop for EvaluationSlot {
    fn drop(&mut self) {
        let (running, freed) = slots();
        *running.locked() -= 1;
        freed.notify_one();
    }
}

#[cfg(test)]
mod pac_tests {
    use super::{evaluate, evaluate_with, file_path, parse_result, target_url, PacConfig};
    use crate::{
        communication::proxy::{ProxyUrl, Route},
        ErrorKind,
    };

    fn call(body: &str, url: &str, host: &str) -> String {
        evaluate(
            &format!("function FindProxyForURL(url, host) {{ {body} }}"),
            url,
            host,
        )
        .unwrap()
    }

    fn truth(expression: &str) -> bool {
        call(&format!("return String({expression});"), "http://x/", "x") == "true"
    }

    /// Evaluates `expression` at 2026-10-07 (a Wednesday) 14:30:15 UTC.
    fn at_fixed_time(expression: &str) -> bool {
        // Date.UTC(2026, 9, 7, 14, 30, 15)
        evaluate_with(
            &format!("function FindProxyForURL(url, host) {{ return String({expression}); }}"),
            "http://x/",
            "x",
            Some(1_791_383_415_000.0),
        )
        .unwrap()
            == "true"
    }

    #[test]
    fn pac_host_name_helpers_follow_the_netscape_definitions() {
        assert!(truth("isPlainHostName('www')"));
        assert!(!truth("isPlainHostName('www.example.com')"));
        assert!(truth("dnsDomainIs('www.Example.com', '.example.com')"));
        assert!(!truth("dnsDomainIs('www', '.example.com')"));
        assert!(truth(
            "localHostOrDomainIs('www.example.com', 'www.example.com')"
        ));
        assert!(truth("localHostOrDomainIs('www', 'www.example.com')"));
        assert!(!truth(
            "localHostOrDomainIs('www.other.com', 'www.example.com')"
        ));
        assert!(truth("dnsDomainLevels('www.example.com') === 2"));
        assert!(truth("dnsDomainLevels('www') === 0"));
    }

    #[test]
    fn pac_sh_exp_match_supports_wildcards_and_escapes_the_rest() {
        assert!(truth("shExpMatch('http://home.example.com/x', '*/home.*')"));
        assert!(truth("shExpMatch('a.b', 'a?b')"));
        assert!(!truth("shExpMatch('axb', 'a.b')"), "a dot is literal");
        assert!(!truth("shExpMatch('www.example.com', '*.net')"));
        assert!(truth("shExpMatch('(x)', '(x)')"));
    }

    #[test]
    fn pac_dns_helpers_resolve_and_match_networks() {
        assert!(truth("dnsResolve('127.0.0.1') === '127.0.0.1'"));
        assert!(truth("dnsResolve('localhost') !== null"));
        assert!(truth("isResolvable('localhost')"));
        assert!(!truth("isResolvable('no-such-host.invalid')"));
        assert!(truth("isInNet('10.1.2.3', '10.0.0.0', '255.0.0.0')"));
        assert!(!truth("isInNet('11.1.2.3', '10.0.0.0', '255.0.0.0')"));
        assert!(truth("isInNet('localhost', '127.0.0.0', '255.0.0.0')"));
        assert!(!truth(
            "isInNet('no-such-host.invalid', '0.0.0.0', '0.0.0.0')"
        ));
        assert!(truth("/^\\d+\\.\\d+\\.\\d+\\.\\d+$/.test(myIpAddress())"));
    }

    #[test]
    fn pac_date_and_time_ranges_use_the_clock() {
        assert!(at_fixed_time("weekdayRange('WED', 'GMT')"));
        assert!(at_fixed_time("weekdayRange('MON', 'FRI', 'GMT')"));
        assert!(at_fixed_time("weekdayRange('SAT', 'WED', 'GMT')"), "wraps");
        assert!(!at_fixed_time("weekdayRange('THU', 'SAT', 'GMT')"));
        assert!(at_fixed_time("dateRange(7, 'GMT')"));
        assert!(at_fixed_time("dateRange('OCT', 'GMT')"));
        assert!(at_fixed_time("dateRange(2026, 'GMT')"));
        assert!(at_fixed_time("dateRange(1, 15, 'GMT')"));
        assert!(at_fixed_time("dateRange('SEP', 'NOV', 'GMT')"));
        assert!(at_fixed_time("dateRange(1, 'OCT', 31, 'DEC', 'GMT')"));
        assert!(!at_fixed_time(
            "dateRange(8, 'OCT', 2026, 31, 'DEC', 2026, 'GMT')"
        ));
        assert!(at_fixed_time("timeRange(14, 'GMT')"));
        assert!(at_fixed_time("timeRange(9, 17, 'GMT')"));
        assert!(!at_fixed_time("timeRange(15, 30, 16, 0, 'GMT')"));
        assert!(at_fixed_time("timeRange(14, 30, 0, 14, 30, 20, 'GMT')"));
        assert!(
            !at_fixed_time("timeRange(16, 13, 'GMT')"),
            "a wrapping range"
        );
    }

    #[test]
    fn pac_results_parse_into_routes_in_order() {
        assert_eq!(
            parse_result("PROXY proxy.example:3128; SOCKS5 socks.example:1080; DIRECT"),
            vec![
                Route::Proxy(ProxyUrl::parse("http://proxy.example:3128").unwrap()),
                Route::Proxy(ProxyUrl::parse("socks5://socks.example:1080").unwrap()),
                Route::Direct,
            ]
        );
        assert_eq!(
            parse_result("SOCKS old.example:1080"),
            vec![Route::Proxy(
                ProxyUrl::parse("socks4://old.example:1080").unwrap()
            )]
        );
        assert_eq!(
            parse_result("HTTP a:1;  socks4 b:2"),
            vec![
                Route::Proxy(ProxyUrl::parse("http://a:1").unwrap()),
                Route::Proxy(ProxyUrl::parse("socks4://b:2").unwrap()),
            ]
        );
        // Unknown and malformed entries are skipped; nothing usable connects directly.
        assert_eq!(parse_result("QUIC q:443; PROXY"), vec![Route::Direct]);
        assert_eq!(parse_result(""), vec![Route::Direct]);
    }

    #[test]
    fn pac_find_proxy_for_url_receives_the_url_and_host() {
        let script =
            "function FindProxyForURL(url, host) { return 'PROXY ' + host + ':1 ; ' + url; }";
        assert_eq!(
            evaluate(script, &target_url(true, "hub.example", 443), "hub.example").unwrap(),
            "PROXY hub.example:1 ; https://hub.example/"
        );
        assert_eq!(target_url(false, "::1", 8080), "http://[::1]:8080/");
    }

    #[test]
    fn pac_evaluation_is_bounded() {
        let error = evaluate(
            "function FindProxyForURL(url, host) { while (true) {} }",
            "http://x/",
            "x",
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
        // A loop that calls a function with its own loop is not caught by the per-frame loop
        // limit; the deadline ends it, and with it the evaluation's thread.
        let started = std::time::Instant::now();
        let error = evaluate(
            "function spin() { for (var i = 0; i < 900000; i++) {} } \
             function FindProxyForURL(url, host) { while (true) { spin(); } }",
            "http://x/",
            "x",
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
        assert!(error.to_string().contains("ran longer than"), "{error}");
        assert!(
            started.elapsed() < super::EVALUATION_LIMIT + std::time::Duration::from_secs(2),
            "the evaluation ended at its deadline"
        );
    }

    #[test]
    fn pac_script_errors_are_proxy_errors() {
        for script in [
            "function FindProxyForURL(url, host) { return undefinedName; }",
            "this is not javascript",
            "var FindProxyForURL = 1;",
            "function Other() {}",
        ] {
            let error = evaluate(script, "http://x/", "x").unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Proxy, "{script}: {error}");
        }
    }

    fn slow_lookup(host: &str) -> Option<std::net::Ipv4Addr> {
        std::thread::sleep(std::time::Duration::from_millis(1200));
        (host == "corp.example").then_some(std::net::Ipv4Addr::new(10, 1, 2, 3))
    }

    fn hanging_lookup(_: &str) -> Option<std::net::Ipv4Addr> {
        std::thread::sleep(std::time::Duration::from_secs(20));
        None
    }

    #[test]
    fn pac_dns_lookups_are_cached_per_evaluation() {
        let started = std::time::Instant::now();
        let result = super::evaluate_with_lookup(
            "function FindProxyForURL(url, host) {                var inside = isInNet(host, '10.0.0.0', '255.0.0.0')                  && isInNet(host, '10.1.0.0', '255.255.0.0')                  && isInNet(host, '10.1.2.0', '255.255.255.0')                  && isResolvable(host) && dnsResolve(host) === '10.1.2.3';                return inside ? 'PROXY corp-proxy:8080' : 'DIRECT'; }",
            "http://corp.example/",
            "corp.example",
            None,
            slow_lookup,
        )
        .unwrap();
        assert_eq!(result, "PROXY corp-proxy:8080");
        assert!(
            started.elapsed() < std::time::Duration::from_millis(2500),
            "one lookup for five helper calls ({:?})",
            started.elapsed()
        );
    }

    #[test]
    fn pac_dns_lookups_never_outwait_their_bound() {
        // A resolver that hangs (off VPN): each distinct host waits at most two seconds and
        // resolves to null, so the script still reaches its fallback route.
        let started = std::time::Instant::now();
        let result = super::evaluate_with_lookup(
            "function FindProxyForURL(url, host) {                if (isInNet(host, '10.0.0.0', '255.0.0.0') || isInNet(host, '10.0.0.0', '255.0.0.0')                    || isResolvable('intranet.corp.example')) { return 'PROXY corp-proxy:8080'; }                return 'DIRECT'; }",
            "http://hub.example/",
            "hub.example",
            None,
            hanging_lookup,
        )
        .unwrap();
        assert_eq!(result, "DIRECT");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(3900) && elapsed < super::EVALUATION_LIMIT,
            "two distinct hosts, each bounded at two seconds ({elapsed:?})"
        );
    }

    #[test]
    fn pac_scripts_have_no_host_objects() {
        for name in ["XMLHttpRequest", "fetch", "require", "process", "Deno"] {
            assert!(
                truth(&format!("typeof {name} === 'undefined'")),
                "{name} is not available to PAC scripts"
            );
        }
    }

    #[test]
    fn pac_urls_must_be_http_https_or_file() {
        for url in [
            "http://pac/x.pac",
            "HTTPS://pac/x.pac",
            "file:///etc/proxy.pac",
        ] {
            assert!(PacConfig::new(url).is_ok(), "{url}");
        }
        for url in ["ftp://pac/x.pac", "proxy.pac", "javascript:alert(1)"] {
            let error = PacConfig::new(url).unwrap_err();
            assert_eq!(error.setting(), Some("with_proxy_auto_config"), "{url}");
        }
        assert_eq!(
            file_path("file:///C:/proxy%20files/x.pac").unwrap(),
            std::path::PathBuf::from("C:/proxy files/x.pac")
        );
        assert_eq!(
            file_path("file:///etc/proxy.pac").unwrap(),
            std::path::PathBuf::from("/etc/proxy.pac")
        );
        assert_eq!(file_path("http://x/"), None);
    }

    /// Serves `script` once over HTTP on loopback, with `status`.
    async fn serve_script(status: u16, script: &'static str) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/x-ns-proxy-autoconfig\r\nContent-Length: {}\r\n\r\n{script}",
                script.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        port
    }

    #[tokio::test]
    async fn pac_scripts_load_over_http_and_failures_are_proxy_errors() {
        let tls = crate::communication::tls::TlsContext::default();
        let port = serve_script(
            200,
            "function FindProxyForURL(url, host) { return 'SOCKS5 s:1080; DIRECT'; }",
        )
        .await;
        let pac = PacConfig::new(&format!("http://127.0.0.1:{port}/proxy.pac")).unwrap();
        assert_eq!(
            pac.routes(&tls, true, "hub", 443).await.unwrap(),
            vec![
                Route::Proxy(ProxyUrl::parse("socks5://s:1080").unwrap()),
                Route::Direct
            ]
        );
        let port = serve_script(404, "").await;
        let pac = PacConfig::new(&format!("http://127.0.0.1:{port}/proxy.pac")).unwrap();
        let error = pac.routes(&tls, true, "hub", 443).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
        assert!(error.to_string().contains("404"), "{error}");
    }

    #[tokio::test]
    async fn pac_routes_are_tried_in_order() {
        // The first route's proxy refuses connections; the connection goes DIRECT.
        let refused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let refused_port = refused.local_addr().unwrap().port();
        drop(refused);
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_port = target.local_addr().unwrap().port();
        let script: &'static str = Box::leak(
            format!(
                "function FindProxyForURL(url, host) {{ return 'PROXY 127.0.0.1:{refused_port}; DIRECT'; }}"
            )
            .into_boxed_str(),
        );
        let port = serve_script(200, script).await;
        let config = crate::communication::proxy::ProxyConfig::AutoConfig(
            PacConfig::new(&format!("http://127.0.0.1:{port}/proxy.pac")).unwrap(),
        );
        let tls = crate::communication::tls::TlsContext::default();
        let accepted = tokio::spawn(async move { target.accept().await.is_ok() });
        crate::communication::proxy::connect(&config, &tls, false, "127.0.0.1", target_port)
            .await
            .unwrap();
        assert!(accepted.await.unwrap(), "DIRECT reached the target");

        // Every route fails: the last failure is reported.
        let port = serve_script(
            200,
            "function FindProxyForURL(url, host) { return 'PROXY 127.0.0.1:9'; }",
        )
        .await;
        let config = crate::communication::proxy::ProxyConfig::AutoConfig(
            PacConfig::new(&format!("http://127.0.0.1:{port}/proxy.pac")).unwrap(),
        );
        let error = crate::communication::proxy::connect(&config, &tls, false, "hub", 80)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");

        // A proxy that was reached decides: its refusal is the error, DIRECT is not tried.
        let refusing = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refusing_port = refusing.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = refusing.accept().await.unwrap();
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer).await;
            let _ = stream
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await;
        });
        let script: &'static str = Box::leak(
            format!(
                "function FindProxyForURL(url, host) {{ return 'PROXY 127.0.0.1:{refusing_port}; DIRECT'; }}"
            )
            .into_boxed_str(),
        );
        let port = serve_script(200, script).await;
        let config = crate::communication::proxy::ProxyConfig::AutoConfig(
            PacConfig::new(&format!("http://127.0.0.1:{port}/proxy.pac")).unwrap(),
        );
        let error = crate::communication::proxy::connect(&config, &tls, false, "127.0.0.1", 9)
            .await
            .unwrap_err();
        assert_eq!(
            (error.kind(), error.status()),
            (ErrorKind::Proxy, Some(403)),
            "{error}"
        );
    }

    #[tokio::test]
    async fn pac_scripts_load_from_files_once_per_start() {
        let directory = std::env::temp_dir().join(format!("rayx-pac-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("proxy.pac");
        std::fs::write(
            &path,
            "function FindProxyForURL(url, host) { return 'PROXY first:1'; }",
        )
        .unwrap();
        let url = format!(
            "file:///{}",
            path.display().to_string().trim_start_matches('/')
        );
        let pac = PacConfig::new(&url).unwrap();
        let tls = crate::communication::tls::TlsContext::default();
        let first = pac.routes(&tls, false, "hub", 80).await.unwrap();
        assert_eq!(
            first,
            vec![Route::Proxy(ProxyUrl::parse("http://first:1").unwrap())]
        );
        std::fs::write(
            &path,
            "function FindProxyForURL(url, host) { return 'DIRECT'; }",
        )
        .unwrap();
        assert_eq!(
            pac.routes(&tls, false, "hub", 80).await.unwrap(),
            first,
            "the script is loaded once per start"
        );
        pac.clone().begin_connect();
        assert_eq!(
            pac.routes(&tls, false, "hub", 80).await.unwrap(),
            vec![Route::Direct],
            "a new start loads it again"
        );
        std::fs::remove_dir_all(&directory).unwrap();

        let missing = PacConfig::new("file:///no/such/proxy.pac").unwrap();
        let error = missing.routes(&tls, false, "hub", 80).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
    }
}

#[cfg(test)]
mod log_redaction_tests {
    #[test]
    fn log_redaction_pac_results_never_log_proxy_credentials() {
        let (routes, records) = crate::test_logging::capture(|| {
            super::chosen_routes("PROXY user:secret-pass@proxy.example:8080; DIRECT")
        });
        assert_eq!(routes.len(), 2);
        let logged: Vec<_> = records
            .iter()
            .map(|record| record.message.as_str())
            .collect();
        assert!(!logged.is_empty(), "the choice is logged");
        assert!(
            logged
                .iter()
                .all(|message| !message.contains("secret-pass")),
            "{logged:?}"
        );
    }
}
