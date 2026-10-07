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
}

pub struct ConnectionConfiguration {
    _secure: bool,
    _domain: String,
    _hub: String,
    _port: Option<i32>,
    _authentication: Authentication,
    #[cfg(not(target_arch = "wasm32"))]
    _disconnection: Option<Box<dyn DisconnectionHandler + Send + Sync>>,
    #[cfg(target_arch = "wasm32")]
    _disconnection: Option<Box<dyn DisconnectionHandler>>,
    _reconnection: ReconnectionConfig,
    _protocol: HubProtocolKind,
    _stateful_reconnect: Option<StatefulReconnectConfig>,
    _skip_negotiation: bool,
    _liveness: LivenessConfig,
    _headers: Vec<(String, String)>,
    _invocation_headers: Option<InvocationHeaders>,
    _maximum_message_size: Option<usize>,
    #[cfg(not(target_arch = "wasm32"))]
    _tls: crate::communication::tls::TlsSettings,
    #[cfg(not(target_arch = "wasm32"))]
    _runtime: Option<tokio::runtime::Handle>,
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
    liveness: LivenessConfig,
    /// Application headers for every HTTP request and the native WebSocket upgrade.
    headers: Vec<(String, String)>,
    /// Headers for Invocation and StreamInvocation messages, per target.
    invocation_headers: Option<InvocationHeaders>,
    /// The largest inbound hub message accepted (`None`: unbounded).
    maximum_message_size: Option<usize>,
    /// Credentials for the transport when negotiation replaced them (a redirect access token).
    transport_authentication: Option<Authentication>,
    #[cfg(not(target_arch = "wasm32"))]
    tls: crate::communication::tls::TlsContext,
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

    pub(crate) fn liveness(&self) -> LivenessConfig {
        self.liveness
    }

    pub(crate) fn with_transport_authentication(mut self, authentication: Authentication) -> Self {
        self.transport_authentication = Some(authentication);
        self
    }

    /// The `Authorization` value for the WebSocket: negotiated credentials, else configured ones.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn transport_authorization(&self) -> Option<String> {
        self.transport_authentication
            .as_ref()
            .unwrap_or(&self.authentication)
            .header_value()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_secure(&self) -> bool {
        self.secure
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
            _domain: domain,
            _secure: true,
            _hub: hub,
            _port: None,
            _disconnection: None,
            _reconnection: ReconnectionConfig::default(),
            _protocol: HubProtocolKind::default(),
            _stateful_reconnect: None,
            _skip_negotiation: false,
            _headers: Vec::new(),
            _invocation_headers: None,
            _maximum_message_size: None,
            _liveness: LivenessConfig::default(),
            #[cfg(not(target_arch = "wasm32"))]
            _tls: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            _runtime: None,
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
            authentication: self._authentication.clone(),
            protocol: self._protocol,
            stateful_reconnect: self._stateful_reconnect,
            skip_negotiation: self._skip_negotiation,
            liveness: self._liveness,
            headers: self._headers.clone(),
            invocation_headers: self._invocation_headers.clone(),
            maximum_message_size: self._maximum_message_size,
            transport_authentication: None,
            #[cfg(not(target_arch = "wasm32"))]
            tls: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            runtime: None,
        }
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

    /// Verifies secure endpoints of this connection against the bundled Mozilla roots instead of
    /// the operating system's trust store. Requires the `tls-webpki-roots` feature. Native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_webpki_roots(&mut self) -> &ConnectionConfiguration {
        self._tls.use_webpki_roots = true;
        self
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
