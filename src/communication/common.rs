use crate::client::ConnectionRecipe;
use crate::error::Error;
use crate::execution::UpdatableActionStorage;
use crate::protocol::hub_protocol::HubProtocolKind;
use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloseEnvelope {
    error: Option<String>,
    allow_reconnect: Option<bool>,
}

/// The error a server Close message ends the connection with: its optional error text and its
/// reconnect permission (absent or malformed means no reconnect, as in the official clients).
pub(crate) fn server_close(payload: &crate::protocol::hub_protocol::MessagePayload) -> Error {
    let (message, allow_reconnect) = match payload {
        crate::protocol::hub_protocol::MessagePayload::Text(text) => {
            match serde_json::from_str::<CloseEnvelope>(text) {
                Ok(close) => (close.error, close.allow_reconnect.unwrap_or(false)),
                Err(_) => (None, false),
            }
        }
        #[cfg(feature = "messagepack")]
        crate::protocol::hub_protocol::MessagePayload::Binary(data) => {
            match crate::protocol::msgpack::parse_msgpack_message(data) {
                Ok(items) => (
                    items
                        .get(1)
                        .and_then(rmpv::Value::as_str)
                        .map(str::to_string),
                    items.get(2).and_then(rmpv::Value::as_bool).unwrap_or(false),
                ),
                Err(_) => (None, false),
            }
        }
    };
    Error::closed_by_server(message, allow_reconnect)
}

#[derive(Clone)]
pub struct ConnectionData {
    endpoint: String,
    connection_id: String,
    protocol_kind: HubProtocolKind,
    recipe: ConnectionRecipe,
    stateful_reconnect: Option<crate::StatefulReconnectConfig>,
}

impl ConnectionData {
    pub(crate) fn new(
        endpoint: String,
        connection_id: String,
        protocol_kind: HubProtocolKind,
        recipe: ConnectionRecipe,
        stateful_reconnect: Option<crate::StatefulReconnectConfig>,
    ) -> Self {
        Self {
            endpoint,
            connection_id,
            protocol_kind,
            recipe,
            stateful_reconnect,
        }
    }

    pub fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    #[allow(dead_code)]
    pub fn get_connection_id(&self) -> String {
        self.connection_id.clone()
    }

    pub fn get_protocol_kind(&self) -> HubProtocolKind {
        self.protocol_kind
    }

    pub(crate) fn get_recipe(&self) -> ConnectionRecipe {
        self.recipe.clone()
    }

    pub(crate) fn stateful_reconnect(&self) -> Option<crate::StatefulReconnectConfig> {
        self.stateful_reconnect
    }
}

pub trait Communication: Clone {
    async fn connect(configuration: &ConnectionData) -> Result<Self, Error>;
    async fn send<T: Serialize>(&mut self, data: T) -> Result<(), Error>;
    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), Error>;
    fn get_storage(&self) -> Result<UpdatableActionStorage, Error>;
    fn get_protocol_kind(&self) -> HubProtocolKind;
    /// Closes the connection locally for every handle, like the official clients' `stop`.
    async fn disconnect(&self);
    /// Releases this handle: closes the connection only when no other handle remains.
    async fn release(&mut self);
}

pub struct HttpClient {}

impl HttpClient {
    /// Negotiates (following redirects) or connects directly when negotiation is skipped.
    pub(crate) async fn negotiate(recipe: &ConnectionRecipe) -> Result<ConnectionData, Error> {
        super::negotiation::negotiate_with(recipe, |url, authorization| {
            Self::post_text(url, authorization, recipe)
        })
        .await
    }

    /// Native negotiate: hyper over the connection's TCP or rustls stream, within the connect
    /// timeout.
    #[cfg(not(target_arch = "wasm32"))]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        recipe: &ConnectionRecipe,
    ) -> Result<String, Error> {
        let timeout = recipe.liveness().connect_timeout;
        super::negotiate_http::post(&endpoint, authorization, recipe.tls(), timeout).await
    }

    /// Browser negotiate: `fetch` on the global scope, so it also works in Web Workers. The
    /// request is aborted when the connect timeout elapses or the caller stops waiting.
    #[cfg(target_arch = "wasm32")]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        recipe: &ConnectionRecipe,
    ) -> Result<String, Error> {
        use futures::future::{select, Either};

        /// Aborts the fetch when dropped; aborting a finished request does nothing.
        struct AbortOnDrop(web_sys::AbortController);

        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }

        let timeout = recipe.liveness().connect_timeout;
        let controller = web_sys::AbortController::new().map_err(|_| {
            Error::transport_failure(None, "the browser cannot create an AbortController")
        })?;
        let request = Self::fetch_text(endpoint, authorization, controller.signal());
        let _abort = AbortOnDrop(controller);
        let timer = gloo_timers::future::sleep(timeout);
        match select(std::pin::pin!(request), std::pin::pin!(timer)).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(Error::timed_out(
                crate::error::TimeoutOperation::Negotiate,
                timeout,
            )),
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn fetch_text(
        endpoint: String,
        authorization: Option<String>,
        signal: web_sys::AbortSignal,
    ) -> Result<String, Error> {
        let mut request = gloo_net::http::Request::post(&endpoint).abort_signal(Some(&signal));
        if let Some(value) = authorization {
            request = request.header("Authorization", &value);
        }
        let response = request.send().await.map_err(|error| {
            Error::transport_failure(None, format!("the negotiate request failed: {error}"))
        })?;
        if !response.ok() {
            let body = response.binary().await.unwrap_or_default();
            return Err(super::negotiation::negotiate_failure(
                response.status(),
                &response.status_text(),
                &body,
            ));
        }
        response.text().await.map_err(|error| {
            Error::negotiate(
                Some(response.status()),
                format!("the negotiate response has no text body: {error}"),
            )
        })
    }
}
