use crate::client::ConnectionRecipe;
use crate::execution::UpdatableActionStorage;
use crate::protocol::hub_protocol::HubProtocolKind;
use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloseEnvelope {
    allow_reconnect: Option<bool>,
}

pub(crate) fn close_allows_reconnect(
    payload: &crate::protocol::hub_protocol::MessagePayload,
) -> Result<bool, String> {
    match payload {
        crate::protocol::hub_protocol::MessagePayload::Text(text) => {
            serde_json::from_str::<CloseEnvelope>(text)
                .map(|close| close.allow_reconnect.unwrap_or(false))
                .map_err(|error| format!("Cannot parse Close message: {error}"))
        }
        #[cfg(feature = "messagepack")]
        crate::protocol::hub_protocol::MessagePayload::Binary(data) => {
            let items = crate::protocol::msgpack::parse_msgpack_message(data)?;
            Ok(items.get(2).and_then(rmpv::Value::as_bool).unwrap_or(false))
        }
    }
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
    async fn connect(configuration: &ConnectionData) -> Result<Self, String>;
    async fn send<T: Serialize>(&mut self, data: T) -> Result<(), String>;
    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String>;
    fn get_storage(&self) -> Result<UpdatableActionStorage, String>;
    fn get_protocol_kind(&self) -> HubProtocolKind;
    async fn disconnect(&mut self);
}

pub struct HttpClient {}

impl HttpClient {
    /// Negotiates (following redirects) or connects directly when negotiation is skipped.
    pub(crate) async fn negotiate(recipe: &ConnectionRecipe) -> Result<ConnectionData, String> {
        super::negotiation::negotiate_with(recipe, |url, authorization| {
            Self::post_text(url, authorization, recipe)
        })
        .await
    }

    /// Native negotiate: hyper over the connection's TCP or rustls stream.
    #[cfg(not(target_arch = "wasm32"))]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        recipe: &ConnectionRecipe,
    ) -> Result<String, String> {
        super::negotiate_http::post(&endpoint, authorization, recipe.tls()).await
    }

    /// Browser negotiate: `fetch` on the global scope, so it also works in Web Workers.
    #[cfg(target_arch = "wasm32")]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        _recipe: &ConnectionRecipe,
    ) -> Result<String, String> {
        let mut request = gloo_net::http::Request::post(&endpoint);
        if let Some(value) = authorization {
            request = request.header("Authorization", &value);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("The call failed: {error}"))?;
        if !response.ok() {
            return Err(format!(
                "the server returned {} {}",
                response.status(),
                response.status_text()
            ));
        }
        response
            .text()
            .await
            .map_err(|error| format!("The returned response has no text body: {error}"))
    }
}
