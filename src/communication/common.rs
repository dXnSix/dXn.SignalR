use crate::client::{Authentication, ConnectionRecipe};
use crate::execution::UpdatableActionStorage;
use crate::protocol::hub_protocol::HubProtocolKind;
use crate::protocol::negotiate::NegotiateResponse;
use serde::Deserialize;
use serde::Serialize;

const WEB_SOCKET_TRANSPORT: &str = "WebSockets";

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
    pub(crate) async fn negotiate(recipe: &ConnectionRecipe) -> Result<ConnectionData, String> {
        let negotiate_endpoint = recipe.negotiate_url();
        let protocol_kind = recipe.protocol();
        let authentication = recipe.authentication();
        let json_text =
            HttpClient::post_text(negotiate_endpoint.clone(), authentication.clone()).await;

        match json_text {
            Ok(text) => {
                let negotiate = NegotiateResponse::from_json(&text)
                    .map_err(|e| format!("Failed to parse negotiate response: {e}"))?;

                HttpClient::create_configuration(recipe.socket_url(), negotiate, protocol_kind, &authentication, recipe.clone())
                    .ok_or_else(|| format!(
                        "The negotiation concluded no matching communication protocols for {:?} transfer format",
                        protocol_kind.transfer_format()
                    ))
            }
            Err(e) => Err(format!(
                "HTTP negotiation with endpoint {} failed: {}",
                recipe.display_endpoint(),
                e
            )),
        }
    }

    fn create_configuration(
        endpoint: String,
        negotiate: NegotiateResponse,
        protocol_kind: HubProtocolKind,
        authentication: &Authentication,
        recipe: ConnectionRecipe,
    ) -> Option<ConnectionData> {
        let required_format = protocol_kind.transfer_format();
        let fit = negotiate
            .available_transports()
            .iter()
            .find(|i| i.transport == WEB_SOCKET_TRANSPORT)
            .and_then(|i| {
                i.transfer_formats
                    .iter()
                    .find(|j| j.as_str() == required_format)
            })
            .is_some();

        if fit {
            let stateful_reconnect = if negotiate.use_stateful_reconnect() {
                recipe.stateful_reconnect()
            } else {
                None
            };
            let mut full_endpoint = format!("{}{}", endpoint, negotiate.endpoint_query());

            // Browsers cannot set custom headers on WebSocket connections.
            // Append the bearer token as a query parameter per the SignalR convention.
            if let Authentication::Bearer { ref token } = authentication {
                let separator = if full_endpoint.contains('?') {
                    "&"
                } else {
                    "?"
                };
                full_endpoint = format!("{}{}access_token={}", full_endpoint, separator, token);
            }

            Some(ConnectionData {
                endpoint: full_endpoint,
                connection_id: negotiate.connection_id().to_string(),
                protocol_kind,
                recipe,
                stateful_reconnect,
            })
        } else {
            None
        }
    }

    pub async fn post_text(
        endpoint: String,
        authentication: Authentication,
    ) -> Result<String, String> {
        let (s, r) = futures::channel::oneshot::channel::<Result<String, String>>();

        let mut request = ehttp::Request::post(endpoint, vec![]);

        if let Some(value) = authentication.header_value() {
            request.headers.insert("Authorization", value);
        }

        ehttp::fetch(request, move |result| match result {
            Ok(response) if !response.ok => {
                _ = s.send(Err(format!(
                    "the server returned {} {}",
                    response.status, response.status_text
                )));
            }
            Ok(response) => {
                if let Some(text) = response.text() {
                    _ = s.send(Ok(text.to_string()));
                } else {
                    _ = s.send(Err("The returned response has no text body".to_string()));
                }
            }
            Err(e) => {
                _ = s.send(Err(format!("The call failed: {e}")));
            }
        });

        r.await
            .unwrap_or(Err("The request is cancelled.".to_string()))
    }
}
