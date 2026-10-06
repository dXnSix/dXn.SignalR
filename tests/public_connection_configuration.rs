//! Consumers name `ConnectionConfiguration` to forward connection options through their own helpers.

use rayx_signalr::{ConnectionConfiguration, SignalRClient};

fn configure_secure_bearer(config: &mut ConnectionConfiguration) {
    config.secure();
    config.with_port(5220);
    config.authenticate_bearer("token".to_string());
}

async fn connect_through_consumer_helper<F>(
    endpoint: &str,
    hub: &str,
    options: F,
) -> Result<SignalRClient, String>
where
    F: FnMut(&mut ConnectionConfiguration),
{
    SignalRClient::connect_with(endpoint, hub, options).await
}

#[test]
fn consumer_helpers_can_name_connection_configuration() {
    let options: fn(&mut ConnectionConfiguration) = configure_secure_bearer;
    let connect = connect_through_consumer_helper("localhost", "test", options);
    drop(connect);
}
