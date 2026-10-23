//! Client certificates, as the .NET client's `HttpConnectionOptions.ClientCertificates`: the test
//! service's `/test-mtls` hub refuses every request (negotiate, the WebSocket upgrade, Long
//! Polling and Server-Sent Events requests) that presented no client certificate the run's CA
//! issued. With `with_client_certificate` the client connects and invokes over each transport,
//! and the hub sees the certificate's subject on the WebSocket's connection; without it the
//! negotiate request is refused with 403.

use base64::{engine::general_purpose, Engine};
use dxn_signalr::{ErrorKind, SignalRClient, Transports};

use crate::support::{self, Protocol};

fn decoded(name: &str) -> Vec<u8> {
    general_purpose::STANDARD
        .decode(support::required_variable(name))
        .unwrap_or_else(|_| panic!("{name} is base64"))
}

async fn scenario(protocol: Protocol) {
    let endpoint = support::parse_endpoint(&support::required_variable("DXN_SIGNALR_E2E_TLS_URL"));
    let ca = decoded("DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64");
    let chain = decoded("DXN_SIGNALR_E2E_CLIENT_CERT_PEM_BASE64");
    let key = decoded("DXN_SIGNALR_E2E_CLIENT_KEY_PEM_BASE64");

    let mut transports = vec![Transports::WEB_SOCKETS, Transports::LONG_POLLING];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        let mut client =
            SignalRClient::connect_with(&endpoint.host, "test-mtls", |configuration| {
                endpoint.apply(configuration);
                support::apply_protocol(configuration, protocol);
                configuration.with_additional_root_certificates_pem(ca.clone());
                configuration.with_client_certificate(chain.clone(), key.clone());
                configuration.with_transports(transport);
            })
            .await
            .unwrap_or_else(|error| {
                panic!("{transport:?}: the client certificate was refused: {error}")
            });
        let echoed = client
            .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
                arguments.argument(7);
            })
            .await
            .unwrap();
        assert_eq!(echoed, 7, "{transport:?}");
        if transport == Transports::WEB_SOCKETS {
            let subject = client
                .invoke::<String>("ClientCertificateSubject".to_string())
                .await
                .unwrap();
            assert_eq!(subject, "CN=dXn.SignalR E2E Client");
        }
        client.disconnect();
    }

    // Without a client certificate the hub refuses the negotiate request.
    let error = SignalRClient::connect_with(&endpoint.host, "test-mtls", |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_additional_root_certificates_pem(ca.clone());
    })
    .await
    .err()
    .expect("a client without a certificate is refused");
    assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    assert_eq!(error.status(), Some(403), "{error}");
}

crate::protocol_tests!("client-certificate");
