//! TLS: over the service's HTTPS endpoint, a client that trusts the run's CA through
//! `with_additional_root_certificates_pem` negotiates, connects over `wss` and invokes; the same
//! client without the extra root fails certificate verification and never reaches the hub.
//!
//! On Android the untrusted client verifies against the bundled Mozilla roots: the platform
//! verifier needs the JVM context an app provides (`rayx_signalr::tls::android::init_with_env`),
//! which a test binary run through `adb shell` does not have.
//!
//! Inside the Android test app (`RAYX_SIGNALR_E2E_TLS_PLATFORM_TRUST=1`) the platform verifier is
//! initialized and the device trusts the run's CA as a user CA (the app installs it through device
//! policy, as an organization installs a private CA): the trusted client adds no root, and the
//! rejected endpoint is one whose certificate a second, untrusted CA issued
//! (`RAYX_SIGNALR_E2E_UNTRUSTED_TLS_URL`).

use base64::{engine::general_purpose, Engine};

use crate::support::{self, Protocol};

/// Whether the client runs inside the Android test app, trusting the run's CA through the
/// platform (the device's user CA store) instead of an extra root.
fn platform_trust() -> bool {
    std::env::var("RAYX_SIGNALR_E2E_TLS_PLATFORM_TRUST").is_ok_and(|value| value == "1")
}

pub(crate) async fn scenario(protocol: Protocol) {
    let platform_trust = platform_trust();
    let endpoint = support::parse_endpoint(&support::required_variable("RAYX_SIGNALR_E2E_TLS_URL"));
    assert!(endpoint.secure, "the TLS endpoint must be https");
    let ca = general_purpose::STANDARD
        .decode(support::required_variable(
            "RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64",
        ))
        .expect("the CA is base64");

    let trusted_ca = ca.clone();
    let mut trusted =
        rayx_signalr::SignalRClient::connect_with(&endpoint.host, "test-tls", |configuration| {
            endpoint.apply(configuration);
            support::apply_protocol(configuration, protocol);
            if !platform_trust {
                configuration.with_additional_root_certificates_pem(trusted_ca.clone());
            }
        })
        .await
        .unwrap_or_else(|error| panic!("the trusted TLS connection failed: {error}"));
    let echoed = trusted
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(443);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 443);
    trusted.disconnect();

    // Inside the app the platform trusts the run's CA, so the rejected certificate is the second
    // CA's; elsewhere it is the service's own certificate without the extra root.
    let rejected = if platform_trust {
        support::parse_endpoint(&support::required_variable(
            "RAYX_SIGNALR_E2E_UNTRUSTED_TLS_URL",
        ))
    } else {
        endpoint.clone()
    };
    let error =
        rayx_signalr::SignalRClient::connect_with(&rejected.host, "test-tls", |configuration| {
            rejected.apply(configuration);
            support::apply_protocol(configuration, protocol);
            #[cfg(target_os = "android")]
            if !platform_trust {
                configuration.use_webpki_roots();
            }
        })
        .await
        .err()
        .expect("a client without the test CA must reject the server certificate");
    assert_eq!(
        error.kind(),
        rayx_signalr::ErrorKind::Tls,
        "expected a certificate trust failure: {error}"
    );

    // Only the trusted client's negotiate reached the service.
    let mut probe = support::connect("test", protocol, |_| {}).await;
    let negotiates = probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-tls".to_string());
        })
        .await
        .unwrap();
    assert_eq!(
        negotiates, 1,
        "the untrusted client must never reach the hub"
    );
    probe.disconnect();
}

crate::protocol_tests!("tls");
