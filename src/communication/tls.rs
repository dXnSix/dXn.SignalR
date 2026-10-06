//! Native TLS for secure endpoints: one rustls `ClientConfig` per connection, shared by the
//! negotiate request, the WebSocket and every reconnect of that connection.
//!
//! The crypto provider and the certificate verifier are always explicit, because consumers unify
//! rustls features across crates and the process-default provider is not under this crate's
//! control. A secure endpoint never falls back to plaintext or to a certificate-accepting
//! verifier.

#[cfg(rayx_tls)]
use std::sync::Arc;

/// Returned for `https`/`wss` endpoints when the crate was built without TLS features.
pub(crate) const TLS_DISABLED: &str = "TLS support is disabled in this build of rayx-signalr; \
enable a verifier feature (tls-platform-verifier or tls-webpki-roots) and a provider feature \
(tls-ring or tls-aws-lc-rs) to connect to https/wss endpoints";

/// TLS options collected by `ConnectionConfiguration`.
#[derive(Clone, Debug, Default)]
pub(crate) struct TlsSettings {
    pub(crate) extra_roots_pem: Option<Vec<u8>>,
    pub(crate) use_webpki_roots: bool,
}

/// The certificate verifier a connection uses.
#[cfg(any(rayx_tls, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerifierKind {
    /// The operating system's trust store (`rustls-platform-verifier`).
    Platform,
    /// The operating system's trust store plus the configured extra roots.
    PlatformWithExtraRoots,
    /// webpki over the extra roots, plus the bundled Mozilla roots when compiled in.
    WebPki,
}

/// The rustls crypto provider a connection uses.
#[cfg(any(rayx_tls, test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderKind {
    AwsLcRs,
    Ring,
}

/// Which TLS features this build compiled in.
#[cfg(any(rayx_tls, test))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct CompiledTls {
    pub(crate) platform_verifier: bool,
    pub(crate) webpki_roots: bool,
    pub(crate) aws_lc_rs: bool,
    pub(crate) ring: bool,
    pub(crate) android: bool,
}

#[cfg(any(rayx_tls, test))]
impl CompiledTls {
    #[cfg(rayx_tls)]
    pub(crate) const CURRENT: CompiledTls = CompiledTls {
        platform_verifier: cfg!(feature = "tls-platform-verifier"),
        webpki_roots: cfg!(feature = "tls-webpki-roots"),
        aws_lc_rs: cfg!(feature = "tls-aws-lc-rs"),
        ring: cfg!(feature = "tls-ring"),
        android: cfg!(target_os = "android"),
    };

    /// aws-lc-rs when enabled, else ring.
    pub(crate) fn provider(self) -> Result<ProviderKind, String> {
        if self.aws_lc_rs {
            Ok(ProviderKind::AwsLcRs)
        } else if self.ring {
            Ok(ProviderKind::Ring)
        } else {
            Err(TLS_DISABLED.to_string())
        }
    }

    /// The platform verifier when enabled (unless `use_webpki_roots` selects bundled roots), else
    /// webpki roots. On Android the platform verifier cannot add roots, so extra roots select
    /// webpki verification over them.
    pub(crate) fn verifier(
        self,
        use_webpki_roots: bool,
        has_extra_roots: bool,
    ) -> Result<VerifierKind, String> {
        if use_webpki_roots {
            return if self.webpki_roots {
                Ok(VerifierKind::WebPki)
            } else {
                Err("use_webpki_roots() requires the tls-webpki-roots feature".to_string())
            };
        }
        if self.platform_verifier {
            return Ok(match (has_extra_roots, self.android) {
                (false, _) => VerifierKind::Platform,
                (true, false) => VerifierKind::PlatformWithExtraRoots,
                (true, true) => VerifierKind::WebPki,
            });
        }
        if self.webpki_roots {
            Ok(VerifierKind::WebPki)
        } else {
            Err(TLS_DISABLED.to_string())
        }
    }
}

/// TLS options of one connection, validated at `connect`.
#[cfg(rayx_tls)]
struct PreparedTls {
    use_webpki_roots: bool,
    extra_roots: Vec<rustls_pki_types::CertificateDer<'static>>,
}

/// The TLS state of one connection. Secure endpoints build the rustls config at `connect`; a
/// plaintext endpoint builds it on first secure use (a negotiate redirect to an `https` service).
/// Either way the connection keeps exactly one config for all of its requests and reconnects.
#[derive(Clone, Default)]
pub(crate) struct TlsContext {
    #[cfg(rayx_tls)]
    prepared: Option<Arc<PreparedTls>>,
    #[cfg(rayx_tls)]
    config: Arc<std::sync::Mutex<Option<Arc<rustls::ClientConfig>>>>,
}

impl TlsContext {
    /// Validates the TLS options and, for a secure endpoint, builds the connection's config.
    /// Runs at `connect`, before any I/O.
    pub(crate) fn for_connection(secure: bool, settings: &TlsSettings) -> Result<Self, String> {
        #[cfg(rayx_tls)]
        {
            let extra_roots = match &settings.extra_roots_pem {
                Some(pem) => parse_certificates(pem)?,
                None => Vec::new(),
            };
            let context = Self {
                prepared: Some(Arc::new(PreparedTls {
                    use_webpki_roots: settings.use_webpki_roots,
                    extra_roots,
                })),
                config: Arc::default(),
            };
            if secure {
                context.config()?;
            }
            Ok(context)
        }
        #[cfg(not(rayx_tls))]
        {
            let _ = settings;
            if secure {
                Err(TLS_DISABLED.to_string())
            } else {
                Ok(Self::default())
            }
        }
    }

    /// The connection's rustls config, built once on first use.
    #[cfg(rayx_tls)]
    pub(crate) fn config(&self) -> Result<Arc<rustls::ClientConfig>, String> {
        let mut slot = self.config.lock().expect("TLS config slot poisoned");
        if let Some(config) = slot.as_ref() {
            return Ok(config.clone());
        }
        let prepared = self
            .prepared
            .as_ref()
            .ok_or_else(|| "The connection was not configured for TLS".to_string())?;
        let compiled = CompiledTls::CURRENT;
        let verifier =
            compiled.verifier(prepared.use_webpki_roots, !prepared.extra_roots.is_empty())?;
        let config = Arc::new(build_config(
            compiled.provider()?,
            verifier,
            prepared.extra_roots.clone(),
        )?);
        *slot = Some(config.clone());
        Ok(config)
    }

    /// Whether the connection has built its rustls config yet.
    #[cfg(all(rayx_tls, test))]
    pub(crate) fn is_built(&self) -> bool {
        self.config
            .lock()
            .expect("TLS config slot poisoned")
            .is_some()
    }

    /// The WebSocket connector for a `wss` endpoint.
    pub(crate) fn websocket_connector(&self) -> Result<tokio_websockets::Connector, String> {
        #[cfg(rayx_tls)]
        {
            Ok(tokio_websockets::Connector::Rustls(
                tokio_rustls::TlsConnector::from(self.config()?),
            ))
        }
        #[cfg(not(rayx_tls))]
        {
            Err(TLS_DISABLED.to_string())
        }
    }
}

/// Parses PEM certificates; fails on malformed PEM or when no certificate is present.
#[cfg(rayx_tls)]
pub(crate) fn parse_certificates(
    pem: &[u8],
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, String> {
    use rustls_pki_types::{pem::PemObject, CertificateDer};

    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("The additional root certificates are not valid PEM: {error}"))?;
    if certificates.is_empty() {
        return Err("The additional root certificates contain no PEM certificate".to_string());
    }
    Ok(certificates)
}

#[cfg(rayx_tls)]
fn crypto_provider(kind: ProviderKind) -> Arc<rustls::crypto::CryptoProvider> {
    match kind {
        #[cfg(feature = "tls-aws-lc-rs")]
        ProviderKind::AwsLcRs => Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        #[cfg(feature = "tls-ring")]
        ProviderKind::Ring => Arc::new(rustls::crypto::ring::default_provider()),
        #[allow(unreachable_patterns)]
        _ => unreachable!("the provider kind is selected from compiled features"),
    }
}

#[cfg(rayx_tls)]
fn build_config(
    provider: ProviderKind,
    verifier: VerifierKind,
    extra_roots: Vec<rustls_pki_types::CertificateDer<'static>>,
) -> Result<rustls::ClientConfig, String> {
    use rustls::client::danger::ServerCertVerifier;

    let provider = crypto_provider(provider);
    let verifier: Arc<dyn ServerCertVerifier> = match verifier {
        #[cfg(feature = "tls-platform-verifier")]
        VerifierKind::Platform => Arc::new(
            rustls_platform_verifier::Verifier::new(provider.clone())
                .map_err(|error| format!("Cannot create the platform TLS verifier: {error}"))?,
        ),
        #[cfg(all(feature = "tls-platform-verifier", not(target_os = "android")))]
        VerifierKind::PlatformWithExtraRoots => Arc::new(
            rustls_platform_verifier::Verifier::new_with_extra_roots(extra_roots, provider.clone())
                .map_err(|error| format!("Cannot create the platform TLS verifier: {error}"))?,
        ),
        VerifierKind::WebPki => {
            let mut store = rustls::RootCertStore::empty();
            #[cfg(feature = "tls-webpki-roots")]
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            for root in extra_roots {
                store.add(root).map_err(|error| {
                    format!("Cannot trust an additional root certificate: {error}")
                })?;
            }
            if store.is_empty() {
                return Err("No trusted root certificates are configured".to_string());
            }
            rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(store),
                provider.clone(),
            )
            .build()
            .map_err(|error| format!("Cannot create the webpki TLS verifier: {error}"))?
        }
        #[allow(unreachable_patterns)]
        other => {
            return Err(format!(
                "The {other:?} verifier is not compiled into this build"
            ))
        }
    };
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("Cannot configure TLS protocol versions: {error}"))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth())
}

#[cfg(test)]
mod tls_configuration {
    use super::{CompiledTls, ProviderKind, TlsContext, TlsSettings, VerifierKind};

    fn compiled(platform: bool, webpki: bool, aws: bool, ring: bool, android: bool) -> CompiledTls {
        CompiledTls {
            platform_verifier: platform,
            webpki_roots: webpki,
            aws_lc_rs: aws,
            ring,
            android,
        }
    }

    #[test]
    fn provider_prefers_aws_lc_rs_then_ring() {
        assert_eq!(
            compiled(true, false, true, true, false).provider(),
            Ok(ProviderKind::AwsLcRs)
        );
        assert_eq!(
            compiled(true, false, false, true, false).provider(),
            Ok(ProviderKind::Ring)
        );
        let disabled = compiled(true, false, false, false, false)
            .provider()
            .unwrap_err();
        assert!(disabled.contains("TLS support is disabled"), "{disabled}");
    }

    #[test]
    fn verifier_selection_follows_features_and_options() {
        let both = compiled(true, true, false, true, false);
        assert_eq!(both.verifier(false, false), Ok(VerifierKind::Platform));
        assert_eq!(
            both.verifier(false, true),
            Ok(VerifierKind::PlatformWithExtraRoots)
        );
        assert_eq!(both.verifier(true, false), Ok(VerifierKind::WebPki));
        assert_eq!(both.verifier(true, true), Ok(VerifierKind::WebPki));

        let webpki_only = compiled(false, true, false, true, false);
        assert_eq!(webpki_only.verifier(false, false), Ok(VerifierKind::WebPki));

        let platform_only = compiled(true, false, false, true, false);
        let error = platform_only.verifier(true, false).unwrap_err();
        assert!(error.contains("tls-webpki-roots"), "{error}");

        let neither = compiled(false, false, false, true, false);
        assert!(neither.verifier(false, false).is_err());
    }

    #[test]
    fn android_extra_roots_select_webpki_verification() {
        let android = compiled(true, false, false, true, true);
        assert_eq!(android.verifier(false, false), Ok(VerifierKind::Platform));
        assert_eq!(android.verifier(false, true), Ok(VerifierKind::WebPki));
    }

    #[test]
    fn plaintext_connections_need_no_tls_configuration() {
        assert!(TlsContext::for_connection(false, &TlsSettings::default()).is_ok());
    }

    #[cfg(rayx_tls)]
    mod with_tls {
        use super::super::{CompiledTls, ProviderKind, TlsContext, TlsSettings};

        /// A static self-signed test CA (public certificate only; no key is checked in).
        const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBrTCCAVOgAwIBAgIUJ+hn8ImrbyJLPKZcD58QVLhW+y0wCgYIKoZIzj0EAwIw
JDEiMCAGA1UEAwwZcmF5eC1zaWduYWxyIHVuaXQgdGVzdCBDQTAeFw0yNjEwMDUy
MjA5MTRaFw00NjA5MzAyMjA5MTRaMCQxIjAgBgNVBAMMGXJheXgtc2lnbmFsciB1
bml0IHRlc3QgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARVjTEexxo1zZfW
jxtRcFWaqAmQMKCrdwTYnfB+ZwysvekquAutMxM4o44z91d8XD0b31lGKT9N1Y7t
gFICkF/+o2MwYTAdBgNVHQ4EFgQUVtnnIhO/ubjolFlMc/PvXXIZsEIwHwYDVR0j
BBgwFoAUVtnnIhO/ubjolFlMc/PvXXIZsEIwDwYDVR0TAQH/BAUwAwEB/zAOBgNV
HQ8BAf8EBAMCAQYwCgYIKoZIzj0EAwIDSAAwRQIhAKCeJMGy4byidGlj8UUCGzfy
ibLUFmTjFJJGwpHGvYImAiB8uRoJylrWYkrBkpR505ENRwNGf5TVaZjgBeJtJS7G
NQ==
-----END CERTIFICATE-----
";

        fn settings(pem: Option<&str>, use_webpki_roots: bool) -> TlsSettings {
            TlsSettings {
                extra_roots_pem: pem.map(|pem| pem.as_bytes().to_vec()),
                use_webpki_roots,
            }
        }

        #[test]
        fn secure_connections_build_one_config_with_the_selected_provider() {
            let context = TlsContext::for_connection(true, &settings(None, false)).unwrap();
            assert!(context.config().is_ok());
            assert!(context.websocket_connector().is_ok());
            let expected = if cfg!(feature = "tls-aws-lc-rs") {
                ProviderKind::AwsLcRs
            } else {
                ProviderKind::Ring
            };
            assert_eq!(CompiledTls::CURRENT.provider(), Ok(expected));
        }

        #[test]
        fn plaintext_connections_build_one_config_only_on_first_secure_use() {
            let context = TlsContext::for_connection(false, &settings(None, false)).unwrap();
            assert!(!context.is_built());
            let first = context.config().unwrap();
            let shared = context.clone();
            assert!(std::sync::Arc::ptr_eq(&first, &shared.config().unwrap()));
        }

        #[test]
        fn secure_connections_build_their_config_at_connect() {
            let context = TlsContext::for_connection(true, &settings(None, false)).unwrap();
            assert!(context.is_built());
        }

        #[test]
        fn invalid_pem_fails_before_any_io() {
            let malformed = "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n";
            let error = TlsContext::for_connection(false, &settings(Some(malformed), false))
                .err()
                .expect("malformed PEM must fail");
            assert!(error.contains("not valid PEM"), "{error}");
        }

        #[test]
        fn pem_without_certificates_fails() {
            let error =
                TlsContext::for_connection(true, &settings(Some("no certificates here"), false))
                    .err()
                    .expect("PEM without certificates must fail");
            assert!(error.contains("no PEM certificate"), "{error}");
        }

        #[test]
        fn extra_roots_build_a_secure_config() {
            let context =
                TlsContext::for_connection(true, &settings(Some(TEST_CA_PEM), false)).unwrap();
            assert!(context.config().is_ok());
        }

        #[cfg(feature = "tls-webpki-roots")]
        #[test]
        fn use_webpki_roots_builds_a_bundled_roots_config() {
            let context = TlsContext::for_connection(true, &settings(None, true)).unwrap();
            assert!(context.config().is_ok());
            let with_extra =
                TlsContext::for_connection(true, &settings(Some(TEST_CA_PEM), true)).unwrap();
            assert!(with_extra.config().is_ok());
        }
    }

    #[cfg(not(rayx_tls))]
    #[test]
    fn secure_endpoints_without_tls_features_return_the_disabled_error() {
        let error = TlsContext::for_connection(true, &TlsSettings::default())
            .err()
            .expect("secure endpoints need TLS features");
        assert_eq!(error, super::TLS_DISABLED);
        for feature in [
            "tls-platform-verifier",
            "tls-webpki-roots",
            "tls-ring",
            "tls-aws-lc-rs",
        ] {
            assert!(error.contains(feature), "{error}");
        }
    }
}
