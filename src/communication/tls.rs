//! Native TLS for secure endpoints: one rustls `ClientConfig` per connection, shared by the
//! negotiate request, the WebSocket and every reconnect of that connection.
//!
//! The crypto provider and the certificate verifier are always explicit, because consumers unify
//! rustls features across crates and the process-default provider is not under this crate's
//! control. A secure endpoint never falls back to plaintext or to a certificate-accepting
//! verifier.

// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

#[cfg(rayx_tls)]
use std::sync::Arc;

use crate::error::Error;

/// Returned for `https`/`wss` endpoints when the crate was built without TLS features.
pub(crate) const TLS_DISABLED: &str = "TLS support is disabled in this build of rayx-signalr; \
enable a verifier feature (tls-platform-verifier or tls-webpki-roots) and a provider feature \
(tls-ring or tls-aws-lc-rs) to connect to https/wss endpoints";

/// The setting a [`TLS_DISABLED`] error names: the crate's Cargo features.
pub(crate) const TLS_FEATURES: &str = "TLS Cargo features";

/// The configuration error of a secure endpoint in a build without TLS features.
pub(crate) fn tls_disabled() -> Error {
    Error::configuration(TLS_FEATURES, TLS_DISABLED)
}

/// TLS options collected by `ConnectionConfiguration`.
#[derive(Clone, Debug, Default)]
pub(crate) struct TlsSettings {
    pub(crate) extra_roots_pem: Option<Vec<u8>>,
    pub(crate) use_webpki_roots: bool,
    pub(crate) client_certificate: Option<ClientCertificatePem>,
    /// `with_tls_config_hook`.
    #[cfg(rayx_tls)]
    pub(crate) config_hook: Option<TlsConfigHook>,
}

/// The hook of `with_tls_config_hook`, run on each connection's config before its first use.
#[cfg(rayx_tls)]
#[derive(Clone)]
pub(crate) struct TlsConfigHook(pub(crate) Arc<dyn Fn(&mut rustls::ClientConfig) + Send + Sync>);

#[cfg(rayx_tls)]
impl std::fmt::Debug for TlsConfigHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TlsConfigHook")
    }
}

/// The client certificate chain and private key (PEM) of `with_client_certificate`. `Debug`
/// never shows the key.
#[derive(Clone)]
#[cfg_attr(not(rayx_tls), allow(dead_code))]
pub(crate) struct ClientCertificatePem {
    pub(crate) chain: Vec<u8>,
    pub(crate) key: Vec<u8>,
}

impl std::fmt::Debug for ClientCertificatePem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientCertificatePem")
            .finish_non_exhaustive()
    }
}

/// The setting client-certificate errors name.
#[cfg_attr(not(rayx_tls), allow(dead_code))]
pub(crate) const CLIENT_CERTIFICATE: &str = "with_client_certificate";

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
    pub(crate) fn provider(self) -> Result<ProviderKind, Error> {
        if self.aws_lc_rs {
            Ok(ProviderKind::AwsLcRs)
        } else if self.ring {
            Ok(ProviderKind::Ring)
        } else {
            Err(tls_disabled())
        }
    }

    /// The platform verifier when enabled (unless `use_webpki_roots` selects bundled roots), else
    /// webpki roots. On Android the platform verifier cannot add roots, so extra roots select
    /// webpki verification over them.
    pub(crate) fn verifier(
        self,
        use_webpki_roots: bool,
        has_extra_roots: bool,
    ) -> Result<VerifierKind, Error> {
        if use_webpki_roots {
            return if self.webpki_roots {
                Ok(VerifierKind::WebPki)
            } else {
                Err(Error::configuration(
                    "use_webpki_roots",
                    "use_webpki_roots() requires the tls-webpki-roots feature",
                ))
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
            Err(tls_disabled())
        }
    }
}

/// TLS options of one connection, validated at `connect`.
#[cfg(rayx_tls)]
struct PreparedTls {
    use_webpki_roots: bool,
    extra_roots: Vec<rustls_pki_types::CertificateDer<'static>>,
    client_certificate: Option<ClientCertificate>,
    config_hook: Option<TlsConfigHook>,
}

/// A parsed client certificate chain and its private key.
#[cfg(rayx_tls)]
struct ClientCertificate {
    chain: Vec<rustls_pki_types::CertificateDer<'static>>,
    key: rustls_pki_types::PrivateKeyDer<'static>,
}

#[cfg(rayx_tls)]
impl ClientCertificate {
    /// Parses the chain (one or more `CERTIFICATE` blocks, the leaf first) and the private key
    /// (PKCS#8, PKCS#1 or SEC1); errors never repeat the key.
    fn parse(pem: &ClientCertificatePem) -> Result<Self, Error> {
        use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

        let chain = CertificateDer::pem_slice_iter(&pem.chain)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Error::configuration(
                    CLIENT_CERTIFICATE,
                    format!("the client certificate chain is not valid PEM: {error}"),
                )
            })?;
        if chain.is_empty() {
            return Err(Error::configuration(
                CLIENT_CERTIFICATE,
                "the client certificate chain contains no PEM certificate",
            ));
        }
        let key = PrivateKeyDer::from_pem_slice(&pem.key).map_err(|_| {
            Error::configuration(
                CLIENT_CERTIFICATE,
                "the client private key is not a PEM private key (PKCS#8, PKCS#1 or SEC1)",
            )
        })?;
        Ok(Self { chain, key })
    }
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
    pub(crate) fn for_connection(secure: bool, settings: &TlsSettings) -> Result<Self, Error> {
        #[cfg(rayx_tls)]
        {
            let extra_roots = match &settings.extra_roots_pem {
                Some(pem) => parse_certificates(pem)?,
                None => Vec::new(),
            };
            let client_certificate = settings
                .client_certificate
                .as_ref()
                .map(ClientCertificate::parse)
                .transpose()?;
            let context = Self {
                prepared: Some(Arc::new(PreparedTls {
                    use_webpki_roots: settings.use_webpki_roots,
                    extra_roots,
                    client_certificate,
                    config_hook: settings.config_hook.clone(),
                })),
                config: Arc::default(),
            };
            // A client certificate is checked against its key before any I/O, also for a
            // plaintext endpoint that may redirect to a secure one.
            if !secure
                && context
                    .prepared
                    .as_ref()
                    .is_some_and(|prepared| prepared.client_certificate.is_some())
            {
                context.config()?;
            }
            if secure {
                context.config()?;
            }
            Ok(context)
        }
        #[cfg(not(rayx_tls))]
        {
            let _ = settings;
            if secure {
                Err(tls_disabled())
            } else {
                Ok(Self::default())
            }
        }
    }

    /// The connection's rustls config, built once on first use.
    #[cfg(rayx_tls)]
    pub(crate) fn config(&self) -> Result<Arc<rustls::ClientConfig>, Error> {
        if let Some(config) = crate::platform::LockExt::locked(&*self.config).as_ref() {
            return Ok(config.clone());
        }
        // Built, and handed to the application's hook, without the slot's lock held. Secure
        // connections build at `connect`, before the context is shared; a redirect's first
        // secure use is a single negotiate request, and should two first uses race, the first
        // config stored serves both.
        let prepared = self
            .prepared
            .as_ref()
            .ok_or_else(|| Error::tls("the connection was not configured for TLS"))?;
        let compiled = CompiledTls::CURRENT;
        let verifier =
            compiled.verifier(prepared.use_webpki_roots, !prepared.extra_roots.is_empty())?;
        let mut config = build_config(
            compiled.provider()?,
            verifier,
            prepared.extra_roots.clone(),
            prepared.client_certificate.as_ref(),
        )?;
        // The application's adjustments (`with_tls_config_hook`), before anything uses it.
        if let Some(hook) = &prepared.config_hook {
            (hook.0)(&mut config);
        }
        let config = Arc::new(config);
        let mut slot = crate::platform::LockExt::locked(&*self.config);
        Ok(slot.get_or_insert(config).clone())
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
    pub(crate) fn websocket_connector(&self) -> Result<tokio_websockets::Connector, Error> {
        #[cfg(rayx_tls)]
        {
            Ok(tokio_websockets::Connector::Rustls(
                tokio_rustls::TlsConnector::from(self.config()?),
            ))
        }
        #[cfg(not(rayx_tls))]
        {
            Err(tls_disabled())
        }
    }
}

/// Whether `error`'s source chain holds a rustls error: a TLS failure that surfaced after the
/// handshake, as a TLS 1.3 server's refusal of a missing client certificate does on the first
/// read. Such failures are `ErrorKind::Tls`, not transport losses.
pub(crate) fn is_tls_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    #[cfg(rayx_tls)]
    {
        let mut current = Some(error);
        while let Some(error) = current {
            if error.downcast_ref::<rustls::Error>().is_some()
                || error
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::get_ref)
                    .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some())
            {
                return true;
            }
            current = error.source();
        }
        false
    }
    #[cfg(not(rayx_tls))]
    {
        let _ = error;
        false
    }
}

/// The error of a failed negotiate or upgrade step: `ErrorKind::Tls` when a TLS failure caused it
/// (see [`is_tls_failure`]), else the given transport error.
pub(crate) fn classify(
    error: impl std::error::Error + Send + Sync + 'static,
    transport: impl FnOnce(String) -> Error,
    context: &str,
) -> Error {
    if is_tls_failure(&error) {
        Error::tls(format!("{context}: the TLS connection failed: {error}")).with_source(error)
    } else {
        transport(format!("{context}: {error}")).with_source(error)
    }
}

/// Parses PEM certificates; fails on malformed PEM or when no certificate is present.
#[cfg(rayx_tls)]
pub(crate) fn parse_certificates(
    pem: &[u8],
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, Error> {
    use rustls_pki_types::{pem::PemObject, CertificateDer};

    const SETTING: &str = "with_additional_root_certificates_pem";
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            Error::configuration(
                SETTING,
                format!("the additional root certificates are not valid PEM: {error}"),
            )
        })?;
    if certificates.is_empty() {
        return Err(Error::configuration(
            SETTING,
            "the additional root certificates contain no PEM certificate",
        ));
    }
    Ok(certificates)
}

#[cfg(rayx_tls)]
pub(crate) fn crypto_provider(kind: ProviderKind) -> Arc<rustls::crypto::CryptoProvider> {
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
    client_certificate: Option<&ClientCertificate>,
) -> Result<rustls::ClientConfig, Error> {
    use rustls::client::danger::ServerCertVerifier;

    let provider = crypto_provider(provider);
    let verifier: Arc<dyn ServerCertVerifier> = match verifier {
        #[cfg(feature = "tls-platform-verifier")]
        VerifierKind::Platform => Arc::new(
            rustls_platform_verifier::Verifier::new(provider.clone()).map_err(|error| {
                Error::tls(format!("cannot create the platform TLS verifier: {error}"))
            })?,
        ),
        #[cfg(all(feature = "tls-platform-verifier", not(target_os = "android")))]
        VerifierKind::PlatformWithExtraRoots => Arc::new(
            rustls_platform_verifier::Verifier::new_with_extra_roots(extra_roots, provider.clone())
                .map_err(|error| {
                    Error::tls(format!("cannot create the platform TLS verifier: {error}"))
                })?,
        ),
        VerifierKind::WebPki => {
            let mut store = rustls::RootCertStore::empty();
            #[cfg(feature = "tls-webpki-roots")]
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            for root in extra_roots {
                store.add(root).map_err(|error| {
                    Error::configuration(
                        "with_additional_root_certificates_pem",
                        format!("cannot trust an additional root certificate: {error}"),
                    )
                })?;
            }
            if store.is_empty() {
                return Err(Error::configuration(
                    TLS_FEATURES,
                    "no trusted root certificates are configured",
                ));
            }
            rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(store),
                provider.clone(),
            )
            .build()
            .map_err(|error| {
                Error::tls(format!("cannot create the webpki TLS verifier: {error}"))
            })?
        }
        #[allow(unreachable_patterns)]
        other => {
            return Err(Error::configuration(
                TLS_FEATURES,
                format!("the {other:?} verifier is not compiled into this build"),
            ))
        }
    };
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            Error::tls(format!(
                "cannot configure the TLS protocol versions: {error}"
            ))
        })?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    // As the .NET client's `HttpConnectionOptions.ClientCertificates`: presented whenever a
    // server asks for one, on every negotiate, WebSocket and HTTP-transport connection.
    match client_certificate {
        Some(certificate) => builder
            .with_client_auth_cert(certificate.chain.clone(), certificate.key.clone_key())
            .map_err(|error| {
                Error::configuration(
                    CLIENT_CERTIFICATE,
                    format!("the client certificate cannot be used with its key: {error}"),
                )
            }),
        None => Ok(builder.with_no_client_auth()),
    }
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
            compiled(true, false, true, true, false).provider().ok(),
            Some(ProviderKind::AwsLcRs)
        );
        assert_eq!(
            compiled(true, false, false, true, false).provider().ok(),
            Some(ProviderKind::Ring)
        );
        let disabled = compiled(true, false, false, false, false)
            .provider()
            .unwrap_err();
        assert_eq!(disabled.kind(), crate::ErrorKind::Configuration);
        assert!(
            disabled.to_string().contains("TLS support is disabled"),
            "{disabled}"
        );
    }

    #[test]
    fn verifier_selection_follows_features_and_options() {
        let both = compiled(true, true, false, true, false);
        assert_eq!(
            both.verifier(false, false).ok(),
            Some(VerifierKind::Platform)
        );
        assert_eq!(
            both.verifier(false, true).ok(),
            Some(VerifierKind::PlatformWithExtraRoots)
        );
        assert_eq!(both.verifier(true, false).ok(), Some(VerifierKind::WebPki));
        assert_eq!(both.verifier(true, true).ok(), Some(VerifierKind::WebPki));

        let webpki_only = compiled(false, true, false, true, false);
        assert_eq!(
            webpki_only.verifier(false, false).ok(),
            Some(VerifierKind::WebPki)
        );

        let platform_only = compiled(true, false, false, true, false);
        let error = platform_only.verifier(true, false).unwrap_err();
        assert_eq!(error.setting(), Some("use_webpki_roots"));
        assert!(error.to_string().contains("tls-webpki-roots"), "{error}");

        let neither = compiled(false, false, false, true, false);
        assert!(neither.verifier(false, false).is_err());
    }

    #[test]
    fn android_extra_roots_select_webpki_verification() {
        let android = compiled(true, false, false, true, true);
        assert_eq!(
            android.verifier(false, false).ok(),
            Some(VerifierKind::Platform)
        );
        assert_eq!(
            android.verifier(false, true).ok(),
            Some(VerifierKind::WebPki)
        );
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
                ..TlsSettings::default()
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
            assert_eq!(CompiledTls::CURRENT.provider().ok(), Some(expected));
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
            assert_eq!(
                error.setting(),
                Some("with_additional_root_certificates_pem"),
                "{error}"
            );
        }

        #[test]
        fn pem_without_certificates_fails() {
            let error =
                TlsContext::for_connection(true, &settings(Some("no certificates here"), false))
                    .err()
                    .expect("PEM without certificates must fail");
            assert_eq!(
                error.setting(),
                Some("with_additional_root_certificates_pem"),
                "{error}"
            );
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
        assert_eq!(error.kind(), crate::ErrorKind::Configuration);
        let error = error.to_string();
        assert!(error.contains(super::TLS_DISABLED), "{error}");
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

#[cfg(all(test, rayx_tls))]
mod client_certificates {
    use std::sync::Arc;

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    };
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{ClientCertificatePem, CompiledTls, TlsContext, TlsSettings};
    use crate::ErrorKind;

    /// A test CA, generated per test (no key is committed).
    fn authority() -> (Issuer<'static, KeyPair>, String) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "client_certificates_ test CA");
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        (Issuer::new(params, key), certificate.pem())
    }

    /// A certificate `issuer` issued: (certificate PEM, key PEM).
    fn issue(issuer: &Issuer<'static, KeyPair>, name: &str, client: bool) -> (String, String) {
        let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, issuer).unwrap();
        (certificate.pem(), key.serialize_pem())
    }

    fn settings(ca: &str, client: Option<(&str, &str)>) -> TlsSettings {
        TlsSettings {
            extra_roots_pem: Some(ca.as_bytes().to_vec()),
            use_webpki_roots: false,
            client_certificate: client.map(|(chain, key)| ClientCertificatePem {
                chain: chain.as_bytes().to_vec(),
                key: key.as_bytes().to_vec(),
            }),
            ..TlsSettings::default()
        }
    }

    /// A TLS server on loopback that requires a client certificate issued by `ca`; it answers
    /// `ok` and reports the DER of the client certificate it saw.
    async fn server_requiring_client_certificates(
        ca: &str,
        server: (String, String),
    ) -> (u16, tokio::task::JoinHandle<Option<Vec<u8>>>) {
        let provider = super::crypto_provider(CompiledTls::CURRENT.provider().unwrap());
        let mut roots = rustls::RootCertStore::empty();
        for root in CertificateDer::pem_slice_iter(ca.as_bytes()) {
            roots.add(root.unwrap()).unwrap();
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                CertificateDer::pem_slice_iter(server.0.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_slice(server.1.as_bytes()).unwrap(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.ok()?;
            let seen = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .map(|leaf| leaf.to_vec());
            let _ = tls.write_all(b"ok").await;
            let _ = tls.shutdown().await;
            seen
        });
        (port, task)
    }

    #[tokio::test]
    async fn client_certificates_are_presented_when_the_server_asks_for_one() {
        let (issuer, ca) = authority();
        let (port, server) =
            server_requiring_client_certificates(&ca, issue(&issuer, "localhost", false)).await;
        let (chain, key) = issue(&issuer, "client_certificates_ client", true);
        let tls = TlsContext::for_connection(true, &settings(&ca, Some((&chain, &key)))).unwrap();
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut stream = crate::communication::negotiate_http::tls_stream(&tls, "localhost", tcp)
            .await
            .unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.unwrap();
        assert_eq!(answer, b"ok");
        let presented = CertificateDer::from_pem_slice(chain.as_bytes()).unwrap();
        assert_eq!(server.await.unwrap(), Some(presented.to_vec()));
    }

    #[tokio::test]
    async fn client_certificates_absent_a_server_that_requires_one_refuses_negotiate_with_tls() {
        let (issuer, ca) = authority();
        let (port, server) =
            server_requiring_client_certificates(&ca, issue(&issuer, "localhost", false)).await;
        let tls = TlsContext::for_connection(true, &settings(&ca, None)).unwrap();
        // TLS 1.3 reports the server's refusal on the first read, after the client finished its
        // handshake; negotiate still reports a TLS failure, not a transport loss.
        let error = crate::communication::negotiate_http::post(
            &format!("https://localhost:{port}/hub/negotiate?negotiateVersion=1"),
            None,
            crate::communication::negotiate_http::Options {
                headers: &[],
                tls: &tls,
                proxy: &crate::communication::proxy::ProxyConfig::Disabled,
                cookies: &crate::CookieJar::new(),
                integrated: None,
                hook: None,
            },
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Tls, "{error}");
        assert_eq!(server.await.unwrap(), None);
    }

    #[test]
    fn client_certificates_invalid_pem_is_a_configuration_error_without_the_key() {
        let (issuer, ca) = authority();
        let (chain, key) = issue(&issuer, "client", true);
        let not_a_key =
            "-----BEGIN PRIVATE KEY-----\nc2VjcmV0LWtleQ==\n-----END PRIVATE KEY-----\n";
        for (chain, key) in [
            ("not pem", key.as_str()),
            ("", key.as_str()),
            (chain.as_str(), not_a_key),
            (chain.as_str(), ""),
        ] {
            let error = TlsContext::for_connection(false, &settings(&ca, Some((chain, key))))
                .err()
                .expect("rejected");
            assert_eq!(error.kind(), ErrorKind::Configuration, "{error}");
            assert_eq!(error.setting(), Some("with_client_certificate"), "{error}");
            assert!(!error.to_string().contains("c2VjcmV0"), "{error}");
        }
    }

    #[test]
    fn client_certificates_a_key_of_another_certificate_is_rejected_before_any_io() {
        let (issuer, ca) = authority();
        let (chain, _) = issue(&issuer, "client", true);
        let (_, other_key) = issue(&issuer, "other", true);
        let error = TlsContext::for_connection(false, &settings(&ca, Some((&chain, &other_key))))
            .err()
            .expect("a mismatched key is rejected");
        assert_eq!(error.setting(), Some("with_client_certificate"), "{error}");
    }

    #[tokio::test]
    async fn client_certificates_reach_the_connection_from_the_configuration() {
        let (issuer, _) = authority();
        let (chain, key) = issue(&issuer, "client", true);
        let (_, other_key) = issue(&issuer, "other", true);
        assert!(
            crate::SignalRClient::new("localhost", "hub", |configuration| {
                configuration.with_client_certificate(chain.clone(), key.clone());
            })
            .is_ok()
        );
        let error = crate::SignalRClient::new("localhost", "hub", |configuration| {
            configuration.with_client_certificate(chain.clone(), other_key.clone());
        })
        .err()
        .expect("new validates the client certificate");
        assert_eq!(error.setting(), Some("with_client_certificate"), "{error}");
    }
}

#[cfg(all(test, rayx_tls))]
mod transport_hooks_tls {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::{TlsConfigHook, TlsContext, TlsSettings};

    #[test]
    fn transport_hooks_adjust_each_connection_config_once_before_use() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let settings = TlsSettings {
            config_hook: Some(TlsConfigHook(Arc::new(move |config| {
                counted.fetch_add(1, Ordering::SeqCst);
                config.enable_sni = false;
                config.alpn_protocols = vec![b"http/1.1".to_vec()];
            }))),
            ..TlsSettings::default()
        };
        let context = TlsContext::for_connection(true, &settings).unwrap();
        let config = context.config().unwrap();
        assert!(!config.enable_sni);
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
        // The one adjusted config serves every request and reconnect of the connection.
        assert!(Arc::ptr_eq(&config, &context.clone().config().unwrap()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Each connection builds, and adjusts, its own.
        TlsContext::for_connection(true, &settings).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // A plaintext connection adjusts nothing until it first needs TLS.
        let plaintext = TlsContext::for_connection(false, &settings).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        plaintext.config().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
