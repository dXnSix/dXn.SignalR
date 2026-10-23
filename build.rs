//! Derives the `dxn_tls` cfg: native targets with one TLS verifier feature and one TLS provider
//! feature can open secure endpoints; every other build is plaintext-only (browsers use the
//! platform's TLS and never compile the rustls code). Also derives `dxn_pac`: the `proxy-pac`
//! feature on native targets other than iOS, where the Boa engine's `time` dependency
//! (`num_threads`) does not build. And `dxn_http3`: the `http3` feature where `dxn_tls` holds
//! (QUIC always runs TLS).

fn main() {
    println!("cargo::rustc-check-cfg=cfg(dxn_tls)");
    println!("cargo::rustc-check-cfg=cfg(dxn_pac)");
    println!("cargo::rustc-check-cfg=cfg(dxn_http3)");
    let enabled = |feature: &str| std::env::var_os(format!("CARGO_FEATURE_{feature}")).is_some();
    let native = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32");
    let verifier = enabled("TLS_PLATFORM_VERIFIER") || enabled("TLS_WEBPKI_ROOTS");
    let provider = enabled("TLS_RING") || enabled("TLS_AWS_LC_RS");
    if native && verifier && provider {
        println!("cargo::rustc-cfg=dxn_tls");
        if enabled("HTTP3") {
            println!("cargo::rustc-cfg=dxn_http3");
        }
    }
    let ios = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("ios");
    if native && !ios && enabled("PROXY_PAC") {
        println!("cargo::rustc-cfg=dxn_pac");
    }
}
