//! Operating-system proxy settings, as the .NET client's default proxy reads them when no proxy
//! environment variable is set: Windows through WinHTTP (the current user's settings, with
//! auto-detection and auto-config scripts, as `HttpWindowsProxy`), macOS and iOS through
//! CFNetwork (the system settings, auto-config scripts run by CFNetwork, as `MacProxy`), and
//! Android through `java.net.ProxySelector` (after `tls::android::init_with_env`). Elsewhere
//! (Linux) the environment variables are the system's settings. Each platform's reader runs on a
//! blocking thread and maps what the system reports to the routes a connection tries in order;
//! settings that cannot be read connect directly, as .NET does.

// Platform code must not panic on system input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use super::proxy::{ProxyUrl, Route};
use crate::error::Error;

/// A proxy entry CFNetwork reports for a URL: a route, or an auto-config script to run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(any(test, target_os = "macos", target_os = "ios")),
    allow(dead_code)
)]
pub(crate) enum OsRoute {
    Route(Route),
    AutoConfigUrl(String),
    AutoConfigScript(String),
}

/// Reads the system's proxies for one URL. Blocking.
pub(crate) trait OsProxyReader: Send + Sync {
    /// The routes for `url` (`http(s)://host[:port]/`) of a connection to `host`.
    fn routes(&self, url: &str, secure: bool, host: &str) -> Result<Vec<Route>, Error>;
}

/// The platform's reader, or `None` where the environment variables are the system's settings.
pub(crate) fn platform() -> Option<&'static dyn OsProxyReader> {
    #[cfg(windows)]
    {
        Some(&winhttp::WinHttp)
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        Some(&cfnetwork::CfNetwork)
    }
    #[cfg(all(target_os = "android", feature = "tls-platform-verifier"))]
    {
        java::reader()
    }
    #[cfg(not(any(
        windows,
        target_os = "macos",
        target_os = "ios",
        all(target_os = "android", feature = "tls-platform-verifier")
    )))]
    {
        None
    }
}

/// The routes `reader` reports for a connection to `host:port`; no route, or settings that cannot
/// be read, mean `DIRECT`, as .NET connects directly when its system proxy fails.
pub(crate) async fn routes(
    reader: &'static dyn OsProxyReader,
    secure: bool,
    host: &str,
    port: u16,
) -> Vec<Route> {
    let target = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let url = super::proxy::target_url(secure, &target, port);
    let mut routes = match tokio::task::spawn_blocking(move || reader.routes(&url, secure, &target))
        .await
    {
        Ok(Ok(routes)) => routes,
        Ok(Err(error)) => {
            log::warn!("{error}; connecting directly");
            Vec::new()
        }
        Err(error) => {
            log::warn!("Reading the system proxy settings stopped ({error}); connecting directly");
            Vec::new()
        }
    };
    if routes.is_empty() {
        routes.push(Route::Direct);
    }
    routes
}

/// Whether `host` is a loopback name or address, which the system proxy never serves (.NET's
/// `HttpWindowsProxy` bypasses loopback).
#[cfg_attr(not(any(test, windows)), allow(dead_code))]
pub(crate) fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// The routes of a WinHTTP/WinINet proxy list (`host:port`, `http=host:port;https=host:port`, ...)
/// for a plain or `secure` endpoint, as .NET's `MultiProxy` parses it: entries separated by `;`
/// or whitespace, `http=` entries for plain endpoints only, `https=` for secure ones, unprefixed
/// ones for both, other prefixes (`socks=`, `ftp=`) skipped; every entry is an HTTP proxy.
#[cfg_attr(not(any(test, windows)), allow(dead_code))]
pub(crate) fn proxy_list_routes(list: &str, secure: bool) -> Vec<Route> {
    list.split(|character: char| character == ';' || character.is_whitespace())
        .filter(|entry| !entry.is_empty())
        .filter_map(|entry| {
            let address = match entry.split_once('=') {
                Some((scheme, address)) if scheme.eq_ignore_ascii_case("http") => {
                    (!secure).then_some(address)?
                }
                Some((scheme, address)) if scheme.eq_ignore_ascii_case("https") => {
                    secure.then_some(address)?
                }
                Some(_) => return None,
                None => entry,
            };
            let address = address
                .strip_prefix("http://")
                .or_else(|| address.strip_prefix("https://"))
                .unwrap_or(address);
            ProxyUrl::parse(&format!("http://{address}"))
                .ok()
                .map(Route::Proxy)
        })
        .collect()
}

/// Whether a WinINet bypass list (`<local>;*.corp.example;10.*`) exempts `host`: `<local>`
/// matches host names without a dot, other entries match with `*` wildcards, ignoring case.
#[cfg_attr(not(any(test, windows)), allow(dead_code))]
pub(crate) fn bypassed(bypass: &str, host: &str) -> bool {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    bypass
        .split(|character: char| character == ';' || character.is_whitespace())
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            let entry = entry.to_ascii_lowercase();
            match entry.as_str() {
                "<local>" => !host.contains('.') && !host.contains(':'),
                "<-loopback>" => false,
                pattern => wildcard_match(pattern, &host),
            }
        })
}

/// Matches `text` against `pattern`, where `*` matches any run of characters.
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    let parts: Vec<&str> = parts.collect();
    let Some((last, middle)) = parts.split_last() else {
        return rest.is_empty();
    };
    for part in middle {
        match rest.find(part) {
            Some(index) => rest = rest.get(index + part.len()..).unwrap_or_default(),
            None => return false,
        }
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

/// The routes of the entries `java.net.ProxySelector` returned (type name, host, port): `DIRECT`,
/// `HTTP` proxies, and `SOCKS` proxies (Java's sockets speak SOCKS5).
#[cfg_attr(
    not(any(test, all(target_os = "android", feature = "tls-platform-verifier"))),
    allow(dead_code)
)]
pub(crate) fn java_routes(entries: &[(String, Option<String>, i32)]) -> Vec<Route> {
    entries
        .iter()
        .filter_map(|(kind, host, port)| {
            let scheme = match kind.as_str() {
                "DIRECT" => return Some(Route::Direct),
                "HTTP" => "http",
                "SOCKS" => "socks5",
                _ => return None,
            };
            let host = host.as_deref()?;
            let host = if host.contains(':') && !host.starts_with('[') {
                format!("[{host}]")
            } else {
                host.to_string()
            };
            ProxyUrl::parse(&format!("{scheme}://{host}:{port}"))
                .ok()
                .map(Route::Proxy)
        })
        .collect()
}

/// One proxy dictionary of `CFNetworkCopyProxiesForURL`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(
    not(any(test, target_os = "macos", target_os = "ios")),
    allow(dead_code)
)]
pub(crate) struct CfProxy {
    /// The `kCFProxyTypeKey` value (`kCFProxyTypeHTTP`, ...).
    pub(crate) kind: String,
    pub(crate) host: Option<String>,
    pub(crate) port: Option<i64>,
    pub(crate) auto_config_url: Option<String>,
    pub(crate) auto_config_script: Option<String>,
}

/// The routes of CFNetwork's proxies for a URL: `kCFProxyTypeNone` is `DIRECT`, `HTTP` and
/// `HTTPS` are HTTP proxies (`HTTPS` is the proxy for secure URLs), `SOCKS` is SOCKS5, and the
/// auto-config types carry a script's URL or text for CFNetwork to run.
#[cfg_attr(
    not(any(test, target_os = "macos", target_os = "ios")),
    allow(dead_code)
)]
pub(crate) fn cf_routes(proxies: &[CfProxy]) -> Vec<OsRoute> {
    proxies
        .iter()
        .filter_map(|proxy| {
            let scheme = match proxy.kind.as_str() {
                "kCFProxyTypeNone" => return Some(OsRoute::Route(Route::Direct)),
                "kCFProxyTypeHTTP" | "kCFProxyTypeHTTPS" => "http",
                "kCFProxyTypeSOCKS" => "socks5",
                "kCFProxyTypeAutoConfigurationURL" => {
                    return proxy.auto_config_url.clone().map(OsRoute::AutoConfigUrl)
                }
                "kCFProxyTypeAutoConfigurationJavaScript" => {
                    return proxy
                        .auto_config_script
                        .clone()
                        .map(OsRoute::AutoConfigScript)
                }
                _ => return None,
            };
            let host = proxy.host.as_deref()?;
            let host = if host.contains(':') && !host.starts_with('[') {
                format!("[{host}]")
            } else {
                host.to_string()
            };
            let port = proxy
                .port
                .and_then(|port| u16::try_from(port).ok())
                .filter(|port| *port != 0);
            let url = match port {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            };
            ProxyUrl::parse(&url)
                .ok()
                .map(|proxy| OsRoute::Route(Route::Proxy(proxy)))
        })
        .collect()
}

// The readers that report it: WinHTTP, CFNetwork, and Android's (with the platform verifier's JVM).
#[cfg_attr(
    not(any(
        windows,
        target_os = "macos",
        target_os = "ios",
        all(target_os = "android", feature = "tls-platform-verifier")
    )),
    allow(dead_code)
)]
fn system_failure(message: impl std::fmt::Display) -> Error {
    Error::proxy(
        None,
        format!("the system proxy settings could not be read: {message}"),
    )
}

#[cfg(windows)]
mod winhttp {
    //! WinHTTP: the current user's Internet settings (`WinHttpGetIEProxyConfigForCurrentUser`)
    //! and, for auto-detection or an auto-config URL, `WinHttpGetProxyForUrl`, as .NET's
    //! `HttpWindowsProxy`.

    use std::{
        ptr,
        sync::{Mutex, OnceLock},
        time::{Duration, Instant},
    };

    use windows_sys::Win32::{
        Foundation::{GetLastError, GlobalFree},
        Networking::WinHttp::{
            WinHttpGetIEProxyConfigForCurrentUser, WinHttpGetProxyForUrl, WinHttpOpen,
            ERROR_WINHTTP_AUTODETECTION_FAILED, ERROR_WINHTTP_LOGIN_FAILURE,
            WINHTTP_ACCESS_TYPE_NAMED_PROXY, WINHTTP_ACCESS_TYPE_NO_PROXY,
            WINHTTP_AUTOPROXY_AUTO_DETECT, WINHTTP_AUTOPROXY_CONFIG_URL, WINHTTP_AUTOPROXY_OPTIONS,
            WINHTTP_AUTO_DETECT_TYPE_DHCP, WINHTTP_AUTO_DETECT_TYPE_DNS_A,
            WINHTTP_CURRENT_USER_IE_PROXY_CONFIG, WINHTTP_PROXY_INFO,
        },
    };

    use super::{bypassed, is_loopback, proxy_list_routes, system_failure, OsProxyReader};
    use crate::{communication::proxy::Route, error::Error, platform::LockExt};

    /// How long a failed auto-detection is not retried, as .NET's `RecentAutoDetectionInterval`.
    const AUTO_DETECTION_RETRY: Duration = Duration::from_secs(120);

    pub(super) struct WinHttp;

    /// A string WinHTTP allocated, freed with `GlobalFree`.
    struct Allocated(*mut u16);

    impl Allocated {
        fn text(&self) -> Option<String> {
            if self.0.is_null() {
                return None;
            }
            // SAFETY: WinHTTP returns NUL-terminated wide strings.
            let text = unsafe {
                let length = (0..).take_while(|&index| *self.0.add(index) != 0).count();
                String::from_utf16_lossy(std::slice::from_raw_parts(self.0, length))
            };
            Some(text).filter(|text| !text.trim().is_empty())
        }
    }

    impl Drop for Allocated {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: allocated by WinHTTP for the caller, freed once.
                unsafe { GlobalFree(self.0.cast()) };
            }
        }
    }

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    /// The WinHTTP session for auto-proxy queries, opened once (its handle is thread-safe).
    fn session() -> Option<usize> {
        static SESSION: OnceLock<usize> = OnceLock::new();
        let handle = *SESSION.get_or_init(|| {
            let agent = wide("dXn SignalR");
            // SAFETY: the agent string lives across the call; no proxy names are passed.
            unsafe {
                WinHttpOpen(
                    agent.as_ptr(),
                    WINHTTP_ACCESS_TYPE_NO_PROXY,
                    ptr::null(),
                    ptr::null(),
                    0,
                ) as usize
            }
        });
        (handle != 0).then_some(handle)
    }

    /// When auto-detection last failed.
    fn detection_failed() -> &'static Mutex<Option<Instant>> {
        static FAILED: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
        FAILED.get_or_init(Mutex::default)
    }

    /// One `WinHttpGetProxyForUrl` query: the proxy and bypass lists, `None` for no proxy.
    fn query(
        session: usize,
        url: &[u16],
        options: &mut WINHTTP_AUTOPROXY_OPTIONS,
    ) -> Result<Option<(String, Option<String>)>, u32> {
        let mut info = WINHTTP_PROXY_INFO::default();
        // SAFETY: the session is a WinHTTP session handle; the strings and structures live across
        // the call; WinHTTP allocates the result strings, freed by the guards below.
        let succeeded =
            unsafe { WinHttpGetProxyForUrl(session as *mut _, url.as_ptr(), options, &mut info) };
        let (proxy, bypass) = (Allocated(info.lpszProxy), Allocated(info.lpszProxyBypass));
        if succeeded == 0 {
            // SAFETY: reads the calling thread's last error.
            return Err(unsafe { GetLastError() });
        }
        if info.dwAccessType != WINHTTP_ACCESS_TYPE_NAMED_PROXY {
            return Ok(None);
        }
        Ok(proxy.text().map(|proxy| (proxy, bypass.text())))
    }

    /// The proxy and bypass lists an auto-proxy query returns, `None` for `DIRECT`. As .NET, the
    /// first query does not log on to the auto-config server (WinHTTP then caches the result);
    /// one that needs a logon is sent again with it.
    fn auto_proxy(
        url: &str,
        auto_detect: bool,
        config_url: Option<&str>,
    ) -> Result<Option<(String, Option<String>)>, u32> {
        let session = session().ok_or(0u32)?;
        let url = wide(url);
        let config = config_url.map(wide);
        let mut options = WINHTTP_AUTOPROXY_OPTIONS::default();
        if auto_detect {
            options.dwFlags |= WINHTTP_AUTOPROXY_AUTO_DETECT;
            options.dwAutoDetectFlags =
                WINHTTP_AUTO_DETECT_TYPE_DHCP | WINHTTP_AUTO_DETECT_TYPE_DNS_A;
        }
        if let Some(config) = &config {
            options.dwFlags |= WINHTTP_AUTOPROXY_CONFIG_URL;
            options.lpszAutoConfigUrl = config.as_ptr();
        }
        match query(session, &url, &mut options) {
            Err(ERROR_WINHTTP_LOGIN_FAILURE) => {
                options.fAutoLogonIfChallenged = 1;
                query(session, &url, &mut options)
            }
            result => result,
        }
    }

    impl OsProxyReader for WinHttp {
        fn routes(&self, url: &str, secure: bool, host: &str) -> Result<Vec<Route>, Error> {
            if is_loopback(host) {
                return Ok(vec![Route::Direct]);
            }
            let mut config = WINHTTP_CURRENT_USER_IE_PROXY_CONFIG::default();
            // SAFETY: WinHTTP fills the structure and allocates its strings, freed below.
            let read = unsafe { WinHttpGetIEProxyConfigForCurrentUser(&mut config) };
            let (config_url, proxy, bypass) = (
                Allocated(config.lpszAutoConfigUrl),
                Allocated(config.lpszProxy),
                Allocated(config.lpszProxyBypass),
            );
            if read == 0 {
                // SAFETY: reads the calling thread's last error.
                let error = unsafe { GetLastError() };
                return Err(system_failure(format!("WinHTTP error {error}")));
            }
            let routes = |list: &str, bypass: Option<&str>| {
                if bypass.is_some_and(|bypass| bypassed(bypass, host)) {
                    return vec![Route::Direct];
                }
                let routes = proxy_list_routes(list, secure);
                if routes.is_empty() {
                    vec![Route::Direct]
                } else {
                    routes
                }
            };

            let config_url = config_url.text();
            let recently_failed = detection_failed()
                .locked()
                .is_some_and(|failed| failed.elapsed() < AUTO_DETECTION_RETRY);
            // A recent auto-detection failure skips auto-detection (.NET skips the whole automatic
            // path then); a configured auto-config URL is still used, which only adds a chance
            // to follow the user's script.
            let auto_detect = config.fAutoDetect != 0 && !recently_failed;
            if auto_detect || config_url.is_some() {
                match auto_proxy(url, auto_detect, config_url.as_deref()) {
                    Ok(Some((list, auto_bypass))) => {
                        return Ok(routes(&list, auto_bypass.as_deref()))
                    }
                    // The script (or detection) chose DIRECT: final, as in .NET.
                    Ok(None) => return Ok(vec![Route::Direct]),
                    Err(error) => {
                        if error == ERROR_WINHTTP_AUTODETECTION_FAILED {
                            *detection_failed().locked() = Some(Instant::now());
                        }
                        log::debug!(
                            "WinHTTP automatic proxy configuration failed ({error}); using the manual settings"
                        );
                    }
                }
            }
            Ok(match proxy.text() {
                Some(list) => routes(&list, bypass.text().as_deref()),
                None => vec![Route::Direct],
            })
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
mod cfnetwork {
    //! CFNetwork: `CFNetworkCopySystemProxySettings` and `CFNetworkCopyProxiesForURL`, with
    //! auto-config scripts run by CFNetwork itself (`CFNetworkExecuteProxyAutoConfigurationURL`
    //! and `…Script` on a private run-loop mode), as .NET's `MacProxy`.

    use std::{
        ffi::c_void,
        ptr,
        time::{Duration, Instant},
    };

    use core_foundation::{
        base::{CFType, TCFType},
        dictionary::CFDictionary,
        number::CFNumber,
        string::CFString,
        url::CFURL,
    };
    use core_foundation_sys::{
        array::{CFArrayGetCount, CFArrayGetValueAtIndex, CFArrayRef},
        base::{kCFAllocatorDefault, CFIndex, CFRelease, CFRetain, CFTypeRef},
        dictionary::CFDictionaryRef,
        error::CFErrorRef,
        runloop::{
            CFRunLoopAddSource, CFRunLoopGetCurrent, CFRunLoopRemoveSource, CFRunLoopRunInMode,
            CFRunLoopSourceInvalidate, CFRunLoopSourceRef, CFRunLoopStop,
        },
        string::CFStringRef,
        url::{CFURLCreateWithString, CFURLRef},
    };

    use super::{cf_routes, system_failure, CfProxy, OsProxyReader, OsRoute};
    use crate::{communication::proxy::Route, error::Error};

    /// How long one auto-config script may take, download included.
    const SCRIPT_LIMIT: Duration = Duration::from_secs(10);

    type ResultCallback =
        extern "C" fn(client: *mut c_void, proxies: CFArrayRef, error: CFErrorRef);

    /// `CFStreamClientContext` with optional (null) callbacks.
    #[repr(C)]
    struct ClientContext {
        version: CFIndex,
        info: *mut c_void,
        retain: *const c_void,
        release: *const c_void,
        copy_description: *const c_void,
    }

    #[link(name = "CFNetwork", kind = "framework")]
    extern "C" {
        fn CFNetworkCopySystemProxySettings() -> CFDictionaryRef;
        fn CFNetworkCopyProxiesForURL(url: CFURLRef, settings: CFDictionaryRef) -> CFArrayRef;
        fn CFNetworkExecuteProxyAutoConfigurationURL(
            script: CFURLRef,
            target: CFURLRef,
            callback: ResultCallback,
            context: *mut ClientContext,
        ) -> CFRunLoopSourceRef;
        fn CFNetworkExecuteProxyAutoConfigurationScript(
            script: CFStringRef,
            target: CFURLRef,
            callback: ResultCallback,
            context: *mut ClientContext,
        ) -> CFRunLoopSourceRef;
        static kCFProxyTypeKey: CFStringRef;
        static kCFProxyHostNameKey: CFStringRef;
        static kCFProxyPortNumberKey: CFStringRef;
        static kCFProxyAutoConfigurationURLKey: CFStringRef;
        static kCFProxyAutoConfigurationJavaScriptKey: CFStringRef;
    }

    pub(super) struct CfNetwork;

    fn url(text: &str) -> Option<CFURL> {
        let text = CFString::new(text);
        // SAFETY: creates a URL from a valid CFString; the result follows the create rule.
        let raw = unsafe {
            CFURLCreateWithString(kCFAllocatorDefault, text.as_concrete_TypeRef(), ptr::null())
        };
        // SAFETY: a URL just created, owned here.
        (!raw.is_null()).then(|| unsafe { CFURL::wrap_under_create_rule(raw) })
    }

    fn text(dictionary: &CFDictionary<CFString, CFType>, key: CFStringRef) -> Option<String> {
        // SAFETY: the key is a CFNetwork constant string.
        let key = unsafe { CFString::wrap_under_get_rule(key) };
        let value = dictionary.find(&key)?;
        if let Some(string) = value.downcast::<CFString>() {
            return Some(string.to_string());
        }
        value
            .downcast::<CFURL>()
            .map(|url| url.get_string().to_string())
    }

    /// The proxy dictionaries of a CFNetwork proxy array.
    fn dictionaries(proxies: CFArrayRef) -> Vec<CfProxy> {
        let mut entries = Vec::new();
        // SAFETY: an array of proxy dictionaries, valid while the caller holds it.
        let count = unsafe { CFArrayGetCount(proxies) };
        for index in 0..count {
            let raw = unsafe { CFArrayGetValueAtIndex(proxies, index) } as CFDictionaryRef;
            if raw.is_null() {
                continue;
            }
            // SAFETY: the array holds dictionaries with string keys.
            let dictionary: CFDictionary<CFString, CFType> =
                unsafe { CFDictionary::wrap_under_get_rule(raw) };
            // SAFETY: the keys are CFNetwork constants.
            let (kind, host, port_key, pac_url, pac_script) = unsafe {
                (
                    kCFProxyTypeKey,
                    kCFProxyHostNameKey,
                    kCFProxyPortNumberKey,
                    kCFProxyAutoConfigurationURLKey,
                    kCFProxyAutoConfigurationJavaScriptKey,
                )
            };
            let port = dictionary
                .find(&unsafe { CFString::wrap_under_get_rule(port_key) })
                .and_then(|value| value.downcast::<CFNumber>())
                .and_then(|number| number.to_i64());
            entries.push(CfProxy {
                kind: text(&dictionary, kind).unwrap_or_default(),
                host: text(&dictionary, host),
                port,
                auto_config_url: text(&dictionary, pac_url),
                auto_config_script: text(&dictionary, pac_script),
            });
        }
        entries
    }

    /// What a script run reported.
    struct Outcome {
        done: bool,
        proxies: CFArrayRef,
    }

    extern "C" fn finished(client: *mut c_void, proxies: CFArrayRef, error: CFErrorRef) {
        // SAFETY: `client` is the `Outcome` the run below passed, alive until its source is
        // removed and invalidated.
        let outcome = unsafe { &mut *client.cast::<Outcome>() };
        outcome.done = true;
        if error.is_null() && !proxies.is_null() {
            // SAFETY: the array is retained for the run, which releases it.
            unsafe { CFRetain(proxies as CFTypeRef) };
            outcome.proxies = proxies;
        }
        // SAFETY: stops the run loop of this thread, which is running the script's source.
        unsafe { CFRunLoopStop(CFRunLoopGetCurrent()) };
    }

    /// Runs an auto-config script (`Ok` a URL, `Err` its text) for `target` on this thread's run
    /// loop, within [`SCRIPT_LIMIT`]: its proxy dictionaries, or `None` when it fails.
    fn run_script(script: Result<&CFURL, &CFString>, target: &CFURL) -> Option<Vec<CfProxy>> {
        // Owned through a raw pointer only: the callback writes it, this loop reads it.
        let outcome = Box::into_raw(Box::new(Outcome {
            done: false,
            proxies: ptr::null(),
        }));
        let mut context = ClientContext {
            version: 0,
            info: outcome.cast(),
            retain: ptr::null(),
            release: ptr::null(),
            copy_description: ptr::null(),
        };
        // SAFETY: the script, target and context outlive the source, which is removed and
        // invalidated before `outcome` is freed.
        let source = unsafe {
            match script {
                Ok(url) => CFNetworkExecuteProxyAutoConfigurationURL(
                    url.as_concrete_TypeRef(),
                    target.as_concrete_TypeRef(),
                    finished,
                    &mut context,
                ),
                Err(text) => CFNetworkExecuteProxyAutoConfigurationScript(
                    text.as_concrete_TypeRef(),
                    target.as_concrete_TypeRef(),
                    finished,
                    &mut context,
                ),
            }
        };
        if source.is_null() {
            // SAFETY: never shared: no source was created.
            drop(unsafe { Box::from_raw(outcome) });
            return None;
        }
        let mode = CFString::new("dXnSignalRProxyAutoConfiguration");
        let deadline = Instant::now() + SCRIPT_LIMIT;
        // SAFETY: the source runs on this thread's run loop in a private mode, then is removed,
        // invalidated and released; `outcome` is read through its pointer, which the callback
        // writes on this thread during `CFRunLoopRunInMode`.
        let proxies = unsafe {
            let run_loop = CFRunLoopGetCurrent();
            CFRunLoopAddSource(run_loop, source, mode.as_concrete_TypeRef());
            loop {
                if ptr::read_volatile(ptr::addr_of!((*outcome).done)) {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                CFRunLoopRunInMode(mode.as_concrete_TypeRef(), remaining.as_secs_f64(), 0);
            }
            CFRunLoopRemoveSource(run_loop, source, mode.as_concrete_TypeRef());
            CFRunLoopSourceInvalidate(source);
            CFRelease(source as CFTypeRef);
            Box::from_raw(outcome).proxies
        };
        if proxies.is_null() {
            return None;
        }
        let entries = dictionaries(proxies);
        // SAFETY: retained by the callback.
        unsafe { CFRelease(proxies as CFTypeRef) };
        Some(entries)
    }

    impl OsProxyReader for CfNetwork {
        fn routes(&self, address: &str, _secure: bool, _host: &str) -> Result<Vec<Route>, Error> {
            let target =
                url(address).ok_or_else(|| system_failure("the endpoint is not a valid URL"))?;
            // SAFETY: both functions follow the create rule; each result is released below.
            let settings = unsafe { CFNetworkCopySystemProxySettings() };
            if settings.is_null() {
                return Err(system_failure("CFNetwork returned no settings"));
            }
            let proxies =
                unsafe { CFNetworkCopyProxiesForURL(target.as_concrete_TypeRef(), settings) };
            let entries = if proxies.is_null() {
                Vec::new()
            } else {
                let entries = dictionaries(proxies);
                unsafe { CFRelease(proxies as CFTypeRef) };
                entries
            };
            unsafe { CFRelease(settings as CFTypeRef) };

            let mut routes = Vec::new();
            for entry in cf_routes(&entries) {
                let script = match &entry {
                    OsRoute::Route(route) => {
                        routes.push(route.clone());
                        continue;
                    }
                    OsRoute::AutoConfigUrl(script) => url(script).map(Ok),
                    OsRoute::AutoConfigScript(script) => Some(Err(CFString::new(script))),
                };
                let result = script
                    .as_ref()
                    .and_then(|script| run_script(script.as_ref(), &target));
                match result {
                    // A script's own auto-config entries are not followed.
                    Some(proxies) => routes.extend(cf_routes(&proxies).into_iter().filter_map(
                        |entry| match entry {
                            OsRoute::Route(route) => Some(route),
                            _ => None,
                        },
                    )),
                    None => log::warn!(
                        "The system's proxy auto-config script could not be run; it is skipped"
                    ),
                }
            }
            Ok(routes)
        }
    }
}

#[cfg(all(target_os = "android", feature = "tls-platform-verifier"))]
pub(crate) mod java {
    //! `java.net.ProxySelector.getDefault().select(URI)` through JNI, with the Java VM stored by
    //! `tls::android::init_with_env`.

    use std::sync::OnceLock;

    use jni::{jni_sig, jni_str, objects::JString, JValue};

    use super::{java_routes, system_failure, OsProxyReader};
    use crate::{communication::proxy::Route, error::Error};

    static VM: OnceLock<jni::JavaVM> = OnceLock::new();

    /// Keeps the app's Java VM for the proxy selector (the first one wins).
    pub(crate) fn set_java_vm(vm: jni::JavaVM) {
        let _ = VM.set(vm);
    }

    pub(super) struct Java;

    pub(super) fn reader() -> Option<&'static dyn OsProxyReader> {
        VM.get().map(|_| &Java as &dyn OsProxyReader)
    }

    impl OsProxyReader for Java {
        fn routes(&self, url: &str, _secure: bool, _host: &str) -> Result<Vec<Route>, Error> {
            let vm = VM
                .get()
                .ok_or_else(|| system_failure("the Java VM is not initialized"))?;
            let entries = vm
                .attach_current_thread(|env| -> Result<_, jni::errors::Error> {
                    let selector = env
                        .call_static_method(
                            jni_str!("java/net/ProxySelector"),
                            jni_str!("getDefault"),
                            jni_sig!("()Ljava/net/ProxySelector;"),
                            &[],
                        )?
                        .l()?;
                    if selector.is_null() {
                        return Ok(Vec::new());
                    }
                    let text = env.new_string(url)?;
                    let uri = env
                        .call_static_method(
                            jni_str!("java/net/URI"),
                            jni_str!("create"),
                            jni_sig!("(Ljava/lang/String;)Ljava/net/URI;"),
                            &[JValue::Object(&text)],
                        )?
                        .l()?;
                    let list = env
                        .call_method(
                            &selector,
                            jni_str!("select"),
                            jni_sig!("(Ljava/net/URI;)Ljava/util/List;"),
                            &[JValue::Object(&uri)],
                        )?
                        .l()?;
                    let size = env
                        .call_method(&list, jni_str!("size"), jni_sig!("()I"), &[])?
                        .i()?;
                    let mut entries = Vec::new();
                    for index in 0..size {
                        let proxy = env
                            .call_method(
                                &list,
                                jni_str!("get"),
                                jni_sig!("(I)Ljava/lang/Object;"),
                                &[JValue::Int(index)],
                            )?
                            .l()?;
                        let kind = env
                            .call_method(
                                &proxy,
                                jni_str!("type"),
                                jni_sig!("()Ljava/net/Proxy$Type;"),
                                &[],
                            )?
                            .l()?;
                        let name = env
                            .call_method(
                                &kind,
                                jni_str!("name"),
                                jni_sig!("()Ljava/lang/String;"),
                                &[],
                            )?
                            .l()?;
                        let name = env.cast_local::<JString>(name)?.try_to_string(env)?;
                        let address = env
                            .call_method(
                                &proxy,
                                jni_str!("address"),
                                jni_sig!("()Ljava/net/SocketAddress;"),
                                &[],
                            )?
                            .l()?;
                        let (host, port) = if address.is_null() {
                            (None, 0)
                        } else {
                            let host = env
                                .call_method(
                                    &address,
                                    jni_str!("getHostString"),
                                    jni_sig!("()Ljava/lang/String;"),
                                    &[],
                                )?
                                .l()?;
                            let host = env.cast_local::<JString>(host)?.try_to_string(env)?;
                            let port = env
                                .call_method(&address, jni_str!("getPort"), jni_sig!("()I"), &[])?
                                .i()?;
                            (Some(host), port)
                        };
                        entries.push((name, host, port));
                    }
                    Ok(entries)
                })
                .map_err(system_failure)?;
            Ok(java_routes(&entries))
        }
    }
}

#[cfg(test)]
mod os_proxy_tests {
    use super::{
        bypassed, cf_routes, is_loopback, java_routes, proxy_list_routes, CfProxy, OsProxyReader,
        OsRoute,
    };
    use crate::communication::proxy::{ProxyUrl, Route};

    fn http(address: &str) -> Route {
        Route::Proxy(ProxyUrl::parse(&format!("http://{address}")).unwrap())
    }

    #[test]
    fn os_proxy_windows_lists_route_by_scheme_as_dotnet() {
        assert_eq!(
            proxy_list_routes("proxy:8080", false),
            vec![http("proxy:8080")]
        );
        assert_eq!(
            proxy_list_routes("proxy:8080", true),
            vec![http("proxy:8080")]
        );
        let list = "http=plain:1;https=secure:2 both:3; socks=s:4 ftp=f:5";
        assert_eq!(
            proxy_list_routes(list, false),
            vec![http("plain:1"), http("both:3")]
        );
        assert_eq!(
            proxy_list_routes(list, true),
            vec![http("secure:2"), http("both:3")]
        );
        assert_eq!(
            proxy_list_routes("http://scheme:3128", false),
            vec![http("scheme:3128")],
            "a scheme prefix is removed"
        );
        assert_eq!(proxy_list_routes("proxy", false), vec![http("proxy:80")]);
        assert!(proxy_list_routes("socks=s:4", true).is_empty());
        assert!(proxy_list_routes("", true).is_empty());
    }

    #[test]
    fn os_proxy_windows_bypass_lists_match_local_and_wildcards() {
        let bypass = "<local>;*.corp.example; 10.*;exact.example";
        assert!(bypassed(bypass, "intranet"));
        assert!(bypassed(bypass, "hub.corp.example"));
        assert!(bypassed(bypass, "HUB.Corp.Example"));
        assert!(bypassed(bypass, "10.1.2.3"));
        assert!(bypassed(bypass, "exact.example"));
        assert!(!bypassed(bypass, "other.example"));
        assert!(!bypassed(bypass, "corp.example.evil"));
        assert!(!bypassed("<-loopback>", "localhost"));
        assert!(bypassed("*", "anything.example"));
        assert!(bypassed("a*c*e", "abcde"));
        assert!(!bypassed("a*c*e", "abcdf"));
    }

    #[test]
    fn os_proxy_never_serves_loopback() {
        for host in [
            "localhost",
            "LOCALHOST",
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "[::1]",
        ] {
            assert!(is_loopback(host), "{host}");
        }
        for host in ["hub.example", "10.0.2.2", "localhost.example"] {
            assert!(!is_loopback(host), "{host}");
        }
    }

    #[test]
    fn os_proxy_android_selector_entries_map_to_routes() {
        let entries = [
            ("HTTP".to_string(), Some("proxy".to_string()), 3128),
            ("SOCKS".to_string(), Some("::1".to_string()), 1080),
            ("DIRECT".to_string(), None, 0),
            ("UNKNOWN".to_string(), Some("x".to_string()), 1),
            ("HTTP".to_string(), None, 0),
        ];
        assert_eq!(
            java_routes(&entries),
            vec![
                http("proxy:3128"),
                Route::Proxy(ProxyUrl::parse("socks5://[::1]:1080").unwrap()),
                Route::Direct,
            ]
        );
    }

    #[test]
    fn os_proxy_apple_dictionaries_map_to_routes() {
        let proxy = |kind: &str| CfProxy {
            kind: kind.to_string(),
            host: Some("proxy.example".to_string()),
            port: Some(3128),
            ..CfProxy::default()
        };
        let proxies = [
            proxy("kCFProxyTypeHTTPS"),
            proxy("kCFProxyTypeSOCKS"),
            CfProxy {
                kind: "kCFProxyTypeAutoConfigurationURL".to_string(),
                auto_config_url: Some("http://wpad/wpad.dat".to_string()),
                ..CfProxy::default()
            },
            CfProxy {
                kind: "kCFProxyTypeAutoConfigurationJavaScript".to_string(),
                auto_config_script: Some("function FindProxyForURL(u, h) {}".to_string()),
                ..CfProxy::default()
            },
            proxy("kCFProxyTypeFTP"),
            CfProxy {
                kind: "kCFProxyTypeNone".to_string(),
                ..CfProxy::default()
            },
        ];
        assert_eq!(
            cf_routes(&proxies),
            vec![
                OsRoute::Route(http("proxy.example:3128")),
                OsRoute::Route(Route::Proxy(
                    ProxyUrl::parse("socks5://proxy.example:3128").unwrap()
                )),
                OsRoute::AutoConfigUrl("http://wpad/wpad.dat".to_string()),
                OsRoute::AutoConfigScript("function FindProxyForURL(u, h) {}".to_string()),
                OsRoute::Route(Route::Direct),
            ]
        );
        // A missing port takes the scheme's default.
        let defaulted = CfProxy {
            port: None,
            ..proxy("kCFProxyTypeHTTP")
        };
        assert_eq!(
            cf_routes(&[defaulted]),
            vec![OsRoute::Route(http("proxy.example:80"))]
        );
    }

    /// A reader whose settings cannot be read.
    struct Unreadable;

    impl OsProxyReader for Unreadable {
        fn routes(&self, _: &str, _: bool, _: &str) -> Result<Vec<Route>, crate::Error> {
            Err(super::system_failure("no settings for this account"))
        }
    }

    /// A reader with no proxy for the URL.
    struct Nothing;

    impl OsProxyReader for Nothing {
        fn routes(&self, _: &str, _: bool, _: &str) -> Result<Vec<Route>, crate::Error> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn os_proxy_settings_that_cannot_be_read_connect_directly() {
        assert_eq!(
            super::routes(&Unreadable, true, "hub.example", 443).await,
            vec![Route::Direct],
            "as .NET, a failing system proxy connects directly"
        );
        assert_eq!(
            super::routes(&Nothing, false, "hub.example", 80).await,
            vec![Route::Direct]
        );
    }

    #[cfg(windows)]
    #[test]
    fn os_proxy_windows_reader_answers_for_the_current_user() {
        // The developer's own settings decide the routes; the reader answers without failing,
        // and loopback is always direct.
        let reader = super::platform().expect("WinHTTP on Windows");
        assert_eq!(
            reader
                .routes("http://localhost:5000/", false, "localhost")
                .unwrap(),
            vec![Route::Direct]
        );
        let routes = reader
            .routes(
                "https://dxn-signalr-os-proxy.invalid/",
                true,
                "dxn-signalr-os-proxy.invalid",
            )
            .unwrap();
        assert!(!routes.is_empty());
    }
}
