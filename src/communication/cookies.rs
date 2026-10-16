//! The native HTTP cookie jar, as the official .NET client's `HttpConnectionOptions.Cookies`
//! (`CookieContainer`): negotiate responses, the WebSocket upgrade and the HTTP transports store
//! their `Set-Cookie` headers, and every native request sends the cookies that match its URL, so
//! a load balancer's affinity cookie keeps a connection on one server across reconnects. A subset
//! of RFC 6265: host-only and `Domain` cookies, `Path`, `Secure`, `Expires` and `Max-Age`, with
//! the .NET container's limits. Browsers manage cookies themselves.

// Header parsing must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::platform::LockExt;

/// The largest cookie (name and value) the jar keeps, as `CookieContainer.MaxCookieSize`.
const MAX_COOKIE_SIZE: usize = 4096;
/// The most cookies the jar keeps per domain, as `CookieContainer.PerDomainCapacity`.
const PER_DOMAIN_CAPACITY: usize = 20;
/// The most cookies the jar keeps, as `CookieContainer.Capacity`.
const CAPACITY: usize = 300;

/// A cookie jar shared by a client's native HTTP requests and WebSocket upgrades, as the official
/// .NET client's `HttpConnectionOptions.Cookies`.
///
/// Every client has its own jar; pass one to
/// [`ConnectionConfiguration::with_cookie_jar`](crate::ConnectionConfiguration::with_cookie_jar)
/// to share it between clients or to read and seed it. Clones share the same cookies. `Debug`
/// shows only the number of cookies, never their values.
#[derive(Clone, Default)]
pub struct CookieJar {
    cookies: Arc<Mutex<Store>>,
}

#[derive(Default)]
struct Store {
    cookies: Vec<Cookie>,
    /// Creation order, which breaks ties between cookies sent together and picks the eviction.
    sequence: u64,
}

#[derive(Clone, Debug)]
struct Cookie {
    name: String,
    value: String,
    /// Lower case, without a leading dot.
    domain: String,
    /// Sent only to `domain` itself, not its subdomains (no `Domain` attribute).
    host_only: bool,
    path: String,
    secure: bool,
    expires: Option<SystemTime>,
    created: u64,
}

impl std::fmt::Debug for CookieJar {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CookieJar")
            .field("cookies", &self.len())
            .finish()
    }
}

/// The parts of a request URL cookies are matched against.
struct Origin {
    host: String,
    path: String,
    secure: bool,
}

impl Origin {
    /// `http`, `https`, `ws` and `wss` URLs; anything else has no cookies.
    fn parse(url: &str) -> Option<Self> {
        let uri: http::Uri = url.parse().ok()?;
        let secure = match uri.scheme_str()? {
            "https" | "wss" => true,
            "http" | "ws" => false,
            _ => return None,
        };
        let host = uri
            .host()?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let path = match uri.path() {
            "" => "/".to_string(),
            path => path.to_string(),
        };
        Some(Self { host, path, secure })
    }
}

impl CookieJar {
    /// An empty jar.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores the cookie of one `Set-Cookie` header value received from `url`, as
    /// `CookieContainer.SetCookies`. A malformed cookie, one for another domain or one larger than
    /// 4096 bytes is ignored; an expired one removes the stored cookie it matches.
    pub fn set_cookie(&self, url: &str, set_cookie: &str) {
        self.store(url, set_cookie, SystemTime::now());
    }

    /// The `Cookie` header value for a request to `url` (`name=value; name2=value2`), or `None`
    /// when no stored cookie matches, as `CookieContainer.GetCookieHeader`.
    pub fn cookie_header(&self, url: &str) -> Option<String> {
        self.header_at(url, SystemTime::now())
    }

    /// The number of stored cookies (expired ones included until a request removes them).
    pub fn len(&self) -> usize {
        self.cookies.locked().cookies.len()
    }

    /// Whether the jar stores no cookie.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes every cookie.
    pub fn clear(&self) {
        self.cookies.locked().cookies.clear();
    }

    /// Stores every `Set-Cookie` header of a response from `url`.
    pub(crate) fn store_response(&self, url: &str, headers: &http::HeaderMap) {
        for value in headers.get_all(http::header::SET_COOKIE) {
            if let Ok(value) = value.to_str() {
                self.set_cookie(url, value);
            }
        }
    }

    fn store(&self, url: &str, set_cookie: &str, now: SystemTime) {
        let Some(origin) = Origin::parse(url) else {
            return;
        };
        let Some(mut cookie) = parse_set_cookie(&origin, set_cookie, now) else {
            return;
        };
        let mut store = self.cookies.locked();
        let existing = store.cookies.iter().position(|stored| {
            stored.name == cookie.name
                && stored.domain == cookie.domain
                && stored.path == cookie.path
                && stored.host_only == cookie.host_only
        });
        let expired = cookie.expires.is_some_and(|expires| expires <= now);
        match existing {
            Some(index) if expired => {
                store.cookies.remove(index);
            }
            _ if expired => {}
            Some(index) => {
                if let Some(stored) = store.cookies.get_mut(index) {
                    cookie.created = stored.created;
                    *stored = cookie;
                }
            }
            None => {
                store.sequence += 1;
                cookie.created = store.sequence;
                let domain = cookie.domain.clone();
                store.cookies.push(cookie);
                store.evict(&domain, now);
            }
        }
    }

    fn header_at(&self, url: &str, now: SystemTime) -> Option<String> {
        let origin = Origin::parse(url)?;
        let mut store = self.cookies.locked();
        store
            .cookies
            .retain(|cookie| cookie.expires.is_none_or(|expires| expires > now));
        let mut matching: Vec<&Cookie> = store
            .cookies
            .iter()
            .filter(|cookie| cookie.matches(&origin))
            .collect();
        if matching.is_empty() {
            return None;
        }
        // RFC 6265 5.4: longer paths first, then the earlier created.
        matching.sort_by(|left, right| {
            right
                .path
                .len()
                .cmp(&left.path.len())
                .then(left.created.cmp(&right.created))
        });
        Some(
            matching
                .iter()
                .map(|cookie| format!("{}={}", cookie.name, cookie.value))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

impl Store {
    /// Drops expired cookies, then the oldest of `domain` beyond its capacity, then the oldest
    /// overall beyond the jar's capacity.
    fn evict(&mut self, domain: &str, now: SystemTime) {
        self.cookies
            .retain(|cookie| cookie.expires.is_none_or(|expires| expires > now));
        while self
            .cookies
            .iter()
            .filter(|cookie| cookie.domain == domain)
            .count()
            > PER_DOMAIN_CAPACITY
        {
            self.remove_oldest(|cookie| cookie.domain == domain);
        }
        while self.cookies.len() > CAPACITY {
            self.remove_oldest(|_| true);
        }
    }

    fn remove_oldest(&mut self, scope: impl Fn(&Cookie) -> bool) {
        if let Some(index) = self
            .cookies
            .iter()
            .enumerate()
            .filter(|(_, cookie)| scope(cookie))
            .min_by_key(|(_, cookie)| cookie.created)
            .map(|(index, _)| index)
        {
            self.cookies.remove(index);
        }
    }
}

impl Cookie {
    fn matches(&self, origin: &Origin) -> bool {
        let host = if self.host_only {
            origin.host == self.domain
        } else {
            domain_matches(&origin.host, &self.domain)
        };
        host && path_matches(&origin.path, &self.path) && (origin.secure || !self.secure)
    }
}

/// RFC 6265 5.1.3: the host is the domain or a subdomain of it (never for IP addresses).
fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain
        || (host.ends_with(domain)
            && host.len() > domain.len()
            && host.as_bytes().get(host.len() - domain.len() - 1) == Some(&b'.')
            && host.parse::<std::net::IpAddr>().is_err())
}

/// RFC 6265 5.1.4.
fn path_matches(request: &str, cookie: &str) -> bool {
    request == cookie
        || (request.starts_with(cookie)
            && (cookie.ends_with('/') || request.as_bytes().get(cookie.len()) == Some(&b'/')))
}

/// RFC 6265 5.1.4: the request path up to its last `/`, or `/`.
fn default_path(request: &str) -> String {
    match request.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(last) => request.get(..last).unwrap_or("/").to_string(),
    }
}

/// An RFC 6265 cookie name: an HTTP token.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !b"()<>@,;:\\\"/[]?={}".contains(&byte))
}

/// A cookie value that can go in a `Cookie` header: printable ASCII and inner spaces (as browsers
/// accept them), without `;`, control characters or line breaks.
fn valid_value(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| (byte.is_ascii_graphic() || byte == b' ') && byte != b';')
}

/// RFC 6265 5.2, for a cookie received from `origin`.
fn parse_set_cookie(origin: &Origin, header: &str, now: SystemTime) -> Option<Cookie> {
    let mut parts = header.split(';');
    let (name, value) = parts.next()?.split_once('=')?;
    let (name, value) = (name.trim(), value.trim());
    if !valid_name(name) || !valid_value(value) || name.len() + value.len() > MAX_COOKIE_SIZE {
        return None;
    }
    let mut cookie = Cookie {
        name: name.to_string(),
        value: value.to_string(),
        domain: origin.host.clone(),
        host_only: true,
        path: default_path(&origin.path),
        secure: false,
        expires: None,
        created: 0,
    };
    let mut max_age = None;
    let mut expires = None;
    for attribute in parts {
        let (key, value) = attribute
            .split_once('=')
            .map_or((attribute.trim(), ""), |(key, value)| {
                (key.trim(), value.trim())
            });
        match key.to_ascii_lowercase().as_str() {
            "domain" if !value.is_empty() => {
                let domain = value.trim_start_matches('.').to_ascii_lowercase();
                // A domain without a dot (`com`) only names the host itself, as .NET's
                // `CookieContainer` decides; it never widens a cookie to a whole top-level domain.
                if !domain_matches(&origin.host, &domain)
                    || (!domain.contains('.') && domain != origin.host)
                {
                    return None;
                }
                cookie.domain = domain;
                cookie.host_only = false;
            }
            "path" if value.starts_with('/') => cookie.path = value.to_string(),
            "secure" => cookie.secure = true,
            "max-age" => {
                max_age = value.parse::<i64>().ok();
            }
            "expires" => expires = parse_cookie_date(value),
            _ => {}
        }
    }
    // A plain origin cannot set a secure cookie (RFC 6265bis).
    if cookie.secure && !origin.secure {
        return None;
    }
    cookie.expires = match max_age {
        // Max-Age wins over Expires; zero or less expires the cookie now.
        Some(seconds) if seconds <= 0 => Some(UNIX_EPOCH),
        Some(seconds) => now.checked_add(Duration::from_secs(seconds.unsigned_abs())),
        None => expires,
    };
    Some(cookie)
}

/// RFC 6265 5.1.1: a lenient cookie date (`Wed, 21 Oct 2026 07:28:00 GMT` and its variants).
fn parse_cookie_date(text: &str) -> Option<SystemTime> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let delimiter = |c: char| matches!(c, '\x09' | '\x20'..='\x2f' | '\x3b'..='\x40' | '\x5b'..='\x60' | '\x7b'..='\x7e');
    let digits = |token: &str, minimum: usize, maximum: usize| -> Option<u32> {
        let count = token.chars().take_while(char::is_ascii_digit).count();
        (minimum..=maximum)
            .contains(&count)
            .then(|| token.get(..count)?.parse().ok())
            .flatten()
    };
    let (mut time, mut day, mut month, mut year) = (None, None, None, None);
    for token in text.split(delimiter).filter(|token| !token.is_empty()) {
        if time.is_none() {
            let mut fields = token.splitn(3, ':');
            if let (Some(hour), Some(minute), Some(second)) =
                (fields.next(), fields.next(), fields.next())
            {
                if let (Some(hour), Some(minute), Some(second)) = (
                    digits(hour, 1, 2),
                    digits(minute, 1, 2),
                    digits(second, 1, 2),
                ) {
                    time = Some((hour, minute, second));
                    continue;
                }
            }
        }
        if day.is_none() {
            if let Some(value) = digits(token, 1, 2) {
                day = Some(value);
                continue;
            }
        }
        if month.is_none() {
            let prefix = token.get(..3).map(str::to_ascii_lowercase);
            if let Some(index) = MONTHS
                .iter()
                .position(|name| prefix.as_deref() == Some(*name))
            {
                month = Some(index as u32 + 1);
                continue;
            }
        }
        if year.is_none() {
            if let Some(value) = digits(token, 2, 4) {
                year = Some(value);
            }
        }
    }
    let (hour, minute, second) = time?;
    let (day, month) = (day?, month?);
    let year = match year? {
        value @ 70..=99 => value + 1900,
        value @ 0..=69 => value + 2000,
        value => value,
    };
    if !(1..=31).contains(&day) || year < 1601 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(i64::from(year), month, day);
    let seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    if seconds >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(seconds.unsigned_abs()))
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(seconds.unsigned_abs()))
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod cookie_jar_tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{parse_cookie_date, CookieJar, PER_DOMAIN_CAPACITY};

    const SERVICE: &str = "http://service.example.com/chat/negotiate?negotiateVersion=1";

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[test]
    fn cookie_jar_sends_a_host_only_cookie_to_its_host_and_default_path() {
        let jar = CookieJar::new();
        jar.set_cookie(SERVICE, "ARRAffinity=server-1; Path=/; HttpOnly");
        jar.set_cookie(SERVICE, "scoped=1");
        assert_eq!(
            jar.cookie_header("ws://service.example.com/chat?id=token")
                .as_deref(),
            Some("scoped=1; ARRAffinity=server-1"),
            "the upgrade carries the negotiate cookies, the longer path first"
        );
        // The default path of `/chat/negotiate` is `/chat`.
        assert_eq!(
            jar.cookie_header("http://service.example.com/other")
                .as_deref(),
            Some("ARRAffinity=server-1")
        );
        assert_eq!(
            jar.cookie_header("http://sub.service.example.com/chat"),
            None
        );
        assert_eq!(jar.cookie_header("http://example.com/chat"), None);
    }

    #[test]
    fn cookie_jar_domain_cookies_reach_subdomains_and_foreign_domains_are_ignored() {
        let jar = CookieJar::new();
        jar.set_cookie(SERVICE, "shared=1; Domain=.example.com; Path=/");
        jar.set_cookie(SERVICE, "foreign=1; Domain=other.com; Path=/");
        jar.set_cookie(SERVICE, "tld=1; Domain=com; Path=/");
        assert_eq!(
            jar.cookie_header("https://api.example.com/hub").as_deref(),
            Some("shared=1")
        );
        assert_eq!(
            jar.len(),
            1,
            "cookies for another domain or a whole top-level domain are ignored"
        );
        // A dot-less domain that is the host itself is a host cookie.
        jar.set_cookie("http://localhost/hub", "local=1; Domain=localhost; Path=/");
        assert_eq!(
            jar.cookie_header("http://localhost/hub").as_deref(),
            Some("local=1")
        );
    }

    #[test]
    fn cookie_jar_secure_cookies_go_only_over_tls_and_only_from_tls() {
        let jar = CookieJar::new();
        jar.set_cookie("https://service.example.com/hub", "token=1; Secure; Path=/");
        jar.set_cookie(SERVICE, "plain=1; Secure; Path=/");
        assert_eq!(
            jar.cookie_header("wss://service.example.com/hub")
                .as_deref(),
            Some("token=1")
        );
        assert_eq!(jar.cookie_header("ws://service.example.com/hub"), None);
    }

    #[test]
    fn cookie_jar_more_specific_paths_come_first_and_paths_match_by_segment() {
        let jar = CookieJar::new();
        jar.store(SERVICE, "a=root; Path=/", at(10));
        jar.store(SERVICE, "b=chat; Path=/chat", at(10));
        assert_eq!(
            jar.header_at("http://service.example.com/chat/x", at(11))
                .as_deref(),
            Some("b=chat; a=root")
        );
        assert_eq!(
            jar.header_at("http://service.example.com/chatter", at(11))
                .as_deref(),
            Some("a=root"),
            "/chat does not match /chatter"
        );
    }

    #[test]
    fn cookie_jar_expiry_replacement_and_removal() {
        let jar = CookieJar::new();
        jar.store(SERVICE, "session=1; Max-Age=60; Path=/", at(1_000));
        assert!(jar.header_at(SERVICE, at(1_059)).is_some());
        assert_eq!(jar.header_at(SERVICE, at(1_060)), None, "expired");
        jar.store(
            SERVICE,
            "id=a; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/",
            at(1_000),
        );
        assert_eq!(
            jar.header_at(SERVICE, at(1_000)).as_deref(),
            Some("id=a"),
            "Expires in 2015 is still ahead of 1970 + 1000 s"
        );
        jar.store(SERVICE, "id=b; Path=/", at(1_001));
        assert_eq!(jar.header_at(SERVICE, at(1_002)).as_deref(), Some("id=b"));
        jar.store(SERVICE, "id=gone; Max-Age=0; Path=/", at(1_003));
        assert_eq!(
            jar.header_at(SERVICE, at(1_004)),
            None,
            "Max-Age=0 removes it"
        );
        // Max-Age wins over Expires.
        jar.store(
            SERVICE,
            "both=1; Expires=Thu, 01 Jan 1970 00:00:01 GMT; Max-Age=100; Path=/",
            at(1_000),
        );
        assert!(jar.header_at(SERVICE, at(1_050)).is_some());
    }

    #[test]
    fn cookie_jar_parses_lenient_cookie_dates() {
        let expected = Some(UNIX_EPOCH + Duration::from_secs(1_445_412_480));
        for date in [
            "Wed, 21 Oct 2015 07:28:00 GMT",
            "Wednesday, 21-Oct-15 07:28:00 GMT",
            "Wed Oct 21 07:28:00 2015",
            "21 oct 2015 7:28:0",
        ] {
            assert_eq!(parse_cookie_date(date), expected, "{date}");
        }
        for invalid in [
            "",
            "Wed, 32 Oct 2015 07:28:00 GMT",
            "21 Oct 2015",
            "not a date",
        ] {
            assert_eq!(parse_cookie_date(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn cookie_jar_ignores_malformed_and_oversized_cookies() {
        let jar = CookieJar::new();
        for header in [
            "",
            "novalue",
            "=value",
            ";Path=/",
            "a b=1",
            "a=line\r\nbreak",
            "a=\u{1}",
            "a=caf\u{e9}",
            "na(me)=1",
        ] {
            jar.set_cookie(SERVICE, header);
        }
        jar.set_cookie(SERVICE, &format!("big={}", "x".repeat(4096)));
        jar.set_cookie("ftp://service.example.com/", "a=1");
        assert!(jar.is_empty());
    }

    #[test]
    fn cookie_jar_keeps_the_dotnet_capacities() {
        let jar = CookieJar::new();
        for index in 0..=PER_DOMAIN_CAPACITY {
            jar.store(
                SERVICE,
                &format!("c{index}=1; Path=/"),
                at(10 + index as u64),
            );
        }
        assert_eq!(jar.len(), PER_DOMAIN_CAPACITY);
        let header = jar.header_at(SERVICE, at(100)).unwrap();
        assert!(!header.contains("c0=1"), "the oldest was evicted: {header}");
    }

    #[test]
    fn cookie_jar_clones_share_cookies_and_debug_shows_no_values() {
        let jar = CookieJar::new();
        jar.clone().set_cookie(SERVICE, "secret=value; Path=/");
        assert_eq!(jar.len(), 1);
        let debug = format!("{jar:?}");
        assert!(!debug.contains("value"), "{debug}");
        jar.clear();
        assert!(jar.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cookie_jar_negotiate_cookies_reach_the_upgrade_and_every_reconnect() {
        use std::sync::Arc;

        use crate::communication::loopback_server::{LoopbackOptions, LoopbackServer};

        let server = LoopbackServer::start(LoopbackOptions {
            negotiate_cookie: Some("ARRAffinity=server-1; Path=/; HttpOnly"),
            ..LoopbackOptions::default()
        })
        .await;
        let port = i32::from(server.port);
        let jar = CookieJar::new();
        let shared = jar.clone();
        let client = crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.with_header("Cookie", "app=1");
            configuration.with_cookie_jar(shared.clone());
            configuration.with_reconnection_policy(crate::ReconnectionConfig {
                policy: Arc::new(crate::ConstantDelayPolicy::new(Duration::ZERO, Some(3))),
            });
        })
        .unwrap();
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        // The first negotiate has no cookie yet; the upgrade carries the application's cookie
        // and the jar's.
        assert_eq!(
            server.negotiate_requests()[0].header("cookie"),
            Some("app=1")
        );
        assert_eq!(
            server.upgrade_requests()[0].header("cookie"),
            Some("app=1; ARRAffinity=server-1")
        );
        assert_eq!(
            jar.cookie_header(&format!("http://127.0.0.1:{port}/hub"))
                .as_deref(),
            Some("ARRAffinity=server-1"),
            "the application's jar holds the cookie"
        );

        // The reconnect's negotiate and upgrade send it too.
        server.drop_connection();
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.upgrade_requests().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the client reconnects");
        assert_eq!(
            server.negotiate_requests()[1].header("cookie"),
            Some("app=1; ARRAffinity=server-1")
        );
        assert_eq!(
            server.upgrade_requests()[1].header("cookie"),
            Some("app=1; ARRAffinity=server-1")
        );
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cookie_jar_a_stateful_resume_sends_the_affinity_cookie_without_negotiating() {
        use crate::communication::loopback_server::{LoopbackOptions, LoopbackServer};

        let server = LoopbackServer::start(LoopbackOptions {
            stateful: true,
            negotiate_cookie: Some("ARRAffinity=server-1; Path=/"),
            ..LoopbackOptions::default()
        })
        .await;
        let port = i32::from(server.port);
        let client = crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.with_stateful_reconnect(crate::StatefulReconnectConfig::default());
        })
        .unwrap();
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.drop_connection();
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.resumes().0 < 1 || server.upgrade_requests().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the transport is resumed");
        assert_eq!(server.negotiations(), 1, "a resume negotiates nothing");
        let upgrades = server.upgrade_requests();
        assert_eq!(upgrades.len(), 2);
        assert_eq!(
            upgrades[1].header("cookie"),
            Some("ARRAffinity=server-1"),
            "the resume reaches the server the affinity cookie names"
        );
        client.disconnect();
    }

    #[test]
    fn cookie_jar_stores_every_set_cookie_header_of_a_response() {
        let jar = CookieJar::new();
        let mut headers = http::HeaderMap::new();
        headers.append(http::header::SET_COOKIE, "a=1; Path=/".parse().unwrap());
        headers.append(http::header::SET_COOKIE, "b=2; Path=/".parse().unwrap());
        jar.store_response(SERVICE, &headers);
        assert_eq!(jar.cookie_header(SERVICE).as_deref(), Some("a=1; b=2"));
    }
}
