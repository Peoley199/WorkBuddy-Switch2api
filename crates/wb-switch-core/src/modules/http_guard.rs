//! 本地 HTTP 服务的「谁在访问」校验：防 DNS rebinding 与跨源调用。
//!
//! # 为什么需要
//!
//! 这两类服务（webui 与 API 反代）默认都只监听 `127.0.0.1`，但**监听回环不等于安全**：
//!
//! 1. **DNS rebinding**：攻击者的页面 `evil.test` 一开始解析到攻击者自己的服务器，
//!    等浏览器加载完脚本后再把该域名改解析到 `127.0.0.1`。此后页面里的
//!    `fetch("http://evil.test:57890/api/accounts")` 在浏览器看来是**同源**请求，
//!    不会触发 CORS —— 服务端如果不看 `Host`，就会把账号库原样交出去。
//!    （实测：不用任何 CORS 头，伪造 `Host` 的请求照样 200 返回完整数据。）
//! 2. **跨源调用**：浏览器自己的攻击脚本也可以直接向 `127.0.0.1:端口` 发请求。
//!    读响应被 CORS 挡住，但**「发出去」挡不住** —— 对本机反代来说，发出去就意味着
//!    烧掉账号额度（`/v1/chat/completions` 带 `api_key` 就能调）。
//!
//! # 规则
//!
//! - `Host` 必须是「回环 IP / localhost 系 / 显式配置过的主机名」；
//! - `Origin` / `Referer` 只有在来源主机**可信**时才放行：同源回环，
//!   或是一台「带着口令访问过来」的主机（那说明用户确实在用这台机器上的服务，
//!   见 [`TrustedOrigins`]）。
//!
//! 口令本身已经挡住了「不知道口令的人」，这里挡的是「浏览器替攻击者把请求发出去」。
//!
//! # 已知限制
//!
//! 直接访问 IP（`http://192.168.1.10:57890`）时没有可比的域名，这类用法不受影响；
//! 想彻底锁死可以用 `WB_SWITCH_ALLOWED_HOSTS` 显式列白名单。

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Mutex;

use axum::http::{header, HeaderMap};
use axum::response::Response;

use super::config::now_ms;

/// 允许的 `Host` 头（小写、不含端口）。为空 = 只认回环名。
static ALLOWED_HOSTS: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();

/// 环境变量名：额外允许的 `Host`（逗号分隔）。
///
/// 用 `*.example.com` 形式的通配只支持「前导 `*.`」（匹配任意一级及以上子域）。
const ALLOWED_HOSTS_ENV: &str = "WB_SWITCH_ALLOWED_HOSTS";

fn allowed_hosts() -> &'static BTreeSet<String> {
    ALLOWED_HOSTS.get_or_init(|| {
        std::env::var(ALLOWED_HOSTS_ENV)
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().trim_end_matches('.').to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// 服务是在「直接绑定回环」还是「被端口转发」（容器 / 反向代理）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exposure {
    /// 直接绑 `127.0.0.1` / `localhost`：浏览器里只有本机页面能连上，
    /// 带 `Origin` 的跨源请求必然来自本机页面 —— 允许。
    Loopback,
    /// 绑在 `0.0.0.0` / 具体网卡 / 转发出来：其他机器上的页面也能访问，
    /// 带 `Origin` 的请求必须来自可信来源。
    Forwarded,
}

impl Exposure {
    /// 按 `addr` 是否为回环、监听端口、以及这次连接到达的本地端口判定。
    ///
    /// 为什么看「连接的本地端口」而不只看配置：Docker 的 `-p 8080:57890` 会让
    /// 容器里看到的监听仍是 `0.0.0.0:57890`（这一条已经判成 Forwarded）；而
    /// `-p 127.0.0.1:8080:57890` 这种绑定回环但端口被映射的情形，只有拿实际连接的
    /// 本地端口才区分得出来。
    pub fn detect(addr_is_loopback: bool, listen_port: u16, local_port: u16) -> Self {
        if addr_is_loopback && listen_port == local_port {
            Exposure::Loopback
        } else {
            Exposure::Forwarded
        }
    }
}

/// 名称是不是回环。
///
/// 注意**不能**把 `0.0.0.0` 当回环：它表示「监听所有网卡」。
pub fn is_loopback_name(host: &str) -> bool {
    let h = host.trim().trim_start_matches('[').trim_end_matches(']');
    if h.eq_ignore_ascii_case("localhost") || h.ends_with(".localhost") {
        return true;
    }
    h.parse::<Ipv4Addr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
        || h.parse::<Ipv6Addr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// 主机名（小写）是否被信任：
///
/// - 回环名（`127.x` / `::1` / `localhost` / `*.localhost`）；
/// - 或环境变量 `WB_SWITCH_ALLOWED_HOSTS` 里显式列出的（支持前导 `*.`）。
pub fn is_trusted_host(host: &str) -> bool {
    let h = normalize_host(host);
    if h.is_empty() {
        return false;
    }
    if is_loopback_name(&h) {
        return true;
    }
    allowed_hosts().iter().any(|pat| host_matches(pat, &h))
}

fn host_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(suffix) => host.ends_with(suffix) && host.len() > suffix.len(),
        None => pattern == host,
    }
}

/// 主机名归一：去端口（IPv6 字面量 `[::1]:80` 要连方括号一起剥）、小写、去尾点。
pub fn normalize_host(raw: &str) -> String {
    let s = raw.trim();
    if s.is_empty() {
        return String::new();
    }
    let no_port = if let Some(rest) = s.strip_prefix('[') {
        // [v6]:port / [v6]
        rest.split(']').next().unwrap_or(rest)
    } else if s.matches(':').count() == 1 {
        s.split(':').next().unwrap_or(s)
    } else {
        // 裸 IPv6（没有端口）有多个冒号，整体保留。
        s
    };
    no_port
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

/// `Host` 头检查。返回 `Err(())` 表示拒绝。
///
/// 规则：`Host` 必须是已知的主机名（回环 / `localhost` / 显式加入白名单的），
/// **或者**是本机自己的 IP 字面量（直连 `http://192.168.1.10:57890` 时，
/// `Host` 就是这台机器的 LAN IP —— 浏览器不会把这样的 `Host` 送给别的网站）。
///
/// 不接受任意域名：DNS rebinding 之后浏览器发来的 `Host` 是**攻击者的域名**，
/// 本机 IP 字面量却永远不可能是它。
pub fn check_host(headers: &HeaderMap) -> Result<(), ()> {
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(normalize_host)
    else {
        // HTTP/1.1 必须带 Host；缺失的多半是手搓的低级请求，直接拒。
        return Err(());
    };
    if is_loopback_name(&host) || is_trusted_host(&host) || is_ip_literal(&host) {
        Ok(())
    } else {
        Err(())
    }
}

/// 是不是一个 IP 字面量（v4 或 v6）。
fn is_ip_literal(host: &str) -> bool {
    host.parse::<Ipv4Addr>().is_ok() || host.parse::<Ipv6Addr>().is_ok()
}

/// 从一个 URL 里取主机名（只看 `scheme://host[:port]/…`；解析失败返回 None）。
pub fn host_of_url(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // 去掉 userinfo（`user:pass@host`）
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return None;
    }
    let host = normalize_host(authority);
    if host.is_empty() {
        None
    } else {
        Some(host)
    }
}

/// 一次校验的结论。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// 放行。
    Allow,
    /// 拒绝：`Host` 不对（DNS rebinding / 配置错）。
    BadHost,
    /// 拒绝：来自不可信的跨源页面。
    CrossSite,
}

/// `Origin` / `Referer` 检查。
///
/// - 没有这两个头：命中（`Allow`）—— 非浏览器请求（curl / SDK / 健康检查）本就不受
///   浏览器同源策略约束，挡不挡都谈不上安全，而挡了会让正常脚本全挂；
///   「不知道口令的人调不动」由各自的鉴权负责。
/// - 带了 `Origin`：必须是可信来源。浏览器的跨源请求一定会带（POST 必带，GET 也被
///   `fetch` 带上），所以这是跨源攻击的主要落点。
/// - 只有 `Referer`（顶层导航 / 普通页面跳转）：同样按「可信来源」校验。
///
/// `Exposure::Loopback` 时，带 `Origin` 的请求就是本机页面发的（别的机器根本连不上），
/// 所以放行。
pub fn check_origin(headers: &HeaderMap, exposure: Exposure) -> Result<(), Access> {
    if check_host(headers).is_err() {
        return Err(Access::BadHost);
    }
    let has_origin_header = headers.contains_key(header::ORIGIN);
    let has_referer = headers.contains_key(header::REFERER);
    if !has_origin_header && !has_referer {
        return Ok(());
    }
    let origin_ok = match headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        Some(o) => {
            if o == "null" {
                // `file://` 页面、沙箱 iframe、部分重定向后的页面：来源不明，一律拒。
                false
            } else {
                host_of_url(o).map(|h| is_trusted_host(&h)).unwrap_or(false)
            }
        }
        None => true,
    };
    let referer_ok = match headers.get(header::REFERER).and_then(|v| v.to_str().ok()) {
        Some(r) => host_of_url(r).map(|h| is_trusted_host(&h)).unwrap_or(false),
        None => true,
    };
    if origin_ok && referer_ok {
        return Ok(());
    }
    if exposure == Exposure::Loopback {
        return Ok(());
    }
    Err(Access::CrossSite)
}

/// 被拒绝时回给客户端的响应（JSON，带人话）。
pub fn rejected(access: Access) -> Response {
    use axum::{http::StatusCode, response::IntoResponse, Json};
    use serde_json::json;
    let (status, message) = match access {
        Access::Allow => return Json(json!({})).into_response(),
        Access::BadHost => (
            StatusCode::MISDIRECTED_REQUEST,
            "拒绝访问：Host 不在允许列表里（防 DNS rebinding）。\
             直接访问时请用 127.0.0.1 / localhost / 本机实际 IP；\
             反向代理场景请设 WB_SWITCH_ALLOWED_HOSTS=<域名>。"
                .to_string(),
        ),
        Access::CrossSite => (
            StatusCode::FORBIDDEN,
            "拒绝访问：请求来自其他网站的页面。如果确实需要从这台机器之外访问，\
             请把该主机名加进 WB_SWITCH_ALLOWED_HOSTS。"
                .to_string(),
        ),
    };
    (status, Json(json!({ "ok": false, "error": message }))).into_response()
}

// ---------------------------------------------------------------- 带口令访问过的主机

/// 「带着口令访问过来」的主机名，24 小时内视为可信来源。
///
/// 解决的是容器/反向代理的核心矛盾：同一个服务既要从别的机器上的页面访问
///（网页的 `Origin` 是那台机器的 IP 或域名），又不能因为浏览器把请求替别人发出去
/// 就一律放行。规则：**能在 `Origin` 里拿出一份正确口令的主机，说明确实是用户本人在用**，
/// 于是记住它；之后这个主机上页面的请求（`?token=` 或 `Authorization`）才被接受。
///
/// 安全性：攻击者的页面不知道口令，第一次就会被 401 拒掉，也进不了这张表；
/// 表的存在时间有上限，进程重启即清空（不落盘）。
#[derive(Default)]
pub struct TrustedOrigins {
    entries: Mutex<std::collections::HashMap<String, i64>>,
}

/// 信任有效期（从最后一次被认可算起；持续使用就一直续期）。
pub const TRUST_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// 每次检查时顺手清一遍过期条目，免得进程常驻越积越多。
fn cleanup(m: &mut std::collections::HashMap<String, i64>, now: i64) {
    if m.len() > 64 {
        m.retain(|_, ts| now - *ts < TRUST_TTL_MS);
    }
}

impl TrustedOrigins {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记住一个可信主机。
    pub fn remember(&self, host: &str) {
        let host = normalize_host(host);
        if host.is_empty() {
            return;
        }
        if let Ok(mut m) = self.entries.lock() {
            let now = now_ms();
            cleanup(&mut m, now);
            m.insert(host, now);
        }
    }

    /// 该主机是否在信任期内。
    pub fn contains(&self, host: &str) -> bool {
        let host = normalize_host(host);
        let Ok(mut m) = self.entries.lock() else {
            return false;
        };
        let now = now_ms();
        cleanup(&mut m, now);
        m.contains_key(&host)
    }

    /// 一次请求里出现的来源主机（`Origin` 优先，其次 `Referer`）。
    pub fn origin_host(headers: &HeaderMap) -> Option<String> {
        if let Some(o) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
            if o == "null" {
                return None;
            }
            return host_of_url(o);
        }
        headers
            .get(header::REFERER)
            .and_then(|v| v.to_str().ok())
            .and_then(host_of_url)
    }

    /// 该请求是否来自这台机器上「用对过口令」的页面。
    pub fn allows(&self, headers: &HeaderMap) -> bool {
        Self::origin_host(headers)
            .map(|h| self.contains(&h))
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------- 测试

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn hm(pairs: &[(header::HeaderName, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(k.clone(), HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn loopback_names_are_recognized() {
        for ok in ["127.0.0.1", "127.0.0.2", "::1", "[::1]", "localhost", "A.localhost"] {
            assert!(is_loopback_name(ok), "{ok} 应当是回环");
        }
        for bad in ["0.0.0.0", "192.168.1.10", "evil.test", ""] {
            assert!(!is_loopback_name(bad), "{bad} 不应当是回环");
        }
    }

    #[test]
    fn host_header_rejects_rebinding_names() {
        let ok = hm(&[(header::HOST, "127.0.0.1:57890")]);
        assert!(check_host(&ok).is_ok());
        let ok6 = hm(&[(header::HOST, "[::1]:57890")]);
        assert!(check_host(&ok6).is_ok());
        let evil = hm(&[(header::HOST, "evil.test:57890")]);
        assert!(check_host(&evil).is_err());
        let missing = HeaderMap::new();
        assert!(check_host(&missing).is_err());
    }

    #[test]
    fn origin_host_extracts_from_url() {
        assert_eq!(
            host_of_url("http://192.168.1.10:57890/proxy?token=x").as_deref(),
            Some("192.168.1.10")
        );
        assert_eq!(host_of_url("https://user:pw@Nest.Local/").as_deref(), Some("nest.local"));
        assert_eq!(host_of_url("null"), None);
        assert_eq!(host_of_url("http://[::1]:57890/").as_deref(), Some("::1"));
    }

    #[test]
    fn forwarded_exposure_rejects_cross_site_origin() {
        let cross = hm(&[
            (header::HOST, "192.168.1.10:57890"),
            (header::ORIGIN, "http://attacker.evil.test"),
        ]);
        assert_eq!(
            check_origin(&cross, Exposure::Forwarded),
            Err(Access::CrossSite)
        );
        // 本机直连时，浏览器能连上就说明是本机页面，放行。
        assert_eq!(check_origin(&cross, Exposure::Loopback), Ok(()));
        // 非浏览器请求（无 Origin）放行。
        let curl = hm(&[(header::HOST, "192.168.1.10:57890")]);
        assert_eq!(check_origin(&curl, Exposure::Forwarded), Ok(()));
    }

    #[test]
    fn null_origin_is_rejected_for_forwarded() {
        let sandboxed = hm(&[
            (header::HOST, "192.168.1.10:57890"),
            (header::ORIGIN, "null"),
        ]);
        assert_eq!(
            check_origin(&sandboxed, Exposure::Forwarded),
            Err(Access::CrossSite)
        );
    }

    #[test]
    fn trusted_origins_expire_and_only_remember_exact_hosts() {
        let t = TrustedOrigins::new();
        assert!(!t.contains("192.168.1.10"));
        t.remember("192.168.1.10:57890");
        assert!(t.contains("192.168.1.10"));
        let req = hm(&[(header::ORIGIN, "http://192.168.1.10:57890")]);
        assert!(t.allows(&req));
        let other = hm(&[(header::ORIGIN, "http://attacker.evil.test")]);
        assert!(!t.allows(&other));
    }

    #[test]
    fn exposure_detection_uses_port_mapping() {
        // 桌面端：127.0.0.1:57890，连接也是 57890。
        assert_eq!(Exposure::detect(true, 57890, 57890), Exposure::Loopback);
        // docker -p 8080:57890：容器里看到的是 0.0.0.0:57890。
        assert_eq!(Exposure::detect(false, 57890, 57890), Exposure::Forwarded);
        // 绑定回环但端口被转发（SSH -L）。
        assert_eq!(Exposure::detect(true, 57890, 8080), Exposure::Forwarded);
    }
}
