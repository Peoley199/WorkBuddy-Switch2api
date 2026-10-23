//! 安全姿态：
//! - **默认**只绑 `127.0.0.1`、不校验口令 —— token 不出本机，与以前一致；
//! - 一旦把监听地址改成非回环（Docker / 反代），**入口会强制要求设置访问口令**
//!   （见 `main.rs` 的 `resolve_bind`），中间件 [`auth_guard`] 校验 `/api/*`。
//!   这两道是配套的：只改地址不设口令，等于把账号库挂到网上。
//! - 口令之外还有一道「谁在访问」校验（[`wb_switch_core::modules::http_guard`]）：
//!   `Host` 必须是回环 / 本机 IP / 白名单域名，否则直接 421 —— 挡的是 **DNS rebinding**
//!   （攻击者页面把自己的域名解析到 `127.0.0.1` 后，浏览器把它当同源，CORS 管不到）。
//! - 从**别的机器**上的页面访问时，浏览器带的 `Origin` 是那台机器的地址。这里采用
//!   「**凭口令认主机**」：第一次必须用 `Authorization: Bearer <口令>`，成功后记住该来源主机
//!   24 小时；地址栏里的 `?token=` 只在**顶层导航**里被拿来 302 跳转一次
//!   （见 [`auth_guard`] 里的 `__auth/boot` 分支），跳完就从地址栏消失。

use std::sync::{Mutex, OnceLock};

#[cfg(target_os = "windows")]
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{RawQuery, Request};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use serde_json::{json, Value};

use wb_switch_core::modules::http_guard::{self, Access, Exposure, TrustedOrigins};

/// 访问口令。`None` = 不校验（本机默认行为，与以前完全一致）。
///
/// Docker / 反向代理场景下必须设：本服务能读写账号库、能切号、能签到，
/// 裸奔在网络上等于把账号交出去。
static ACCESS_TOKEN: OnceLock<Option<String>> = OnceLock::new();

/// 由入口（`main.rs`）在 `router()` 之前调用，设置访问口令。
pub fn set_access_token(token: Option<String>) {
    let _ = ACCESS_TOKEN.set(token);
}

static FORCED_EXPOSURE: OnceLock<bool> = OnceLock::new();

/// 由入口在 `router()` 之前调用：这次服务不是「直接绑回环」（容器 / 端口转发）。
pub fn set_forwarded(forwarded: bool) {
    let _ = FORCED_EXPOSURE.set(forwarded);
}

fn access_token() -> Option<&'static str> {
    ACCESS_TOKEN
        .get()
        .and_then(|t| t.as_deref())
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// 这次连接来自「别处」：听的不是回环，或连进来的端口 ≠ 监听端口（Docker 映射 / SSH 转发）。
fn exposure_of(headers: &HeaderMap, local_port: u16) -> Exposure {
    if FORCED_EXPOSURE.get().copied().unwrap_or(false) {
        return Exposure::Forwarded;
    }
    let (host, host_port) = match headers.get(header::HOST).and_then(|v| v.to_str().ok()) {
        Some(raw) => {
            let port = raw
                .rsplit_once(':')
                .and_then(|(_, p)| p.trim_end_matches(']').parse::<u16>().ok());
            (http_guard::normalize_host(raw), port)
        }
        None => (String::new(), None),
    };
    let host_is_loopback = host.is_empty() || http_guard::is_loopback_name(&host);
    // 拿不到端口的旧式请求不做端口比对（按 0 处理会让一切变成 Forwarded）。
    Exposure::detect(host_is_loopback, host_port.unwrap_or(0), local_port)
}

fn trusted() -> &'static TrustedOrigins {
    static T: OnceLock<TrustedOrigins> = OnceLock::new();
    T.get_or_init(TrustedOrigins::new)
}

/// 记下本机来源（`main.rs` 启动浏览器前调用）。本机上打开页面走的是 `127.0.0.1` /
/// `localhost`，本来就与「能连上这个回环服务」等价；记下来是为了容器环境下同样放行。
pub fn trust_loopback_origin() {
    for h in ["127.0.0.1", "localhost", "::1"] {
        trusted().remember(h);
    }
}

/// 极简 query 解析：只按 `&` / `=` 切，不做百分号转义。
///
/// 口令要求只用 `[A-Za-z0-9._~-]`（URL 里不需要转义的字符），
/// 所以这里不引 `serde_urlencoded` 也够用。
fn query_pairs(q: &str) -> Vec<(&str, &str)> {
    q.split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| kv.split_once('=').unwrap_or((kv, "")))
        .collect()
}

/// 带 `?token=` 的 GET 地址（文档 / 启动横幅推荐的那条）落到这里做一次 302。
/// 地址栏口令入口（`main.rs` 的启动横幅也用它拼地址）。
pub const BOOTSTRAP_PATH: &str = "/__auth/boot";

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
}

fn query_token(uri: &Uri) -> Option<String> {
    uri.query()
        .map(query_pairs)
        .unwrap_or_default()
        .into_iter()
        .find(|(k, _)| *k == "token")
        .map(|(_, v)| v.to_string())
}

/// 常数时间比较：先比长度再逐字节，别让口令成为计时侧信道的突破口。
fn same_token(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "message": "未授权：请带上访问口令（首次访问用 ?token=…，之后前端会自动带 Bearer）"
        })),
    )
        .into_response()
}

/// 访问口令中间件。
///
/// 校验顺序：
/// 1. `Host` 不合法（DNS rebinding）⇒ 421，**任何路径都拒**；
/// 2. 静态资源（HTML / JS / CSS）放行 —— 它们不含用户数据，页面加载后才会带着
///    `Bearer` 去请求数据；
/// 3. `?token=…`（页面加载时由前端跳来）：只接受**导航式**请求
///    （`Sec-Fetch-Mode: navigate`，或无 `Sec-Fetch-*` 头的旧浏览器里 `Accept` 含 `text/html`），
///    验证口令后 302 到去掉 token 的地址；`<img>` / `<iframe>` / `fetch` 嵌入一律拒绝，
///    口令因此不会被第三方页面的标注请求带进日志；
/// 4. 其余 `/api/*`：`Authorization: Bearer`，或 URL 里的 `?token=`（兼容本地脚本）。
///    来源主机与监听地址不同时（别的机器上的页面），还要求该来源已经「凭口令」被记住过。
async fn auth_guard(req: Request, next: Next) -> Response {
    if http_guard::check_host(req.headers()).is_err() {
        return http_guard::rejected(Access::BadHost);
    }
    let Some(want) = access_token() else {
        return next.run(req).await;
    };
    let path = req.uri().path().to_string();
    let boot = path == BOOTSTRAP_PATH;
    if !boot && !path.starts_with("/api/") {
        return next.run(req).await;
    }

    let has_header = bearer_token(req.headers())
        .map(|t| same_token(t, want))
        .unwrap_or(false);
    let has_query = !boot && query_token(req.uri()).map(|t| same_token(&t, want)).unwrap_or(false);

    if boot {
        let navigating = match req
            .headers()
            .get("sec-fetch-mode")
            .and_then(|v| v.to_str().ok())
        {
            Some(m) => m.eq_ignore_ascii_case("navigate"),
            // 老浏览器不带 Sec-Fetch-*：退化成「Accept 里有 text/html」。
            None => req
                .headers()
                .get(header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .map(|a| a.contains("text/html"))
                .unwrap_or(false),
        };
        if !navigating && !has_header {
            return (StatusCode::FORBIDDEN, "只接受浏览器地址栏 / 链接的跳转").into_response();
        }
        if !has_header && !(query_token(req.uri()).map(|t| same_token(&t, want)).unwrap_or(false)) {
            return unauthorized();
        }
        if let Some(host) = TrustedOrigins::origin_host(req.headers()) {
            trusted().remember(&host);
        }
        // 302 到去掉 token 的地址：口令不再留在地址栏、历史记录、截图里。
        let rest: Vec<String> = req
            .uri()
            .query()
            .map(query_pairs)
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| *k != "token")
            .map(|(k, v)| {
                if v.is_empty() {
                    k.to_string()
                } else {
                    format!("{k}={v}")
                }
            })
            .collect();
        let target = if rest.is_empty() {
            path
        } else {
            format!("{path}?{}", rest.join("&"))
        };
        return Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, target)
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::REFERRER_POLICY, "no-referrer")
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    if !has_header && !has_query {
        return unauthorized();
    }

    // 来源主机与监听地址不同：必须已经「凭口令」被记住过（见 [`TrustedOrigins`]）。
    let local_port = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.port())
        .unwrap_or(0);
    let exposure = exposure_of(req.headers(), local_port);
    if let Err(access) = http_guard::check_origin(req.headers(), exposure) {
        if !trusted().allows(req.headers()) {
            return http_guard::rejected(access);
        }
    }
    if exposure == Exposure::Forwarded {
        // 🔴 只有「别的机器也访问得到」时才凭口令认主机。
        // 本机直连（Loopback）时不记：否则本机上任何页面只要拿到一次口令，
        // 对应的 Host 就会被信任 —— 而本地回环本来就该只认回环名。
        if let Some(host) = TrustedOrigins::origin_host(req.headers()) {
            // 校验通过的请求顺手给来源续期（持续使用就一直有效）。
            trusted().remember(&host);
        }
    }
    next.run(req).await
}
use wb_switch_core::modules::{
    account, auth_file, checkin, codebuddy_cli, codebuddy_cn_ide, config, credit_usage, credits,
    edition::Edition, export_import,
    oauth, process, proxy, refresh, rotate, session, switch, token_stats, travel, update,
    update_guard,
};

/// WorkBuddy 运行状态缓存：Windows 上检测要跑 tasklist（慢），缓存几秒避免
/// 前端切 tab 频繁触发命令行导致卡顿/闪窗。
#[cfg(target_os = "windows")]
static RUNNING_CACHE: Mutex<Option<(Instant, bool)>> = Mutex::new(None);

fn cached_workbuddy_running() -> bool {
    #[cfg(target_os = "windows")]
    {
        let mut cache = RUNNING_CACHE.lock().unwrap();
        if let Some((t, v)) = cache.as_ref() {
            if t.elapsed() < Duration::from_secs(3) {
                return *v;
            }
        }
        let v = process::is_workbuddy_running();
        *cache = Some((Instant::now(), v));
        v
    }
    #[cfg(not(target_os = "windows"))]
    {
        process::is_workbuddy_running()
    }
}

#[derive(RustEmbed)]
#[folder = "../../dist/"]
struct Assets;

/// 切换进度缓存：webui 通过 GET /api/switch/progress 轮询。
static SWITCH_PROGRESS: Mutex<Option<String>> = Mutex::new(None);
static SWITCH_RUNNING: Mutex<bool> = Mutex::new(false);

pub fn router() -> Router {
    Router::new()
        // 地址栏里的口令入口（`/__auth/boot?token=…`，任何带 `?token=` 的 GET 都会被
        // `main.rs` 改写到这里）：只接受顶层导航，302 到去掉口令的 `/`。
        .route(BOOTSTRAP_PATH, get(auth_bootstrap_redirect))
        .route("/api/status", get(api_status))
        .route("/api/accounts", get(api_accounts))
        .route("/api/codebuddy-cli/status", get(api_codebuddy_cli_status))
        .route(
            "/api/codebuddy-cli/install-helper",
            post(api_codebuddy_cli_install_helper),
        )
        .route("/api/codebuddy-cli/switch", post(api_codebuddy_cli_switch))
        .route("/api/codebuddy-cn-ide/status", get(api_codebuddy_cn_ide_status))
        .route("/api/codebuddy-cn-ide/switch", post(api_codebuddy_cn_ide_switch))
        .route("/api/codebuddy-cn-ide/detect", post(api_codebuddy_cn_ide_detect))
        .route("/api/delete", post(api_delete))
        .route("/api/oauth/start", post(api_oauth_start))
        .route("/api/oauth/status", post(api_oauth_status))
        .route("/api/import-local", post(api_import_local))
        .route("/api/export-accounts", post(api_export_accounts))
        .route(
            "/api/export-accounts-to-path",
            post(api_export_accounts_to_path),
        )
        .route("/api/import/preview", post(api_preview_import))
        .route("/api/import", post(api_import))
        .route("/api/switch", post(api_switch))
        .route("/api/switch/progress", get(api_switch_progress))
        .route("/api/sessions", get(api_sessions))
        .route("/api/sessions/copy", post(api_copy_sessions))
        .route("/api/checkin/status", get(api_checkin_status))
        .route("/api/credits", post(api_credits))
        .route("/api/credits/stats", get(api_credit_statistics))
        .route("/api/token-stats", get(api_token_statistics))
        .route("/api/checkin", post(api_checkin))
        .route("/api/checkin/all", post(api_checkin_all))
        .route(
            "/api/checkin/config",
            get(api_checkin_config).post(api_save_checkin_config),
        )
        .route("/api/checkin/logs", get(api_checkin_logs))
        .route("/api/travel/status", get(api_travel_status))
        .route(
            "/api/travel/config",
            get(api_travel_config).post(api_save_travel_config),
        )
        .route(
            "/api/rotate/config",
            get(api_rotate_config).post(api_save_rotate_config),
        )
        .route("/api/rotate/status", get(api_rotate_status))
        .route("/api/rotate/run", post(api_rotate_run))
        .route("/api/rotate/logs", get(api_rotate_logs))
        .route("/api/refresh-token", post(api_refresh_token))
        .route("/api/update/check", get(api_update_check))
        .route(
            "/api/update/config",
            get(api_update_config).post(api_save_update_config),
        )
        .route("/api/update-guard/status", get(api_update_guard_status))
        .route("/api/update-guard/set", post(api_update_guard_set))
        .route(
            "/api/update-guard/clear-cache",
            post(api_update_guard_clear_cache),
        )
        .route(
            "/api/update-guard/set-frozen",
            post(api_update_guard_set_frozen),
        )
        // API 反代（容器 / CLI 形态靠这几个才配得起来）
        .route("/api/proxy/config", get(api_proxy_config).post(api_proxy_save))
        .route("/api/proxy/status", get(api_proxy_status))
        .route("/api/proxy/usage", get(api_proxy_usage))
        .route("/api/proxy/usage/reset", post(api_proxy_usage_reset))
        .route("/api/proxy/models", get(api_proxy_models))
        .route("/api/proxy/restart", post(api_proxy_restart))
        .fallback(static_handler)
        // 🔴 必须在所有路由**之后**挂：layer 只对它之前注册的项生效。
        // 没设口令时这层是空操作，本机行为与以前一模一样。
        .layer(axum::middleware::from_fn(auth_guard))
}

/// 顶层导航到 `/__auth/boot?token=…` 之后，302 回不带口令的 `/`。
///
/// 口令本身由 [`auth_guard`] 校验；这里只负责把地址栏里的口令抹掉。
async fn auth_bootstrap_redirect() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, "/")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::REFERRER_POLICY, "no-referrer")
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn json_ok(v: Value) -> Response {
    Json(v).into_response()
}

fn json_err(e: String, code: StatusCode) -> Response {
    (code, Json(json!({ "ok": false, "error": e }))).into_response()
}

// ---------------------------------------------------------------------------
// 状态 / 账号
// ---------------------------------------------------------------------------

async fn api_status() -> Response {
    // 桌面通道（commands.rs）同款：两个版本各读各的鉴权文件；
    // 展示字段口径走 core 的共享实现，别在这条路上重写一遍。
    json_ok(json!({
        "running": cached_workbuddy_running(),
        "authFile": auth_file::auth_file_path().to_string_lossy(),
        "current": auth_file::current_account_summary_for(Edition::Domestic),
        "currentInternational": auth_file::current_account_summary_for(Edition::International),
        "appPath": auth_file::workbuddy_app_path().to_string_lossy(),
        "version": update::APP_VERSION,
    }))
}

async fn api_accounts() -> Response {
    json_ok(json!({
        "accounts": account::load_accounts()
            .iter()
            .map(account::account_meta)
            .collect::<Vec<_>>(),
        "current": auth_file::read_auth_file()
            .and_then(|a| a.get("account").and_then(|x| x.get("uid")).and_then(|x| x.as_str()).map(String::from)),
    }))
}

async fn api_codebuddy_cli_status() -> Response {
    json_ok(codebuddy_cli::status())
}

async fn api_codebuddy_cli_install_helper() -> Response {
    match codebuddy_cli::install_helper() {
        Ok(result) => json_ok(result),
        Err(error) => json_err(error, StatusCode::BAD_REQUEST),
    }
}

async fn api_codebuddy_cli_switch(Json(body): Json<Value>) -> Response {
    let id = body.get("accountId").and_then(|v| v.as_str()).unwrap_or("");
    match codebuddy_cli::set_active_account(id) {
        Ok(result) => json_ok(result),
        Err(error) => json_err(error, StatusCode::BAD_REQUEST),
    }
}

async fn api_codebuddy_cn_ide_status() -> Response {
    json_ok(codebuddy_cn_ide::status())
}

async fn api_codebuddy_cn_ide_switch(Json(body): Json<Value>) -> Response {
    let account_id = body
        .get("accountId")
        .or_else(|| body.get("account_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let restart = body.get("restart").and_then(|v| v.as_bool()).unwrap_or(true);
    match codebuddy_cn_ide::switch_account(account_id, restart) {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_codebuddy_cn_ide_detect() -> Response {
    match codebuddy_cn_ide::detect_current_account() {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}


async fn api_delete(Json(body): Json<Value>) -> Response {
    let id = body.get("accountId").and_then(|v| v.as_str()).unwrap_or("");
    match account::delete_account(id) {
        Ok(()) => json_ok(json!({ "ok": true })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_import_local() -> Response {
    match account::import_local() {
        Ok(acc) => json_ok(json!({ "ok": true, "account": acc })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

// ---------------------------------------------------------------------------
// 导出 / 导入账号
// ---------------------------------------------------------------------------

async fn api_export_accounts(Json(body): Json<Value>) -> Response {
    let ids: Vec<String> = body
        .get("accountIds")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    match export_import::export_accounts(&ids) {
        Ok(records) => json_ok(json!({ "ok": true, "accounts": records })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_export_accounts_to_path(Json(body): Json<Value>) -> Response {
    let ids: Vec<String> = body
        .get("accountIds")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let path = body
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    match export_import::export_accounts_to_path(&ids, &path) {
        Ok(path) => json_ok(json!({ "ok": true, "path": path })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_preview_import(Json(body): Json<Value>) -> Response {
    let text = body
        .get("fileText")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    match export_import::preview_accounts(&text) {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_import(Json(body): Json<Value>) -> Response {
    let text = body
        .get("fileText")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let indexes: Vec<usize> = body
        .get("indexes")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    match export_import::import_accounts(&text, &indexes) {
        Ok(result) => json_ok(json!({
            "ok": true,
            "imported": result.imported,
            "skipped": result.skipped,
            "overwritten": result.overwritten,
        "encrypted": result.encrypted,
        })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

// ---------------------------------------------------------------------------
// OAuth 登录
// ---------------------------------------------------------------------------

async fn api_oauth_start(Json(body): Json<Value>) -> Response {
    // 缺省国内版；传 international 走国际版域名与 platform=workbuddy-ai
    let edition = body
        .get("edition")
        .and_then(|v| v.as_str())
        .map(wb_switch_core::modules::edition::parse_lenient)
        .unwrap_or_default();
    match oauth::oauth_start_for(edition).await {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

async fn api_oauth_status(Json(body): Json<Value>) -> Response {
    let login_id = body
        .get("loginId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    json_ok(oauth::oauth_poll(&login_id).await)
}

// ---------------------------------------------------------------------------
// 切换
// ---------------------------------------------------------------------------

async fn api_switch(Json(body): Json<Value>) -> Response {
    let account_id = body
        .get("accountId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if account_id.trim().is_empty() {
        return json_err("缺少 accountId".to_string(), StatusCode::BAD_REQUEST);
    }
    let restart = body
        .get("restart")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let share_sessions = body
        .get("shareSessions")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let copy_ids: Vec<String> = body
        .get("copySessionIds")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    {
        let mut running = SWITCH_RUNNING.lock().unwrap();
        if *running {
            return json_err("已有切换任务进行中".to_string(), StatusCode::CONFLICT);
        }
        *running = true;
        *SWITCH_PROGRESS.lock().unwrap() = Some("开始切换账号…".to_string());
    }

    let progress: switch::ProgressFn = Box::new(|msg| {
        *SWITCH_PROGRESS.lock().unwrap() = Some(msg.to_string());
    });

    let result = tokio::task::spawn_blocking(move || {
        switch::switch_account(
            Some(&progress),
            &account_id,
            restart,
            share_sessions,
            &copy_ids,
        )
    })
    .await;

    *SWITCH_RUNNING.lock().unwrap() = false;

    match result {
        Ok(Ok(v)) => json_ok(v),
        Ok(Err(e)) => json_err(e, StatusCode::BAD_REQUEST),
        Err(e) => json_err(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn api_switch_progress() -> Response {
    let p = SWITCH_PROGRESS.lock().unwrap().clone();
    let running = *SWITCH_RUNNING.lock().unwrap();
    json_ok(json!({ "running": running, "progress": p }))
}

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

async fn api_sessions(RawQuery(query): RawQuery) -> Response {
    // ?edition=international 走国际版会话库；缺省国内版
    let edition = query
        .as_deref()
        .and_then(|q| {
            q.split('&')
                .filter_map(|kv| kv.split_once('='))
                .find(|(k, _)| *k == "edition")
                .map(|(_, v)| v.to_string())
        })
        .map(|v| wb_switch_core::modules::edition::parse_lenient(&v))
        .unwrap_or_default();
    match session::current_user_uid_for(edition) {
        Some(uid) => json_ok(json!({
            "sessions": session::list_sessions_for_user_for(edition, &uid),
            "current": uid,
            "edition": edition.key(),
        })),
        None => json_ok(json!({ "sessions": [], "current": null, "edition": edition.key() })),
    }
}

async fn api_copy_sessions(Json(body): Json<Value>) -> Response {
    let target_account_id = body
        .get("targetAccountId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let session_ids: Vec<String> = body
        .get("sessionIds")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let Some(target) = account::find_account(&target_account_id) else {
        return json_err("目标账号不存在".to_string(), StatusCode::BAD_REQUEST);
    };
    // 档位取自目标账号自身，保证读写该档位的会话库
    let edition = wb_switch_core::modules::edition::edition_of(&target);
    let source_uid = session::current_user_uid_for(edition);
    let result = session::copy_sessions_for_switch_for(edition, &target, &session_ids);
    json_ok(json!({
        "sourceUid": source_uid,
        "targetUid": target.get("uid"),
        "edition": edition.key(),
        "copied": result,
    }))
}

// ---------------------------------------------------------------------------
// 签到 / 保活
// ---------------------------------------------------------------------------

async fn api_checkin_status() -> Response {
    let list = account::load_accounts();
    let mut items = Vec::new();
    for acc in &list {
        let status = checkin::get_checkin_status(acc).await;
        items.push(checkin_status_item(acc, status));
    }
    json_ok(json!({ "accounts": items }))
}

fn checkin_status_item(account: &Value, mut status: Value) -> Value {
    status["accountId"] = account.get("id").cloned().unwrap_or(Value::Null);
    status["email"] = json!(account::account_display_name(account));
    status
}

async fn api_credits(Json(body): Json<Value>) -> Response {
    let id = body.get("accountId").and_then(|v| v.as_str()).unwrap_or("");
    let Some(acc) = account::find_account(id) else {
        return json_err("账号不存在".to_string(), StatusCode::BAD_REQUEST);
    };
    json_ok(credits::get_credit_expiry(&acc).await)
}

fn query_flag_enabled(query: Option<&str>, name: &str) -> bool {
    query.unwrap_or("").split('&').any(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, "true"));
        key == name && matches!(value, "" | "1" | "true" | "yes")
    })
}

async fn api_credit_statistics(RawQuery(query): RawQuery) -> Response {
    json_ok(credit_usage::get_statistics(query_flag_enabled(query.as_deref(), "refresh")).await)
}

async fn api_token_statistics(RawQuery(query): RawQuery) -> Response {
    let days = query.as_deref().and_then(|value| {
        value.split('&').find_map(|part| {
            part.strip_prefix("days=")?.parse::<i64>().ok()
        })
    });
    match tokio::task::spawn_blocking(move || token_stats::get_statistics(days)).await {
        Ok(statistics) => json_ok(statistics),
        Err(error) => json_err(
            format!("扫描 Token 统计失败: {error}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn api_checkin(Json(body): Json<Value>) -> Response {
    let id = body.get("accountId").and_then(|v| v.as_str()).unwrap_or("");
    let Some(acc) = account::find_account(id) else {
        return json_err("账号不存在".to_string(), StatusCode::BAD_REQUEST);
    };
    json_ok(checkin::checkin_account(&acc).await)
}

async fn api_checkin_all() -> Response {
    json_ok(checkin::run_checkin_all().await)
}

async fn api_checkin_config() -> Response {
    json_ok(config::load_checkin_config())
}

async fn api_save_checkin_config(Json(body): Json<Value>) -> Response {
    let submitted = body.get("config").unwrap_or(&body);
    match config::save_checkin_config(submitted) {
        Ok(()) => json_ok(config::load_checkin_config()),
        Err(e) => json_err(e.to_string(), StatusCode::BAD_REQUEST),
    }
}

async fn api_checkin_logs() -> Response {
    json_ok(json!({ "logs": config::load_checkin_logs() }))
}

async fn api_travel_status() -> Response {
    travel::reconcile_due_travel(None).await;
    let items = account::load_accounts()
        .iter()
        .map(|acc| {
            let id = acc.get("id").and_then(Value::as_str).unwrap_or("");
            let mut value = travel::travel_display(id);
            value["accountId"] = acc.get("id").cloned().unwrap_or(Value::Null);
            value["email"] = json!(account::account_display_name(acc));
            value
        })
        .collect::<Vec<_>>();
    json_ok(json!({ "accounts": items }))
}

async fn api_travel_config() -> Response {
    json_ok(config::load_travel_config())
}

async fn api_save_travel_config(Json(body): Json<Value>) -> Response {
    let submitted = body.get("config").unwrap_or(&body);
    match config::save_travel_config(submitted) {
        Ok(()) => {
            let saved = config::load_travel_config();
            if saved.get("enabled").and_then(Value::as_bool) == Some(true) {
                tokio::spawn(async {
                    let _ = travel::run_travel_cycle().await;
                    let _ = travel::run_travel_claim_cycle().await;
                });
            }
            json_ok(saved)
        }
        Err(e) => json_err(e.to_string(), StatusCode::BAD_REQUEST),
    }
}

async fn api_refresh_token(Json(body): Json<Value>) -> Response {
    let id = body.get("accountId").and_then(|v| v.as_str()).unwrap_or("");
    let Some(acc) = account::find_account(id) else {
        return json_err("账号不存在".to_string(), StatusCode::BAD_REQUEST);
    };
    // 与桌面端同一口径：手动刷新失败不落盘（见 commands.rs 的 refresh_account_token）。
    let fresh = refresh::refresh_token_only(acc).await;
    if fresh.get("needs_relogin").and_then(Value::as_bool) != Some(true) {
        let _ = account::upsert_account(&fresh);
    }
    json_ok(fresh)
}

// ---------------------------------------------------------------------------
// API 反代
//
// 🔴 这段是「容器 / CLI 形态能不能用」的关键。以前 webui **完全没有**反代相关路由，
// 于是浏览器里打开反代页只能看到「webui 模式暂不支持该操作」——
// 本机 webui 影响不大（可以用桌面端），但**容器部署没有桌面端**，
// 缺了这条就等于「常驻反代」这个核心用途根本配不起来。
//
// 返回值形状刻意对齐 `src-tauri/src/commands.rs` 里的同名命令，前端 ROUTES 直接复用。
// ---------------------------------------------------------------------------

/// 反代总状态（与桌面端 `get_proxy_status` 同形状）。
fn proxy_status_value() -> Value {
    let cfg = proxy::load_proxy_config();
    let endpoints: Vec<Value> = Edition::ALL
        .iter()
        .map(|&e| {
            let ep = cfg.endpoint(e);
            json!({
                "edition": e.key(),
                "label": e.label(),
                "enabled": ep.enabled,
                "listen": ep.listen,
                "hasApiKey": !ep.api_key.trim().is_empty(),
                // 该入口自己的账号数（0 = 该版本的全部账号都参与）
                "accountCount": ep.accounts.len(),
                "running": proxy::is_running(e),
            })
        })
        .collect();
    json!({
        "running": proxy::proxy_running(),
        "endpoints": endpoints,
        // 冷却快照：只列正在冷却的号（原因 + 绝对恢复时刻）。与桌面端同形状。
        "accounts": proxy::cooldown_snapshot(),
    })
}

async fn api_proxy_config() -> Response {
    match serde_json::to_value(proxy::load_proxy_config()) {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// 保存配置并立即生效。与桌面端一致：**逐版本**返回启动结果 ——
/// 某一版起不来（最常见是那一版没号）不该让整个保存失败，也不该连累另一版。
async fn api_proxy_save(Json(body): Json<Value>) -> Response {
    // 前端传的是 `{ config: {...} }`；也容忍直接把配置平铺在 body 里。
    let raw = body.get("config").cloned().unwrap_or(body);
    let cfg: proxy::ProxyConfig = match serde_json::from_value(raw) {
        Ok(c) => c,
        Err(e) => return json_err(format!("配置格式不正确：{e}"), StatusCode::BAD_REQUEST),
    };
    if let Err(e) = proxy::save_proxy_config(&cfg) {
        return json_err(e, StatusCode::BAD_REQUEST);
    }
    let outcomes: Vec<Value> = proxy::apply_config(&cfg)
        .iter()
        .map(|(edition, r)| {
            json!({
                "edition": edition.key(),
                "label": edition.label(),
                "ok": r.is_ok(),
                "error": r.as_ref().err(),
            })
        })
        .collect();
    json_ok(json!({
        "ok": true,
        "running": proxy::proxy_running(),
        "endpoints": outcomes,
    }))
}

async fn api_proxy_status() -> Response {
    json_ok(proxy_status_value())
}

/// 从原始 query 里取一个参数值（不做百分号转义 —— 这里的取值都是 edition 这种短标识）。
fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query.unwrap_or("").split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

/// 取某一版的用量。不传 `edition` 时按国内版（与桌面端 `get_proxy_usage` 同口径）。
async fn api_proxy_usage(RawQuery(query): RawQuery) -> Response {
    let edition = query_param(query.as_deref(), "edition")
        .as_deref()
        .and_then(proxy::parse_edition_key)
        .unwrap_or(Edition::Domestic);
    json_ok(proxy::load_usage_for(edition))
}

/// 清空用量。传了 `edition` 只清那一版；不传则两版都清。
async fn api_proxy_usage_reset(Json(body): Json<Value>) -> Response {
    let raw = body
        .get("edition")
        .and_then(Value::as_str)
        .map(str::to_string);
    let target = match raw.as_deref() {
        Some(v) if !v.trim().is_empty() => match proxy::parse_edition_key(v) {
            Some(e) => Some(e),
            None => return json_err(format!("未知版本：{v}"), StatusCode::BAD_REQUEST),
        },
        _ => None,
    };
    match proxy::reset_usage(target) {
        Ok(()) => json_ok(json!({ "ok": true })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

/// 拉可用模型列表（要打上游，可能慢）。
async fn api_proxy_models(RawQuery(query): RawQuery) -> Response {
    let edition = query_param(query.as_deref(), "edition");
    match proxy::fetch_models(edition).await {
        Ok(v) => json_ok(v),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

/// 按当前配置把两个入口都重启一遍。
async fn api_proxy_restart() -> Response {
    match proxy::restart_proxy_server() {
        Ok(()) => json_ok(json!({ "ok": true, "running": proxy::proxy_running() })),
        Err(e) => json_err(e, StatusCode::BAD_REQUEST),
    }
}

// ---------------------------------------------------------------------------
// 自动轮换（CodeBuddy CLI）
// ---------------------------------------------------------------------------

async fn api_rotate_config() -> Response {
    json_ok(config::load_auto_rotate_config())
}

async fn api_save_rotate_config(Json(body): Json<Value>) -> Response {
    match config::save_auto_rotate_config(&body) {
        Ok(()) => json_ok(json!({ "ok": true, "config": config::load_auto_rotate_config() })),
        Err(e) => json_err(e.to_string(), StatusCode::BAD_REQUEST),
    }
}

async fn api_rotate_status() -> Response {
    json_ok(rotate::rotate_status())
}

async fn api_rotate_run() -> Response {
    json_ok(rotate::run_rotate_cycle().await)
}

async fn api_rotate_logs() -> Response {
    json_ok(json!({ "logs": rotate::rotate_logs() }))
}

// ---------------------------------------------------------------------------
// 更新
// ---------------------------------------------------------------------------

async fn api_update_check() -> Response {
    json_ok(update::update_check(None, false).await)
}

async fn api_update_config() -> Response {
    json_ok(update::load_github_config())
}

async fn api_save_update_config(Json(body): Json<Value>) -> Response {
    match update::save_github_config(&body) {
        Ok(()) => json_ok(json!({ "ok": true, "config": update::load_github_config() })),
        Err(e) => json_err(e.to_string(), StatusCode::BAD_REQUEST),
    }
}

// ---------------------------------------------------------------------------
// 更新防护
//
// WorkBuddy 的更新缓存按用户共用、不分产品，A 版下载的包会被 B 版套用并把对方
// 程序目录覆盖掉。这里把更新源指到黑洞 + 隔离已下载的包。
// 要读写注册表、扫安装目录，一律 spawn_blocking。
// ---------------------------------------------------------------------------

async fn api_update_guard_status() -> Response {
    match tokio::task::spawn_blocking(update_guard::status).await {
        Ok(s) => json_ok(
            serde_json::to_value(s).unwrap_or_else(|e| json!({ "error": e.to_string() })),
        ),
        Err(e) => json_err(
            format!("读取更新防护状态异常：{e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn api_update_guard_set(Json(body): Json<Value>) -> Response {
    let disabled = body.get("disabled").and_then(|v| v.as_bool()).unwrap_or(false);
    let clear = body
        .get("clearCache")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match tokio::task::spawn_blocking(move || update_guard::set_disabled(disabled, clear)).await {
        Ok(Ok(v)) => json_ok(v),
        Ok(Err(e)) => json_err(e, StatusCode::BAD_REQUEST),
        Err(e) => json_err(
            format!("设置更新防护异常：{e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn api_update_guard_clear_cache() -> Response {
    match tokio::task::spawn_blocking(update_guard::clear_cache).await {
        Ok(Ok(v)) => json_ok(v),
        Ok(Err(e)) => json_err(e, StatusCode::BAD_REQUEST),
        Err(e) => json_err(
            format!("清理更新缓存异常：{e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn api_update_guard_set_frozen(Json(body): Json<Value>) -> Response {
    let freeze = body
        .get("freeze")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match tokio::task::spawn_blocking(move || update_guard::set_cache_frozen(freeze)).await {
        Ok(Ok(v)) => json_ok(v),
        Ok(Err(e)) => json_err(e, StatusCode::BAD_REQUEST),
        Err(e) => json_err(
            format!("冻结暂存目录异常：{e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

// ---------------------------------------------------------------------------
// 静态前端
// ---------------------------------------------------------------------------

fn content_type(path: &str) -> &'static str {
    if path.ends_with(".js") || path.ends_with(".mjs") {
        "text/javascript"
    } else if path.ends_with(".css") {
        "text/css"
    } else if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else if path.ends_with(".woff2") {
        "font/woff2"
    } else {
        "application/octet-stream"
    }
}

async fn static_handler(uri: Uri) -> Response {
    // 前端路由回退到 index.html —— 返回的**是 index.html 的内容**，
    // 所以 MIME 必须按实际返回的文件判定，不能按请求路径。
    // 以前这里用请求路径判定：`/api-proxy` 这类深链接回退后带的是
    // `application/octet-stream`，浏览器会直接下载而不是渲染页面。
    let path = uri.path().trim_start_matches('/');
    // 只认「看起来像静态资源」的路径（带扩展名）。其余一律回退 index.html：
    // 反正 Assets 里没有它们，免得带 `.` 的怪路径（如 `/.git/config`）被读成静态文件。
    let looks_static = path.rsplit('/').next().map(|f| f.contains('.')).unwrap_or(false);
    let (asset, name) = match Assets::get(path).filter(|_| looks_static) {
        Some(f) => (Some(f), path),
        None => (Assets::get("index.html"), "index.html"),
    };
    match asset {
        Some(f) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type(name))
            .body(Body::from(f.data.into_owned()))
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::checkin_status_item;
    use serde_json::json;

    #[test]
    fn web_checkin_status_keeps_account_identity() {
        let item = checkin_status_item(
            &json!({"id": "account-1", "email": "user@example.com"}),
            json!({"ok": true, "todayCheckedIn": true}),
        );

        assert_eq!(item["accountId"], "account-1");
        assert_eq!(item["email"], "user@example.com");
        assert_eq!(item["todayCheckedIn"], true);
    }

    #[test]
    fn web_checkin_status_preserves_failure_state() {
        let item = checkin_status_item(
            &json!({"id": "account-2"}),
            json!({"ok": false, "todayCheckedIn": false, "error": "status failed"}),
        );

        assert_eq!(item["accountId"], "account-2");
        assert_eq!(item["ok"], false);
        assert_eq!(item["error"], "status failed");
    }
}
