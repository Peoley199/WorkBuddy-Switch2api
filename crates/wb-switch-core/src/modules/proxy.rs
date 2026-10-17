//! OpenAI 兼容的 API 反代（把 Switch 管理的账号包装成 `/v1/chat/completions`）。
//!
//! 设计要点：
//! - 与 Switch 共用同一份账号库（`accounts.json`）与同一套刷新逻辑，
//!   所以不存在「两个进程各自刷新同一个 refresh token」的问题；
//! - 出站强制 `stream: true`（与上游一致），客户端要非流式时在本地聚合；
//! - 账号选择：健康账号轮转，失败的号冷却一段时间后自动回到池子。

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use chrono::{FixedOffset, Local, NaiveDateTime, TimeZone};

use super::account::{load_accounts, upsert_account};
use super::config::{home_dir, now_ms};
use super::edition::{edition_of, Edition};
use super::refresh::refresh_account_token;

const CONFIG_FILE: &str = "proxy.json";
const DEFAULT_CLIENT_VERSION: &str = "5.5.4";
const DEFAULT_CLI_VERSION: &str = "2.137.1";
/// token 剩余有效期小于该值时先刷新再出站。
const REFRESH_MARGIN_MS: i64 = 2 * 60 * 60 * 1000;
/// 单账号失败后的冷却时间。
const COOLDOWN: Duration = Duration::from_secs(120);

/// 上游「渠道校验」软拦截（`11128`）的最大重试次数。
///
/// 🔴 这个码**不是请求内容的问题，也不是账号的问题**：同一个 payload、同一个号、
/// 同一套出站头，实测连发 12 次全部通过（假 model）／连发 6 次全部 200（真 model）。
/// 上游是**间歇**拦的，而且它自己的文案就在邀请重发
/// （`displayMsg.zh = "请求被拦截，请重新发送"`）。
/// 所以遇到它**先重试**，重试仍失败才当账号故障处理。
const MAX_CHANNEL_RETRY: usize = 2;

/// 上游**连接层**失败（DNS 解析不了 / 超时 / 连接被拒）的最大重试次数。
///
/// 🔴 连接层失败**不是账号的问题**，绝不能拿它去冷却账号。
/// 以前这里无条件 `mark_failed`，于是网络一抖就把整池冻 120 秒：
/// 国际版只有 3 个号，几下就冻空 ⇒ 客户端收到
/// `503 国际版的账号当前都不可用（冷却中，或需要重新登录）`，过两分钟又好了，
/// 然后网络再抖 → 再冻 —— 表现为「一会儿报错停下来、一会儿又能用」反复横跳。
/// （2026-10-11 与 `refresh.rs` 里 `code=-1` 那个坑同源。）
const MAX_TRANSPORT_RETRY: usize = 1;

/// 入站「思考强度」相关字段名，原样透传给上游。
///
/// 🔴 上游真正认的是 `reasoning_effort`（实测：加它之后 `deepseek-v4-pro` /
/// `deepseek-v4.1-flash` 立即开始返回 `reasoning_content` 与非零 `reasoning_tokens`；
/// 而 `thinking`（含 `budget_tokens`）与 `enable_thinking` 上游一律忽略）。
/// 其余几个是 pi-ai 按 `compat.thinkingFormat` 可能发出的其它拼写，一并透传，
/// 免得换个客户端又踩一遍「参数被吃掉」。
const REASONING_PASSTHROUGH: &[&str] = &[
    "reasoning_effort",
    "thinking",
    "reasoning",
    "enable_thinking",
    "chat_template_kwargs",
];
/// 单次请求体上限。
///
/// axum 默认只给 2 MB（`DEFAULT_LIMIT`），长上下文会话（几十万 token）序列化后
/// 很容易超过，会被直接 413 掉、且报错信息里看不到原因。这里显式放宽到 64 MB：
/// 足以容纳百万级 token 的上下文，同时仍留一个防止内存被打爆的上界。
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------- 配置

/// 一个版本的接入点：**监听地址、鉴权 key、开关三者各自独立**。
///
/// 为什么必须分开：国内版（`.workbuddy` / `www.codebuddy.cn`）与国际版
/// （`.workbuddy-ai` / `www.workbuddy.ai`）的**凭证与模型列表完全不互通** ——
/// 上游会校验 token 的 issuer，拿国内版的号去打国际版域名必然被拒。
/// 两边各给一个入口之后：
/// - 客户端按**端口**区分连哪一版，不再依赖「用模型名猜版本」；
/// - 两边各用一把 key，便于分别授权 / 撤销；
/// - `GET /v1/models` 只列本版本的模型，不会被另一版的模型误导。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    #[serde(default)]
    pub enabled: bool,
    /// `host:port`。留空时由 [`ProxyConfig::resolved`] 补成该版本的默认端口。
    #[serde(default)]
    pub listen: String,
    /// 留空 = 不鉴权（仅在 `127.0.0.1` 这类本机监听时才建议留空）。
    #[serde(default)]
    pub api_key: String,
    /// 参与**这个入口**反代的 uid；留空 = 该版本的全部账号。
    ///
    /// 两个入口各存一份：国内版用哪几个号和国际版用哪几个号本来就是两个独立决定，
    /// 共用一个名单只会让人在两处改同一件事。
    #[serde(default)]
    pub accounts: Vec<String>,
    /// **额外放行**的模型：上游列表里没有、但本入口实测可用的。
    ///
    /// 🔴 为什么必须有它：上游的 `/v2/enterprises/personal/models` 是**给客户端 UI 看的展示列表**，
    /// 里面混着 `default-model` / `fast-model` / `primary-model` / `deep-model` 这类「模式名」，
    /// **不等于可用模型全集**。实测（2026-10-11）：`deepseek-v4.1-flash` 在国际版上游
    /// `/v2/chat/completions` 上是 200 成功的，却**完全不在国际版的列表里**
    /// （那 29 KB JSON 里 `"deepseek"` 出现 0 次）；反过来列表里的 `deep-model` 直发反而 11102。
    ///
    /// 版本防火墙是按列表判定归属的，于是这类「能用但没列出」的模型会被误判成另一版专属、
    /// 直接 400 掉。填进这里的模型：
    /// ① 会并入本入口 `GET /v1/models` 的返回；
    /// ② **跳过版本防火墙**（用户显式声明优先于自动推断）。
    #[serde(default)]
    pub extra_models: Vec<String>,
}

impl Default for Endpoint {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: String::new(),
            api_key: String::new(),
            accounts: Vec::new(),
            extra_models: Vec::new(),
        }
    }
}

/// 国内版入口的默认监听地址（沿用 v1 的默认值，老用户升级后地址不变）。
pub const DEFAULT_LISTEN_DOMESTIC: &str = "127.0.0.1:7863";
/// 国际版入口的默认监听地址。
pub const DEFAULT_LISTEN_INTERNATIONAL: &str = "127.0.0.1:7864";

/// 反代配置：**两个入口彼此独立** —— 各有开关、监听地址、API Key、账号名单。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// 国内版入口。
    #[serde(default)]
    pub domestic: Endpoint,
    /// 国际版入口。
    #[serde(default)]
    pub international: Endpoint,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            domestic: Endpoint::default(),
            international: Endpoint::default(),
        }
    }
}

impl ProxyConfig {
    /// 取某个版本的接入点。
    pub fn endpoint(&self, e: Edition) -> &Endpoint {
        match e {
            Edition::Domestic => &self.domestic,
            Edition::International => &self.international,
        }
    }

    /// 同上，可变引用。
    pub fn endpoint_mut(&mut self, e: Edition) -> &mut Endpoint {
        match e {
            Edition::Domestic => &mut self.domestic,
            Edition::International => &mut self.international,
        }
    }

    /// 把空着的监听地址补成该版本的默认端口。**读配置后必调** ——
    /// 配置文件里写 `"domestic": {"enabled": true}` 也是合法的，端口取默认值。
    pub fn resolved(mut self) -> Self {
        for e in Edition::ALL {
            let d = default_listen_of(e);
            let ep = self.endpoint_mut(e);
            if ep.listen.trim().is_empty() {
                ep.listen = d.to_string();
            }
        }
        self
    }

    /// 有没有任何一个入口被启用。
    pub fn any_enabled(&self) -> bool {
        Edition::ALL.iter().any(|&e| self.endpoint(e).enabled)
    }
}

/// 某版本的默认监听地址。
pub fn default_listen_of(e: Edition) -> &'static str {
    match e {
        Edition::Domestic => DEFAULT_LISTEN_DOMESTIC,
        Edition::International => DEFAULT_LISTEN_INTERNATIONAL,
    }
}

/// v1 时代的**扁平**配置：`{ enabled, listen, api_key, accounts }`。
///
/// 只用于读 —— 读到就迁移并立刻落盘（见 [`load_proxy_config`]）。
#[derive(Deserialize)]
struct LegacyConfig {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    accounts: Vec<String>,
}

/// v2 的过渡结构：已有两个入口，但账号名单还是**两边共用**的一份。
#[derive(Deserialize, Default)]
struct V2Endpoint {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    listen: String,
    #[serde(default)]
    api_key: String,
}

#[derive(Deserialize, Default)]
struct V2Config {
    #[serde(default)]
    domestic: V2Endpoint,
    #[serde(default)]
    international: V2Endpoint,
    #[serde(default)]
    accounts: Vec<String>,
}

impl ProxyConfig {
    /// v1 扁平配置 → 现行结构。
    ///
    /// 旧的 `listen` / `api_key` / `enabled` / `accounts` 归到**国内版**（那是 v1
    /// 时代唯一存在的一档），国际版保持默认关闭 —— 不擅自替用户开启一个以前
    /// 不存在的监听端口。
    fn from_legacy(old: LegacyConfig) -> Self {
        ProxyConfig {
            domestic: Endpoint {
                enabled: old.enabled.unwrap_or(false),
                listen: old.listen.unwrap_or_default(),
                api_key: old.api_key.unwrap_or_default(),
                accounts: old.accounts,
                ..Endpoint::default()
            },
            international: Endpoint::default(),
        }
        .resolved()
    }

    /// v2（两边共用一份名单）→ 现行结构（各持一份）。
    ///
    /// 把那份名单**同时**发给两个入口即可 —— 行为完全等价，因为每个入口本来就
    /// 只能用到属于自己版本的号，不会因为多拿到另一版的 uid 而越界。
    fn from_v2(old: V2Config) -> Self {
        ProxyConfig {
            domestic: Endpoint {
                enabled: old.domestic.enabled,
                listen: old.domestic.listen,
                api_key: old.domestic.api_key,
                accounts: old.accounts.clone(),
                ..Endpoint::default()
            },
            international: Endpoint {
                enabled: old.international.enabled,
                listen: old.international.listen,
                api_key: old.international.api_key,
                accounts: old.accounts,
                ..Endpoint::default()
            },
        }
        .resolved()
    }
}

fn config_path() -> std::path::PathBuf {
    home_dir().join(".wb-switch").join(CONFIG_FILE)
}

/// 读配置。**能识别并自动迁移 v1 的扁平格式**。
pub fn load_proxy_config() -> ProxyConfig {
    let path = config_path();
    let Ok(text) = std::fs::read_to_string(&path) else {
        return ProxyConfig::default().resolved();
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("[反代] {path:?} 解析失败({err})，使用默认配置");
            return ProxyConfig::default().resolved();
        }
    };

    let has_endpoints = value.get("domestic").is_some() || value.get("international").is_some();

    // 现行结构：两个入口，各自的 `accounts` 在入口内部（顶层没有 `accounts`）。
    if has_endpoints && value.get("accounts").is_none() {
        return match serde_json::from_value::<ProxyConfig>(value) {
            Ok(cfg) => cfg.resolved(),
            Err(err) => {
                eprintln!("[反代] {path:?} 结构不对({err})，使用默认配置");
                ProxyConfig::default().resolved()
            }
        };
    }

    // 需要迁移：v2（两个入口 + 顶层共用名单）或 v1（完全扁平）。
    let migrated = if has_endpoints {
        serde_json::from_value::<V2Config>(value)
            .map(ProxyConfig::from_v2)
            .map_err(|e| e.to_string())
    } else {
        serde_json::from_value::<LegacyConfig>(value)
            .map(ProxyConfig::from_legacy)
            .map_err(|e| e.to_string())
    };

    match migrated {
        Ok(cfg) => {
            eprintln!("[反代] 检测到旧版配置，已迁移为「国内版 / 国际版」两个独立入口");
            if let Err(e) = save_proxy_config(&cfg) {
                eprintln!("[反代] 迁移结果写回失败：{e}");
            }
            cfg
        }
        Err(err) => {
            eprintln!("[反代] {path:?} 无法识别({err})，使用默认配置");
            ProxyConfig::default().resolved()
        }
    }
}

pub fn save_proxy_config(cfg: &ProxyConfig) -> Result<(), String> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string_pretty(cfg).map_err(|e| e.to_string())?;
    std::fs::write(&path, text).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------- 账号池

struct AccountLease {
    uid: String,
    /// 这个号属于哪一档。国内版和国际版的模型列表不同，
    /// 拿国际版专属的 model 去打国内版网关会被拒，所以选号必须认版本。
    edition: Edition,
    failed_at: Option<Instant>,
    /// **按模型**的长冷却：`model -> 恢复时刻`。
    ///
    /// 60004 只代表「这个模型的额度用完了」，同一账号换模型仍可用（上游文案
    /// 自己就写着「您也可以切换其他模型继续使用」）。账号级长冷却会把还能
    /// 用的模型一起冻掉，所以这类失败记在模型键上；账号仍留在池子里服务
    /// 其他模型。
    model_cooldowns: HashMap<String, Instant>,
}

struct Pool {
    leases: Vec<std::sync::Mutex<AccountLease>>,
    cursor: AtomicUsize,
}

impl Pool {
    fn new(entries: Vec<(String, Edition)>) -> Self {
        Self {
            leases: entries
                .into_iter()
                .map(|(uid, edition)| {
                    std::sync::Mutex::new(AccountLease {
                        uid,
                        edition,
                        failed_at: None,
                        model_cooldowns: HashMap::new(),
                    })
                })
                .collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    /// 同上，但可以只在某一档里取号（`want = None` 表示不限版本）。
    ///
    /// `model` 给出时，还会跳过「该模型正在长冷却」的号 —— 60004 只冻那个模型，
    /// 同一账号的其他模型仍可服务。
    ///
    /// 一个版本都没有可用号时**不会**悄悄换另一个版本 —— 那样只会把
    /// 「模型不存在」的 400 换个地方报出来，不如直接说清楚。
    fn next_uid_of(&self, want: Option<Edition>, model: Option<&str>) -> Option<String> {
        if self.leases.is_empty() {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for i in 0..self.leases.len() {
            let idx = (start + i) % self.leases.len();
            let Ok(lease) = self.leases[idx].lock() else {
                continue;
            };
            if let Some(e) = want {
                if lease.edition != e {
                    continue;
                }
            }
            let cooling = lease
                .failed_at
                .map(|t| t.elapsed() < COOLDOWN)
                .unwrap_or(false);
            if cooling {
                continue;
            }
            if let Some(m) = model {
                let model_cooling = lease
                    .model_cooldowns
                    .get(m)
                    .map(|t| *t > Instant::now())
                    .unwrap_or(false);
                if model_cooling {
                    continue;
                }
            }
            return Some(lease.uid.clone());
        }
        None
    }

    /// 某一档里还有没有号（用于给出更准确的报错）。
    fn has_edition(&self, edition: Edition) -> bool {
        self.leases.iter().any(|slot| {
            slot.lock()
                .map(|l| l.edition == edition)
                .unwrap_or(false)
        })
    }

    fn mark_failed(&self, uid: &str) {
        for slot in &self.leases {
            let Ok(mut lease) = slot.lock() else { continue };
            if lease.uid == uid {
                lease.failed_at = Some(Instant::now());
            }
        }
    }

    /// 只冻「某账号的某个模型」到指定时刻 —— 账号的其他模型继续服务。
    ///
    /// 固定的 120 秒短冷却对付频率超限不够：120 秒后号回池立刻又 429，循环空转。
    /// `until_ms` 是上游声明的重置时刻（毫秒）；已过期则视为不冷却。
    fn mark_model_failed_until(&self, uid: &str, model: &str, until_ms: i64) {
        let now = now_ms_i64();
        if until_ms <= now {
            return;
        }
        let until = Instant::now() + Duration::from_millis((until_ms - now) as u64);
        for slot in &self.leases {
            let Ok(mut lease) = slot.lock() else { continue };
            if lease.uid == uid {
                lease.model_cooldowns.insert(model.to_string(), until);
            }
        }
    }

    /// 该账号剩余冷却毫秒数：短冷却与**所有模型**长冷却里最晚的那个。
    /// 不在冷却中返回 None。
    fn cooling_for(&self, uid: &str) -> Option<i64> {
        let now = Instant::now();
        for slot in &self.leases {
            let Ok(lease) = slot.lock() else { continue };
            if lease.uid != uid {
                continue;
            }
            let short = lease
                .failed_at
                .map(|t| COOLDOWN.as_millis() as i64 - t.elapsed().as_millis() as i64)
                .filter(|m| *m > 0);
            let long = lease
                .model_cooldowns
                .values()
                .map(|t| t.saturating_duration_since(now).as_millis() as i64)
                .filter(|m| *m > 0)
                .max();
            return long.max(short);
        }
        None
    }
}

fn now_ms_i64() -> i64 {
    now_ms() as i64
}

/// 取一个可用的账号（必要时先刷新 token）。
///
/// `model` 用于跳过「该模型正在长冷却」的号 —— 60004 只冻模型，不冻账号。
async fn take_account(
    pool: &Pool,
    _cfg: &ProxyConfig,
    want: Option<Edition>,
    model: Option<&str>,
) -> Option<Value> {
    let uid = pool.next_uid_of(want, model)?;
    let mut acc = load_accounts().into_iter().find(|a| {
        a.get("uid").and_then(Value::as_str) == Some(uid.as_str())
            && a.get("needs_relogin").and_then(Value::as_bool) != Some(true)
    })?;

    let exp = acc.get("expiresAt").and_then(Value::as_i64).unwrap_or(0);
    if exp - now_ms_i64() < REFRESH_MARGIN_MS {
        match tokio::time::timeout(
            Duration::from_secs(30),
            refresh_account_token(acc.clone()),
        )
        .await
        {
            Ok(refreshed) => {
                if refreshed.get("needs_relogin").and_then(Value::as_bool) == Some(true) {
                    pool.mark_failed(&uid);
                    return None;
                }
                acc = refreshed;
            }
            Err(_) => {
                eprintln!("[反代] 刷新超时 uid={uid}");
                pool.mark_failed(&uid);
                return None;
            }
        }
    }
    let _ = upsert_account(&acc);
    Some(acc)
}

// ---------------------------------------------------------------- 上游

fn chat_url(acc: &Value) -> String {
    format!("{}/v2/chat/completions", upstream_endpoint(acc))
}

fn models_url(acc: &Value) -> String {
    format!("{}/v2/enterprises/personal/models", upstream_endpoint(acc))
}

/// 位掩码：某个模型出现在**哪几档**的模型列表里。
const MASK_DOMESTIC: u8 = 1 << 0;
const MASK_INTERNATIONAL: u8 = 1 << 1;

fn edition_mask(e: Edition) -> u8 {
    match e {
        Edition::Domestic => MASK_DOMESTIC,
        Edition::International => MASK_INTERNATIONAL,
    }
}

/// 模型 id → **出现在哪几档的列表里**（位掩码），由「拉模型列表」这条路径**累加**写入。
///
/// 反代池里国内版和国际版的号是混着的，而两版的模型列表不一样：拿只属于另一版的 model
/// 去打本版网关会被上游拒（`11102 only available for authorized users`）。所以请求进来先按
/// model 查这张表，**确实只属于另一版**时才拦。
///
/// 🔴 **必须是累加，不能是覆盖**：两版列表有大量重叠（`glm-5.3` / `hy3` / `lite` …），
/// 覆盖会让重叠模型被记成「后拉的那一档」，于是**另一档的入口连自己列表里的模型都 400 掉**。
/// 实测踩过：国际版列表后拉之后，`7863` 发 `glm-5.3` 直接报「属于国际版」。
fn model_routes() -> &'static Mutex<HashMap<String, u8>> {
    static ROUTES: std::sync::OnceLock<Mutex<HashMap<String, u8>>> = std::sync::OnceLock::new();
    ROUTES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 记下「这些模型**也**出现在这一档的列表里」。拉列表成功时调用（取并集，不覆盖）。
fn remember_model_routes(edition: Edition, ids: &[String]) {
    if let Ok(mut m) = model_routes().lock() {
        let bit = edition_mask(edition);
        for id in ids {
            *m.entry(id.clone()).or_insert(0) |= bit;
        }
    }
}

/// 某个 model **只**属于哪一档。
///
/// 只在「恰好出现在一档的列表里」时返回 `Some`；**两档都有、或谁都没见过 → `None`** ——
/// 这两种情况都不该拦，交给上游判定。（`deepseek-v4.1-flash` 这类「上游能用但没列出」的
/// 模型，可用 [`Endpoint::extra_models`] 显式放行。）
pub fn model_edition(model: &str) -> Option<Edition> {
    let mask = model_routes()
        .lock()
        .ok()
        .and_then(|m| m.get(model).copied())
        .unwrap_or(0);
    match mask {
        MASK_DOMESTIC => Some(Edition::Domestic),
        MASK_INTERNATIONAL => Some(Edition::International),
        _ => None,
    }
}

/// 解析版本标识（前端传 `domestic` / `international`）。
pub fn parse_edition_key(s: &str) -> Option<Edition> {
    match s.trim().to_ascii_lowercase().as_str() {
        "domestic" | "cn" | "国内版" => Some(Edition::Domestic),
        "international" | "intl" | "ai" | "国际版" => Some(Edition::International),
        _ => None,
    }
}

/// `/v1/models` 里 `owned_by` 的取值：让客户端一眼看出这个模型是哪一档的。
fn owner_tag(edition: Edition) -> &'static str {
    match edition {
        Edition::Domestic => "workbuddy-cn",
        Edition::International => "workbuddy-intl",
    }
}

/// 该账号所在档位的上游基址。
///
/// **国际版必须用 `https://www.workbuddy.ai`**——把国际版 token 发到
/// `codebuddy.cn` 会被网关拒掉。对照上游 `WbVariant::api_endpoint`。
fn upstream_endpoint(acc: &Value) -> &'static str {
    edition_of(acc).api_endpoint()
}

fn hex_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:032x}", nanos ^ 0x5eed_5eed_5eed_5eedu128)
}

/// 组装出站头族（对齐官方桌面端，缺字段用 X-No-* 约定）。
fn chat_headers(acc: &Value) -> HashMap<String, String> {
    let mut h = HashMap::new();
    let uid = acc.get("uid").and_then(Value::as_str).unwrap_or("");
    let at = acc.get("access_token").and_then(Value::as_str).unwrap_or("");
    let domain = acc.get("domain").and_then(Value::as_str).unwrap_or("");

    h.insert("Content-Type".into(), "application/json".into());
    h.insert(
        "Accept".into(),
        "application/json, text/event-stream".into(),
    );
    h.insert("X-Requested-With".into(), "XMLHttpRequest".into());
    let endpoint = upstream_endpoint(acc);
    h.insert("Origin".into(), endpoint.into());
    h.insert("Referer".into(), format!("{endpoint}/"));
    h.insert(
        "User-Agent".into(),
        format!(
            "WorkBuddy/{DEFAULT_CLIENT_VERSION} WorkBuddy/{DEFAULT_CLIENT_VERSION} CLI/{DEFAULT_CLI_VERSION}"
        ),
    );
    h.insert("X-CodeBuddy-Request".into(), "1".into());
    h.insert("Accept-Language".into(), "zh-CN".into());

    if !at.is_empty() {
        h.insert("Authorization".into(), format!("Bearer {at}"));
    } else {
        h.insert("X-No-Authorization".into(), "1".into());
    }
    if !uid.is_empty() {
        h.insert("X-User-Id".into(), uid.into());
    } else {
        h.insert("X-No-User-Id".into(), "1".into());
    }
    if !domain.is_empty() {
        h.insert("X-Domain".into(), domain.into());
    } else {
        h.insert("X-No-Department-Info".into(), "1".into());
    }
    match acc.get("enterpriseId").and_then(Value::as_str) {
        Some(v) if !v.is_empty() => {
            h.insert("X-Enterprise-Id".into(), v.into());
        }
        _ => {
            h.insert("X-No-Enterprise-Id".into(), "1".into());
        }
    }
    // 用量归属：伪装成桌面端会话，避免上游统计里 client 为空。
    h.insert("X-Agent-Purpose".into(), "conversation".into());
    h.insert("X-IDE-Name".into(), "WorkBuddy".into());
    h.insert("X-IDE-Type".into(), "desktop".into());
    h.insert("X-Product".into(), "WorkBuddy".into());

    // 会话头族（一次对话轮复用同一个聚合主键）。
    let conv_req = hex_id();
    let msg = hex_id();
    h.insert("X-Conversation-Request-ID".into(), conv_req.clone());
    h.insert("X-Conversation-Message-ID".into(), msg.clone());
    h.insert("X-Request-ID".into(), msg.clone());
    h.insert("X-Root-Request-ID".into(), conv_req.clone());
    h.insert("X-Trace-ID".into(), conv_req.clone());
    h.insert("X-B3-TraceId".into(), conv_req);
    h.insert("X-B3-SpanId".into(), msg[..16].to_string());
    h.insert("X-B3-Sampled".into(), "1".into());
    h
}

/// 上游网关只认少数几种内容块类型。客户端（zcode / Cursor / 各类 Agent）常发
/// OpenAI 较新的 `type:"file"`、Responses 的 `input_file`、Anthropic 的 `document`，
/// 原样透传会被上游直接 400，且报错看不出是哪来的：
/// `{"code":11101,"msg":"Parse message failed: unsupported content type at index 0: file"}`
///
/// 这里把它们降级成文本：能解出文本就内联，否则留一行占位说明。
/// 降级记录进 `degraded`，最后随响应头 `x-wb-switch-degraded` 回到客户端。
fn sanitize_messages(messages: &Value, degraded: &mut Vec<String>) -> Value {
    let Some(arr) = messages.as_array() else {
        return messages.clone();
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let mut m = item.clone();
        // 🔴 `role: "developer"` 必须先改写成 `system`。
        //
        // 上游两道网关（国内 `codebuddy.cn` / 国际 `workbuddy.ai`）都把 `developer`
        // 判成「非官方渠道」，整条请求被 **400 `11128 Illegal API invocation from an
        // unapproved channel`** 打回。实测单变量对照（两版各 3/3 稳定复现）：
        //
        // | messages | 结果 |
        // |---|---|
        // | `[system, developer(长正文), user]` | `11128` |
        // | `[system, system(同一正文), user]` | `200` |
        // | `[system, user(同一正文), user]` | `200` |
        //
        // DSH 这类客户端会把 system prompt 用 `developer` 角色发出去（OpenAI 自 o1 起
        // 引入的 system 别名，语义等价）。改写它**不丢任何内容**，只是换回上游认的角色。
        // ⚠️ 必须在下面「system 环境注入净化」**之前**做，这样开发者消息也能吃到净化。
        if m.get("role").and_then(Value::as_str) == Some("developer") {
            m["role"] = Value::String("system".to_string());
            degraded.push("developer-role".to_string());
        }
        // system 消息先过一遍「环境注入」净化，见 strip_env_injection 的说明。
        if m.get("role").and_then(Value::as_str) == Some("system") {
            if let Some(cleaned) = m
                .get("content")
                .and_then(Value::as_str)
                .and_then(strip_env_injection)
            {
                degraded.push("system-env".to_string());
                m["content"] = Value::String(cleaned);
            }
        }
        let Some(parts) = m.get("content").and_then(Value::as_array) else {
            // content 是字符串（最常见）或压根没有：原样放行。
            out.push(m);
            continue;
        };
        let mut kept: Vec<Value> = Vec::with_capacity(parts.len());
        for p in parts {
            match content_part(p, degraded) {
                Some(text) => kept.push(json!({ "type": "text", "text": text })),
                None => kept.push(p.clone()),
            }
        }
        if kept.is_empty() {
            kept.push(json!({ "type": "text", "text": "[内容块已省略：上游不支持该类型]" }));
        }
        m["content"] = Value::Array(kept);
        out.push(m);
    }
    Value::Array(out)
}

/// 摘掉 system prompt 末尾的**环境上下文注入块**。
///
/// 上游在 2026-10-09 起上线了一道**渠道校验**：它会识别 system prompt 里模仿
/// Claude Code 的环境注入（`gitStatus:` / `Current branch:` / `Main branch` /
/// `Git user:` / `Recent commits:` …），命中就把整条请求判成「非官方客户端」：
///
/// ```text
/// {"code":11128,"msg":"Illegal API invocation from an unapproved channel",
///  "displayMsg":{"zh":"请求被拦截，请重新发送"},
///  "displayTips":{"zh":"这次调用没通过渠道校验，可能不是官方客户端发出的。"}}
/// ```
///
/// 🔴 这**不是**出站请求头伪装不够——同一套头、同一个号，只去掉这段就能过。
/// 也**不是**单个关键词：把这段单独拎出来测是通过的，「长前缀 + 这段」的组合
/// 才必然命中，说明上游认的是累积出来的整体模式。
///
/// 实测（**假 model 名，零额度消耗**，靠返回码区分）：
///
/// | 请求 | 结果 |
/// |---|---|
/// | 前 10132 字 | `11102` 过了渠道校验，只是 model 不存在 |
/// | 前 10132 字 + `"x"*39`（同长度换内容） | `11102` 仍通过 |
/// | 前 10132 字 + 真实触发段 | `11128` 命中 |
/// | 去掉 `gitStatus:` 之后整块 | `11102` 恢复 |
/// | 把 system 改成 user | `11102` 恢复 |
///
/// 处理方式：从**最后一个** `gitStatus:` 起截断。ZCode 把这段环境快照追加在
/// system prompt 末尾，丢掉它只是让模型看不到 git 状态——而 ZCode 每轮都会重新
/// 注入，代价远小于整条请求被打回重试。
///
/// 返回 `None` = 没有可摘的块（原样透传，例如没注入过的短 system prompt）。
fn strip_env_injection(text: &str) -> Option<String> {
    let idx = text.rfind("gitStatus:")?;
    let cleaned = text[..idx].trim_end();
    // 摘完什么都不剩就别动，免得把 system prompt 整条清空。
    if cleaned.is_empty() {
        return None;
    }
    Some(cleaned.to_string())
}

/// 上游的失败响应体是不是「渠道校验软拦截」（`11128`）。
///
/// 只看响应体里有没有这个码 / 这句 msg —— 上游把它包在 `message` 字符串里再套一层
/// JSON，所以不能按结构化字段取，直接做子串匹配最稳。
///
/// 视为**可重试**的理由见 [`MAX_CHANNEL_RETRY`]：它是间歇性的，且文案本身在让客户端重发。
/// 判定成它之后**不要立刻冷却账号** —— 号是好的，冻它只会把整个账号池拖空。
fn is_channel_block(text: &str) -> bool {
    text.contains("11128") || text.contains("unapproved channel")
}

/// 解析上游「使用频率超限」（code 60004）响应，返回重置时刻（毫秒）。
///
/// 上游体是双层 JSON：外层 `{"message":"{\"code\":60004,\"msg\":\"…将在
/// 2026-10-17 11:46:56 UTC+8 重置…\"}"}`。直接在原文上抠关键字与时间串，
/// 避免两层反序列化；时间格式固定为 `YYYY-MM-DD HH:MM:SS UTC+8`。
/// 不是频率超限（或缺重置时间）返回 None，调用方走普通冷却。
fn rate_limit_reset_ms(text: &str) -> Option<i64> {
    if !(text.contains("60004") || text.contains("频率限制")) {
        return None;
    }
    let idx = text.find("频率限制")?;
    let tail = &text[idx..];
    let at = tail.find("将在")?;
    let tail = &tail[at + "将在".len()..];
    let raw: String = tail
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take(19)
        .collect();
    let naive = NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
    let offset = FixedOffset::east_opt(8 * 3600)?;
    Some(offset.from_local_datetime(&naive).single()?.timestamp_millis())
}

/// 调试落盘开关：`~/.wb-switch/DEBUG_DUMP` **存在**时，把发往上游的真实请求体
/// 与失败响应写到 `~/.wb-switch/dump/`。
///
/// 🔴 用「开关文件」而不是环境变量：这样**不必改启动方式**（照常双击即可，
/// 免得从终端启动时继承到 `HTTP_PROXY` 之类变量），排查完删掉开关文件就停。
/// 关掉时这里只做一次 `Path::exists()`，开销可忽略。
///
/// 用途：某些 11128 只在真实客户端（DSH 等）的完整报文下复现，
/// 而我们无法从会话存储里看到实际发出的字段 —— 只能把它拦下来看。
fn debug_dump_enabled() -> bool {
    home_dir().join(".wb-switch").join("DEBUG_DUMP").exists()
}

fn debug_dump(name: &str, payload: &str) {
    if !debug_dump_enabled() {
        return;
    }
    let dir = home_dir().join(".wb-switch").join("dump");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join(name), payload);
}

/// 单个内容块：`None` = 原样保留，`Some(text)` = 降级成文本块。
fn content_part(part: &Value, degraded: &mut Vec<String>) -> Option<String> {
    let ty = part.get("type").and_then(Value::as_str).unwrap_or("text");
    match ty {
        // 上游认识的原生块。
        "text" | "input_text" | "image_url" | "input_image" => None,
        // 附件类：能解出文本就内联，否则占位。
        "file" | "input_file" | "file_url" | "document" | "image_file" | "input_audio"
        | "audio_url" => {
            degraded.push(ty.to_string());
            Some(attachment_note(ty, part))
        }
        // 其余未知类型一律降级：宁可少一块内容，也不要整条请求被 400。
        _ => {
            degraded.push(ty.to_string());
            Some(format!("[{ty} 内容块已省略：上游不支持该类型]"))
        }
    }
}

/// 附件块的占位/内联文本。
fn attachment_note(ty: &str, part: &Value) -> String {
    let obj = part
        .get("file")
        .or_else(|| part.get("document"))
        .or_else(|| part.get("image_file"))
        .or_else(|| part.get("input_file"))
        .unwrap_or(part);
    let name = obj
        .get("filename")
        .or_else(|| obj.get("name"))
        .or_else(|| obj.get("file_id"))
        .and_then(Value::as_str)
        .unwrap_or("未命名附件");
    let data = obj
        .get("file_data")
        .or_else(|| obj.get("data"))
        .or_else(|| obj.get("base64"))
        .and_then(Value::as_str);
    match data.and_then(decode_text_data) {
        Some(text) => format!("[附件 {name}]\n{text}"),
        None => format!("[{ty} 附件 {name} 已省略：上游不支持该内容类型，仅文本类附件会内联]"),
    }
}

/// 只在能确定是文本时才解码：二进制（PDF/图片/音频）一律不解 ——
/// 把几十 KB 的 base64 灌进上下文比丢掉这块内容更糟。
fn decode_text_data(data: &str) -> Option<String> {
    use base64::Engine;
    /// 单个附件最多内联这么多字符。
    const MAX: usize = 64 * 1024;

    let Some(rest) = data.strip_prefix("data:") else {
        // 没有 data URL 前缀 = 客户端已经给了明文。
        return Some(clamp(data));
    };
    let (meta, b64) = rest.split_once(";base64,")?;
    let mime = meta.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    let textual = mime.starts_with("text/")
        || mime.contains("json")
        || mime.contains("xml")
        || mime.contains("javascript")
        || mime.contains("yaml")
        || mime.contains("csv")
        || mime.contains("markdown");
    if !textual {
        return None;
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .ok()?;
    if bytes.len() <= MAX {
        return Some(String::from_utf8_lossy(&bytes).to_string());
    }
    let mut t = String::from_utf8_lossy(&bytes[..MAX]).to_string();
    t.push_str("\n…（附件过长，已截断）");
    Some(t)
}

fn clamp(s: &str) -> String {
    const MAX: usize = 64 * 1024;
    if s.len() <= MAX {
        return s.to_string();
    }
    let mut t = s[..cut_at(s, MAX)].to_string();
    t.push_str("\n…（附件过长，已截断）");
    t
}

/// 按 UTF-8 边界往回退，避免切碎多字节字符。
fn cut_at(s: &str, max: usize) -> usize {
    let mut i = max.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// 降级记录的摘要，用作响应头（必须 ASCII）。
fn degraded_summary(degraded: &[String]) -> String {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for t in degraded {
        match counts.iter_mut().find(|(k, _)| k == t) {
            Some((_, n)) => *n += 1,
            None => counts.push((t.clone(), 1)),
        }
    }
    counts
        .iter()
        .map(|(k, n)| format!("{k}={n}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// 给响应挂上降级摘要头（没降级就不挂）。
fn with_degraded_header(mut resp: Response, degraded: &[String]) -> Response {
    if degraded.is_empty() {
        return resp;
    }
    if let Ok(v) = HeaderValue::from_str(&degraded_summary(degraded)) {
        resp.headers_mut().insert("x-wb-switch-degraded", v);
    }
    resp
}

/// 把 OpenAI 入站体改写成上游形态（强制流式、去掉上游不认识的字段）。
fn build_upstream_body(incoming: &Value, edition: Edition, degraded: &mut Vec<String>) -> Value {
    let model = incoming
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("deepseek-v4-flash")
        .to_string();
    let messages = incoming.get("messages").cloned().unwrap_or_else(|| json!([]));
    let mut sanitized = sanitize_messages(&messages, degraded);
    ensure_leading_system(edition, &mut sanitized, degraded);
    let mut body = json!({
        "model": model,
        "messages": sanitized,
        "stream": true,
    });
    if let Some(t) = incoming.get("temperature") {
        body["temperature"] = t.clone();
    }
    if let Some(t) = incoming.get("top_p") {
        body["top_p"] = t.clone();
    }
    if let Some(m) = incoming.get("max_tokens") {
        body["max_tokens"] = m.clone();
    }
    if let Some(t) = incoming.get("tools") {
        body["tools"] = t.clone();
    }
    // 🔴 思考强度：以前这里只转 model/messages/stream/temperature/top_p/max_tokens/tools，
    // 思考类字段**全被丢掉**，表现为「客户端调了推理等级、上游毫无反应、思考内容为空」。
    //
    // 各客户端拼写不同，反代不做翻译、原样透传（只有客户端真的发了才转，
    // 所以对不发这些字段的客户端零影响）：
    // - `reasoning_effort`（字符串）—— 上游真正认的就是它，实测见 API-PROXY.md
    // - `thinking`（对象或字符串，pi-ai 的 deepseek / string-thinking 格式都用它）
    // - `reasoning`（对象，openrouter / together / ant-ling 格式）
    // - `enable_thinking`（布尔，qwen 格式）
    // - `chat_template_kwargs`（对象，chat-template 格式）
    for key in REASONING_PASSTHROUGH {
        if let Some(v) = incoming.get(*key) {
            body[*key] = v.clone();
        }
    }
    body
}

/// 国际版上游要求**第一条消息必须是 `system`**，否则整条请求被拦。
///
/// 实测（2026-10-11，7864 → www.workbuddy.ai）：
/// - 首条是 `user` → `HTTP 400 {"code":11128,"msg":"first message is not system prompt"}`
/// - 同一个请求前面补一条 system → **200 正常返回**
///
/// 国内版没有这个要求（7863 上只发一条 user 是通的），所以只在国际版兜。
///
/// 客户端不一定带 system（探针、简单脚本、部分 OpenAI 兼容客户端都不带），
/// 兜一条默认的即可 —— 不带会被**整个**拦掉，比「多一句无害的 system」糟糕得多。
fn ensure_leading_system(edition: Edition, messages: &mut Value, degraded: &mut Vec<String>) {
    if edition != Edition::International {
        return;
    }
    let Some(arr) = messages.as_array_mut() else {
        return;
    };
    let already = arr
        .first()
        .and_then(|m| m.get("role"))
        .and_then(Value::as_str)
        .map(|r| r == "system")
        .unwrap_or(false);
    if already {
        return;
    }
    arr.insert(
        0,
        json!({"role": "system", "content": "You are a helpful assistant."}),
    );
    degraded.push("leading-system".to_string());
}

// ---------------------------------------------------------------- 处理

#[derive(Clone)]
struct AppState {
    /// 全局配置（主要是 `accounts` 白名单）。
    cfg: Arc<ProxyConfig>,
    /// 🔴 **两个入口共享同一个账号池** —— 否则同一个号会在两处各维护一份失败
    /// 冷却状态，出现「这边还在冷却、那边照用」的错乱。
    pool: Arc<Pool>,
    /// 🔴 本实例只服务这一个版本。
    edition: Edition,
    /// 本实例的接入点（监听地址 + **自己的** API Key）。
    endpoint: Arc<Endpoint>,
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, Json(json!({"error": {"message": "invalid api key"}}))).into_response()
}

/// 校验请求头里的 key。`api_key` 是**本入口自己的**那把，不是全局的。
fn check_auth(headers: &HeaderMap, api_key: &str) -> bool {
    let want = api_key.trim();
    if want.is_empty() {
        return true;
    }
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .trim_start_matches("Bearer ")
        .trim();
    presented == want
}

async fn healthz(State(st): State<AppState>) -> impl IntoResponse {
    let total = st.pool.leases.len();
    let healthy = load_accounts()
        .iter()
        .filter(|a| a.get("needs_relogin").and_then(Value::as_bool) != Some(true))
        .count();
    Json(json!({"ok": true, "service": "wb-switch-proxy", "healthy": healthy, "total": total}))
}

/// 去重后追加到模型 id 列表。
fn push_unique(ids: &mut Vec<String>, id: &str) {
    if !id.is_empty() && !ids.iter().any(|x| x == id) {
        ids.push(id.to_string());
    }
}

/// 上游模型列表的兜底（拉不到时给一组可用的，免得客户端因为空列表直接报错）。
/// 从上游响应里抠出模型 id。
/// 上游结构：`{ code, msg, data: { agents: [ { name, models: [...] } ] } }`，
/// 也可能直接是 `{ data: [...] }` 或裸数组，这里都兜住。
fn parse_model_ids_raw(v: &Value) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    if let Some(agents) = v.pointer("/data/agents").and_then(Value::as_array) {
        for a in agents {
            if let Some(models) = a.get("models").and_then(Value::as_array) {
                for m in models {
                    if let Some(s) = m.as_str() {
                        push_unique(&mut ids, s);
                    }
                }
            }
        }
    }
    if ids.is_empty() {
        let items = match v.get("data").cloned() {
            Some(Value::Array(arr)) => arr,
            _ => v.as_array().cloned().unwrap_or_default(),
        };
        for m in items {
            let id = m
                .get("id")
                .or_else(|| m.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            push_unique(&mut ids, id);
        }
    }
    ids
}

fn cut_text(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

/// `GET /v1/models` —— **只列本入口那一版的模型**。
///
/// 分开入口之后这里不再合并两档：客户端连的是国内版端口，就不该在列表里看到
/// 国际版专有的模型（否则它挑一个发过来，只会换来一个看不懂的 400）。
async fn list_models(State(st): State<AppState>) -> Response {
    match fetch_edition_models(&st.cfg, st.edition).await {
        Ok((ids, _, _)) => {
            let all = merge_extra_models(ids, &st.endpoint.extra_models);
            let data: Vec<Value> = all
                .iter()
                .map(|id| {
                    json!({
                        "id": id,
                        "object": "model",
                        "owned_by": owner_tag(st.edition),
                    })
                })
                .collect();
            Json(json!({"object": "list", "data": data})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": {
                    "message": format!("没有拉到{}的模型列表 —— {e}", st.edition.label()),
                }
            })),
        )
            .into_response(),
    }
}

/// `/v1/models` 的最终清单 = **上游列表 + 本入口声明的 `extra_models`**。
///
/// 上游在前、额外的按声明顺序追加，去重（上游已经列过的不再重复）。
/// 为什么要这份补充，见 [`Endpoint::extra_models`] 的说明 —— 上游那个列表不完整。
fn merge_extra_models(mut upstream: Vec<String>, extra: &[String]) -> Vec<String> {
    for m in extra {
        let m = m.trim();
        if !m.is_empty() && !upstream.iter().any(|x| x == m) {
            upstream.push(m.to_string());
        }
    }
    upstream
}

// ---------------------------------------------------------------- 取模型列表（桌面端按钮用）

/// 从账号库里挑一个可用账号（必要时先刷新 token）。不参与轮转、不动失败冷却。
async fn pick_account_of(cfg: &ProxyConfig, edition: Option<Edition>) -> Option<Value> {
    // 名单取自**对应入口** —— 和 `eligible_accounts` 用同一套判据，别各写一份。
    let want = edition
        .map(|e| cfg.endpoint(e).accounts.clone())
        .unwrap_or_default();
    let acc = load_accounts().into_iter().find(|a| {
        a.get("needs_relogin").and_then(Value::as_bool) != Some(true)
            && edition.map(|e| edition_of(a) == e).unwrap_or(true)
            && (want.is_empty()
                || a.get("uid")
                    .and_then(Value::as_str)
                    .map(|u| want.iter().any(|w| w == u))
                    .unwrap_or(false))
    })?;
    let exp = acc.get("expiresAt").and_then(Value::as_i64).unwrap_or(0);
    if exp - now_ms_i64() >= REFRESH_MARGIN_MS {
        return Some(acc);
    }
    match tokio::time::timeout(Duration::from_secs(30), refresh_account_token(acc.clone())).await {
        Ok(refreshed) if refreshed.get("needs_relogin").and_then(Value::as_bool) != Some(true) => {
            let _ = upsert_account(&refreshed);
            Some(refreshed)
        }
        Ok(_) => None,
        Err(_) => {
            eprintln!("[反代] 取模型列表前刷新超时");
            None
        }
    }
}

/// 拉某一档的模型列表（用**该档自己的账号**）。
///
/// 返回 `(ids, 来源说明, 来源 URL)`；成功时顺手把「模型 → 版本」写进路由表，
/// 之后 `/v1/chat/completions` 就能按 model 挑对版本。
async fn fetch_edition_models(
    cfg: &ProxyConfig,
    edition: Edition,
) -> Result<(Vec<String>, String, String), String> {
    let acc = pick_account_of(cfg, Some(edition)).await.ok_or_else(|| {
        format!(
            "没有可用的{}账号（需要重新登录、不在参与名单里，或账号库里就是没有这一档）",
            edition.label()
        )
    })?;
    let url = models_url(&acc);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client.get(&url);
    for (k, v) in chat_headers(&acc) {
        req = req.header(k, v);
    }
    let resp = req.send().await.map_err(|e| format!("请求上游失败：{e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("上游返回 {status}：{}", cut_text(&text, 160)));
    }
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("上游返回不是 JSON：{e}"))?;
    let ids = parse_model_ids_raw(&v);
    if ids.is_empty() {
        return Err("上游没有返回任何模型".to_string());
    }
    remember_model_routes(edition, &ids);
    Ok((ids, "上游".to_string(), url))
}

/// 「获取模型ID」：按版本分别拉。
///
/// `edition` 传 `"domestic"` / `"international"` 只拉那一档；不传则**两档都拉**。
/// 返回值里 `editions[]` 是分档明细（含各自的失败原因），
/// 另外保留 `models` / `source` / `sourceUrl` 三个扁平字段（= 第一档成功的那份），
/// 老前端不改也能用。
pub async fn fetch_models(edition: Option<String>) -> Result<Value, String> {
    let wanted: Option<Edition> = match edition.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        None => None,
        Some(s) => Some(parse_edition_key(s).ok_or_else(|| {
            format!("未知版本：{s}（可选 domestic / international）")
        })?),
    };

    let cfg = load_proxy_config();
    let list: Vec<Edition> = match wanted {
        Some(e) => vec![e],
        None => Edition::ALL.to_vec(),
    };

    // 两档并行拉，别串行等。
    let results = futures::future::join_all(
        list.iter()
            .map(|e| fetch_edition_models(&cfg, *e))
            .collect::<Vec<_>>(),
    )
    .await;

    let mut editions: Vec<Value> = Vec::new();
    let mut flat_models: Vec<String> = Vec::new();
    let mut flat_source = String::new();
    let mut flat_url = String::new();

    for (e, res) in list.iter().zip(results) {
        match res {
            Ok((ids, source, url)) => {
                // 和 `GET /v1/models` 保持同一口径：并上本入口声明的 `extra_models`。
                // 少了这一步，界面上「获取模型ID」显示的集合会和客户端实际拿到的不一致
                // （上游那个列表不完整，见 `Endpoint::extra_models`）。
                let ids = merge_extra_models(ids, &cfg.endpoint(*e).extra_models);
                if flat_models.is_empty() {
                    flat_models = ids.clone();
                    flat_source = source.clone();
                    flat_url = url.clone();
                }
                editions.push(json!({
                    "edition": e.key(),
                    "label": e.label(),
                    "models": ids,
                    "source": source,
                    "sourceUrl": url,
                    "error": Value::Null,
                }));
            }
            Err(err) => editions.push(json!({
                "edition": e.key(),
                "label": e.label(),
                "models": [],
                "source": "",
                "sourceUrl": "",
                "error": err,
            })),
        }
    }

    if flat_models.is_empty() {
        let why = editions
            .iter()
            .filter_map(|x| x["error"].as_str())
            .collect::<Vec<_>>()
            .join("；");
        return Err(format!("两档都没拉到模型列表 —— {why}"));
    }

    Ok(json!({
        "editions": editions,
        "models": flat_models,
        "source": flat_source,
        "sourceUrl": flat_url,
    }))
}

// ---------------------------------------------------------------- 用量统计

/// 一次出站请求的元信息（用于统计归类）。
#[derive(Clone)]
struct UsageMeta {
    uid: String,
    nickname: String,
    model: String,
    stream: bool,
    /// 🔴 统计必须**按版本分开**：国内版入口只算国内版的量，国际版只算国际版的。
    /// 之前没有这个字段，两版混在同一个 `total` 里，界面上国际版页会显示国内号的用量。
    edition: Edition,
}

const USAGE_FILE: &str = "proxy-usage.json";
/// recent 列表最多保留多少条（**每版各一份**）。
const USAGE_RECENT_MAX: usize = 100;

fn usage_path() -> std::path::PathBuf {
    home_dir().join(".wb-switch").join(USAGE_FILE)
}

static USAGE_LOCK: Mutex<()> = Mutex::new(());

/// `Edition` → 存储里的键名。
fn edition_key(e: Edition) -> &'static str {
    match e {
        Edition::Domestic => "domestic",
        Edition::International => "international",
    }
}

/// 单个版本的用量文档（旧格式的扁平结构原样保留，前端不用大改）。
fn empty_edition_usage() -> Value {
    json!({
        "total": {"requests": 0, "errors": 0, "promptTokens": 0,
                  "completionTokens": 0, "credit": 0},
        "byAccount": {},
        "byModel": {},
        "recent": [],
    })
}

fn empty_usage() -> Value {
    json!({
        "updatedAt": 0,
        "byEdition": {
            "domestic": empty_edition_usage(),
            "international": empty_edition_usage(),
        },
    })
}

/// 把**旧格式**（两版混在一起）迁移成按版本分开。
///
/// 旧文件把两版混在一个 `total` / `byAccount` / `byModel` / `recent` 里，能拆的都拆：
///
/// - `byAccount` 的**键就是 uid** ⇒ 按账号档位精确归位；
/// - `recent` 每条也带 `uid` ⇒ 同样精确归位；
/// - `total` 不猜，直接由**该档的 byAccount 求和**得出（比估算准）；
/// - ⚠️ `byModel` 不含账号信息、**拆不开** ⇒ 只能丢弃（纯展示项，之后重新累积）。
///
/// 之后每次 `record_usage` 只往自己那一档写，`byEdition` 已存在时本函数直接返回。
fn migrate_legacy_usage(doc: Value) -> Value {
    if doc.get("byEdition").is_some() {
        return doc;
    }
    if doc.get("total").is_none() && doc.get("byAccount").is_none() {
        return empty_usage();
    }

    // uid → 档位（按当前账号库判定）
    let mut ed_of: HashMap<String, Edition> = HashMap::new();
    for a in load_accounts() {
        if let Some(uid) = a.get("uid").and_then(Value::as_str) {
            ed_of.insert(uid.to_string(), edition_of(&a));
        }
    }
    migrate_legacy_usage_with(doc, &ed_of)
}

/// [`migrate_legacy_usage`] 的实现体：`uid → 档位` 由调用方给，便于单测。
fn migrate_legacy_usage_with(doc: Value, ed_of: &HashMap<String, Edition>) -> Value {
    if doc.get("byEdition").is_some() {
        return doc;
    }
    if doc.get("total").is_none() && doc.get("byAccount").is_none() {
        return empty_usage();
    }

    let mut out = empty_usage();
    if let Some(t) = doc.get("updatedAt") {
        out["updatedAt"] = t.clone();
    }

    // ① byAccount 精确拆分
    if let Some(m) = doc.get("byAccount").and_then(Value::as_object) {
        for (uid, bucket) in m {
            let key = edition_key(ed_of.get(uid).copied().unwrap_or(Edition::Domestic));
            if let Some(dst) = out["byEdition"][key]["byAccount"].as_object_mut() {
                dst.insert(uid.clone(), bucket.clone());
            }
        }
    }

    // ② 每档 total = 该档 byAccount 求和
    for edition in Edition::ALL {
        let key = edition_key(edition);
        let mut acc = json!({"requests": 0i64, "errors": 0i64, "promptTokens": 0i64,
                             "completionTokens": 0i64, "credit": 0.0f64});
        if let Some(m) = out["byEdition"][key]["byAccount"].as_object() {
            for bucket in m.values() {
                for k in ["requests", "errors", "promptTokens", "completionTokens"] {
                    let v = acc[k].as_i64().unwrap_or(0)
                        + bucket.get(k).and_then(Value::as_i64).unwrap_or(0);
                    acc[k] = json!(v);
                }
                let c = acc["credit"].as_f64().unwrap_or(0.0)
                    + bucket.get("credit").and_then(Value::as_f64).unwrap_or(0.0);
                acc["credit"] = json!(c);
            }
        }
        out["byEdition"][key]["total"] = acc;
    }

    // ③ recent 按 uid 拆分（各自截断）
    if let Some(arr) = doc.get("recent").and_then(Value::as_array) {
        for item in arr {
            let uid = item.get("uid").and_then(Value::as_str).unwrap_or("");
            let key = edition_key(ed_of.get(uid).copied().unwrap_or(Edition::Domestic));
            if let Some(dst) = out["byEdition"][key]["recent"].as_array_mut() {
                if dst.len() < USAGE_RECENT_MAX {
                    dst.push(item.clone());
                }
            }
        }
    }

    out
}

/// 读统计文件；损坏或不存在就返回空结构。旧格式会被就地迁移。
pub fn load_usage() -> Value {
    let p = usage_path();
    if !p.is_file() {
        return empty_usage();
    }
    match std::fs::read_to_string(&p)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
    {
        Some(v) => migrate_legacy_usage(v),
        None => empty_usage(),
    }
}

/// 取**某一版**的用量，形状与旧格式一致（`total` / `byAccount` / `byModel` / `recent`）。
pub fn load_usage_for(edition: Edition) -> Value {
    let doc = load_usage();
    let key = edition_key(edition);
    let mut out = doc
        .get("byEdition")
        .and_then(|v| v.get(key))
        .cloned()
        .unwrap_or_else(empty_edition_usage);
    out["updatedAt"] = doc.get("updatedAt").cloned().unwrap_or(json!(0));
    out
}

/// 清空统计。`edition` 为 `None` 时清**全部**（两版都清）。
pub fn reset_usage(edition: Option<Edition>) -> Result<(), String> {
    let _g = USAGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut doc = match edition {
        None => empty_usage(),
        Some(e) => {
            let mut d = load_usage();
            let key = edition_key(e);
            d["byEdition"][key] = empty_edition_usage();
            d
        }
    };
    doc["updatedAt"] = json!(now_ms());
    std::fs::write(
        usage_path(),
        serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())
}

fn usage_num(usage: Option<&Value>, key: &str) -> i64 {
    usage
        .and_then(|u| u.get(key))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

fn usage_credit(usage: Option<&Value>) -> f64 {
    usage
        .and_then(|u| u.get("credit"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

/// 从上游 SSE 原文里取最后一个 usage 块。
fn usage_from_sse(text: &str) -> Option<Value> {
    let mut last: Option<Value> = None;
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        if let Some(u) = v.get("usage") {
            if !u.is_null() {
                last = Some(u.clone());
            }
        }
    }
    last
}

/// 累加一次请求到统计文件（请求量很小，直接读改写即可）。
fn record_usage(meta: &UsageMeta, usage: Option<&Value>, ok: bool) {
    let prompt = usage_num(usage, "prompt_tokens");
    let completion = usage_num(usage, "completion_tokens");
    let credit = usage_credit(usage);

    let _g = USAGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut doc = load_usage();
    // 🔴 只写自己那一档：国内版入口的量不会跑到国际版页面上（反之亦然）。
    let key = edition_key(meta.edition);
    if doc["byEdition"].get(key).is_none() {
        doc["byEdition"][key] = empty_edition_usage();
    }

    let bump = |obj: &mut Value, prompt: i64, completion: i64, credit: f64, ok: bool| {
        if !obj.is_object() {
            *obj = json!({});
        }
        let o = obj.as_object_mut().unwrap();
        *o.entry("requests").or_insert(json!(0)) =
            json!(o.get("requests").and_then(Value::as_i64).unwrap_or(0) + 1);
        if !ok {
            *o.entry("errors").or_insert(json!(0)) =
                json!(o.get("errors").and_then(Value::as_i64).unwrap_or(0) + 1);
        }
        *o.entry("promptTokens").or_insert(json!(0)) =
            json!(o.get("promptTokens").and_then(Value::as_i64).unwrap_or(0) + prompt);
        *o.entry("completionTokens").or_insert(json!(0)) =
            json!(o.get("completionTokens").and_then(Value::as_i64).unwrap_or(0) + completion);
        *o.entry("credit").or_insert(json!(0.0)) =
            json!(o.get("credit").and_then(Value::as_f64).unwrap_or(0.0) + credit);
    };

    bump(&mut doc["byEdition"][key]["total"], prompt, completion, credit, ok);

    if let Some(m) = doc["byEdition"][key]["byAccount"].as_object_mut() {
        let e = m.entry(meta.uid.clone()).or_insert_with(|| {
            json!({"nickname": meta.nickname, "requests": 0, "errors": 0,
                   "promptTokens": 0, "completionTokens": 0, "credit": 0})
        });
        e["nickname"] = json!(meta.nickname);
        bump(e, prompt, completion, credit, ok);
    }
    if let Some(m) = doc["byEdition"][key]["byModel"].as_object_mut() {
        let e = m
            .entry(meta.model.clone())
            .or_insert_with(|| {
                json!({"requests": 0, "errors": 0, "promptTokens": 0,
                       "completionTokens": 0, "credit": 0})
            });
        bump(e, prompt, completion, credit, ok);
    }

    if let Some(arr) = doc["byEdition"][key]["recent"].as_array_mut() {
        arr.insert(
            0,
            json!({
                "ts": now_ms(),
                "uid": meta.uid,
                "nickname": meta.nickname,
                "model": meta.model,
                "stream": meta.stream,
                "ok": ok,
                "promptTokens": prompt,
                "completionTokens": completion,
                "credit": credit,
            }),
        );
        arr.truncate(USAGE_RECENT_MAX);
    }

    doc["updatedAt"] = json!(now_ms());
    if let Ok(text) = serde_json::to_string_pretty(&doc) {
        let _ = std::fs::write(usage_path(), text);
    }
}

/// 边转发边累积 SSE 原文，流结束时把用量记进统计。
// ---------------------------------------------------------------- 流式重组
//
// 上游会把思考与正文拆成**非常碎**的小帧：实测 `glm-5.3-flash` 一段思考 750 帧、
// 每帧 1~5 个字符（形如 `'The'` `' user'` `' is'`），`kimi-k3-1` 同量级。
//
// 而部分客户端会把**每一帧都当成一个独立的思考段**（ZCode 就是如此，它内部用
// Vercel AI SDK），于是界面上出现几十上百个「思考」小块、每块只有一两个词；
// 正文却正常，因为正文走的是另一套会累积合并的逻辑。
//
// 所以这里不再原样透传，改成有状态的重组：
//   * 同一字段累积到阈值才发一帧；字段切换、流结束、收到末帧时强制 flush；
//   * 顺手规范化上游不合规的地方：中间帧 finish_reason 用 null（上游给的是 `""`）、
//     重组帧不带空字符串字段，role 只在首帧出现；
//   * 🔴 **工具调用帧（`tool_calls` / `function_call` 非空）不参与合并，原样透传** ——
//     它们的 delta 里既没有 reasoning 也没有 content，走合并分支会被整帧吞掉，
//     客户端就永远收不到工具调用（agent 功能全废）。
// 用量统计仍按原有方式在流结束时解析整段文本。
//
// 阈值为什么两个不一样：拿真实抓包跑过（glm-5.3-flash 一段思考 1082 帧、思考合计 2933 字），
// 阈值 24 时思考仍会拆成 112 帧 —— 客户端按帧开块的话就是 112 个方块，等于没修。
// 实测（思考阈值 → 思考帧数）：24→112、96→30、160→19、240→13、400→8。
// 正文必须保持小阈值以维持逐字出字的观感；思考块在被折叠的情况下大一点反而更好读。
// 若你的客户端仍碎得厉害，把 MERGE_MIN_REASONING 调大即可（调到 usize::MAX 就是整段一次出）。
const MERGE_MIN_REASONING: usize = 256;
const MERGE_MIN_CONTENT: usize = 32;

/// 取 `choices[0].delta.<key>` 的字符串（缺失或 null 都当空串）。
fn delta_str(frame: &Value, key: &str) -> String {
    frame
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("delta"))
        .and_then(|d| d.get(key))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// 取 `choices[0].finish_reason`（缺失或 null 都当空串）。
fn finish_str(frame: &Value) -> String {
    frame
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// `delta` 里是否带**实际内容**的工具调用（`tool_calls` 非空数组，或 `function_call` 非空对象）。
///
/// 🔴 这类帧必须**原样透传**，绝不能进重组逻辑：它们的 delta 里既没有 `reasoning_content`
/// 也没有 `content`，会被"只处理这两种字段"的合并分支整帧吞掉 ——
/// 结果是客户端收到 `finish_reason: "tool_calls"` 却拿不到任何工具数据，agent 功能全废。
fn has_tool_delta(frame: &Value) -> bool {
    let Some(delta) = frame
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("delta"))
    else {
        return false;
    };
    if let Some(tc) = delta.get("tool_calls") {
        match tc {
            Value::Array(a) => return !a.is_empty(),
            Value::Null => {}
            _ => return true,
        }
    }
    if let Some(fc) = delta.get("function_call") {
        match fc {
            Value::Object(o) => return !o.is_empty(),
            Value::Null => {}
            _ => return true,
        }
    }
    false
}

/// 上游会把中间帧的 `finish_reason` 填成空字符串 `""`（实测 1081/1082 帧），规范成 `null`。
/// 只在**透传帧**上用；重组出来的帧由 `make_chunk` 直接写 `null`。
fn normalize_finish(frame: &mut Value) {
    let Some(choice) = frame
        .get_mut("choices")
        .and_then(|c| c.as_array_mut())
        .and_then(|a| a.first_mut())
        .and_then(|c| c.as_object_mut())
    else {
        return;
    };
    if matches!(choice.get("finish_reason"), Some(Value::String(s)) if s.is_empty()) {
        choice.insert("finish_reason".to_string(), Value::Null);
    }
}

/// 以上游首帧为模板，重造一帧规范的 OpenAI 流式 chunk。
fn make_chunk(template: &Value, reasoning: &str, content: &str, with_role: bool) -> Value {
    let mut frame = template.clone();
    if let Some(obj) = frame.as_object_mut() {
        obj.insert("usage".to_string(), Value::Null);
        if !obj.contains_key("choices") {
            obj.insert("choices".to_string(), json!([{}]));
        }
        if let Some(cv) = obj.get_mut("choices") {
            if let Some(choices) = cv.as_array_mut() {
                if choices.is_empty() {
                    choices.push(json!({}));
                }
                if let Some(choice) = choices[0].as_object_mut() {
                    choice.insert("finish_reason".to_string(), Value::Null);
                    choice.insert("logprobs".to_string(), Value::Null);
                    let mut delta = serde_json::Map::new();
                    if with_role {
                        delta.insert("role".to_string(), json!("assistant"));
                    }
                    // 只在非空时插入字段：某些客户端按「delta 里出现了 reasoning_content 键」
                    // 来判断是否开启新的思考块，若正文帧也携带空串，会凭空多出一堆空思考块。
                    if !reasoning.is_empty() {
                        delta.insert("reasoning_content".to_string(), json!(reasoning));
                    }
                    if !content.is_empty() {
                        delta.insert("content".to_string(), json!(content));
                    }
                    choice.insert("delta".to_string(), Value::Object(delta));
                }
            }
        }
    }
    frame
}

fn sse_bytes(frame: &Value) -> Option<bytes::Bytes> {
    let text = serde_json::to_string(frame).ok()?;
    Some(bytes::Bytes::from(format!("data: {text}\n\n")))
}

/// 把累积内容合成一帧发出去；返回 None 表示当前没有可发的累积。
fn take_flush(
    template: Option<&Value>,
    field: &str,
    acc: &mut String,
    role_done: &mut bool,
) -> Option<bytes::Bytes> {
    if acc.is_empty() {
        return None;
    }
    let t = template?;
    let (reasoning, content) = if field == "reasoning_content" {
        (acc.as_str(), "")
    } else {
        ("", acc.as_str())
    };
    let out = sse_bytes(&make_chunk(t, reasoning, content, !*role_done));
    *role_done = true;
    acc.clear();
    out
}

/// 读完整条上游流，边重组边推给客户端；结束时补末帧与 `[DONE]`，并记录用量。
async fn sse_pump<S>(
    mut upstream: S,
    tx: tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    meta: UsageMeta,
) where
    S: futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let mut raw = String::new();
    let mut tail = String::new();
    let mut template: Option<Value> = None;
    let mut last_frame: Option<Value> = None;
    let mut role_done = false;
    let mut field = String::new();
    let mut acc = String::new();

    'outer: while let Some(item) = upstream.next().await {
        match item {
            Ok(chunk) => {
                let text = String::from_utf8_lossy(&chunk).to_string();
                raw.push_str(&text);
                tail.push_str(&text);
            }
            Err(e) => {
                record_usage(&meta, None, false);
                let _ = tx
                    .send(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )))
                    .await;
                return;
            }
        }

        // 按空行切出完整事件；不足一个事件的留在 tail 里等下一块
        while let Some(idx) = tail.find("\n\n") {
            let event = tail[..idx].to_string();
            tail.drain(..idx + 2);

            // SSE 规范允许一个事件里有多行 data:，拼接后才是完整 JSON
            let mut data = String::new();
            for line in event.lines() {
                if let Some(rest) = line.strip_prefix("data:") {
                    data.push_str(rest.trim_start());
                }
            }
            if data.is_empty() || data.trim() == "[DONE]" {
                continue;
            }
            let Ok(frame) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            if template.is_none() {
                template = Some(frame.clone());
            }

            // 🔴 工具调用帧优先原样透传（必须在合并分支之前判断）。
            // 它们的 delta 里没有 reasoning_content / content，走下面的分支会被整帧吞掉，
            // 客户端只能收到 finish_reason="tool_calls" 却拿不到工具数据。
            if has_tool_delta(&frame) {
                if let Some(b) = take_flush(template.as_ref(), &field, &mut acc, &mut role_done) {
                    if tx.send(Ok(b)).await.is_err() {
                        break 'outer;
                    }
                }
                field.clear();
                let mut fwd = frame.clone();
                normalize_finish(&mut fwd);
                if let Some(b) = sse_bytes(&fwd) {
                    if tx.send(Ok(b)).await.is_err() {
                        break 'outer;
                    }
                }
                // 这帧已带着自己的 finish_reason 原样发出，不能再进 last_frame
                // （那里会把 delta 清空，工具调用就没了）。
                continue;
            }

            let reasoning = delta_str(&frame, "reasoning_content");
            let content = delta_str(&frame, "content");
            if !reasoning.is_empty() || !content.is_empty() {
                let (next_field, piece) = if !reasoning.is_empty() {
                    ("reasoning_content", reasoning)
                } else {
                    ("content", content)
                };
                if field != next_field {
                    if let Some(b) = take_flush(template.as_ref(), &field, &mut acc, &mut role_done) {
                        if tx.send(Ok(b)).await.is_err() {
                            break 'outer;
                        }
                    }
                    field = next_field.to_string();
                }
                acc.push_str(&piece);
                let limit = if field == "reasoning_content" {
                    MERGE_MIN_REASONING
                } else {
                    MERGE_MIN_CONTENT
                };
                if acc.chars().count() >= limit {
                    if let Some(b) = take_flush(template.as_ref(), &field, &mut acc, &mut role_done) {
                        if tx.send(Ok(b)).await.is_err() {
                            break 'outer;
                        }
                    }
                }
            }

            // 末帧要留着最后发：先把它手里的 delta 并入累积，避免丢结尾
            if !finish_str(&frame).is_empty() {
                last_frame = Some(frame);
            }
        }
    }

    if let Some(b) = take_flush(template.as_ref(), &field, &mut acc, &mut role_done) {
        let _ = tx.send(Ok(b)).await;
    }
    if let Some(mut frame) = last_frame {
        normalize_finish(&mut frame);
        // 末帧的 delta 已经并入上面的累积了，这里清空以免重复输出。
        // ⚠️ 但如果这帧本身带工具调用，就不能清 —— 那是唯一的数据来源。
        if !has_tool_delta(&frame) {
            if let Some(obj) = frame.as_object_mut() {
                if let Some(cv) = obj.get_mut("choices") {
                    if let Some(choices) = cv.as_array_mut() {
                        if let Some(choice) = choices.first_mut().and_then(|c| c.as_object_mut()) {
                            choice.insert("delta".to_string(), json!({}));
                        }
                    }
                }
            }
        }
        if let Some(b) = sse_bytes(&frame) {
            let _ = tx.send(Ok(b)).await;
        }
    }
    let _ = tx
        .send(Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n")))
        .await;

    let usage = usage_from_sse(&raw);
    record_usage(&meta, usage.as_ref(), true);
}

async fn chat_completions(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(incoming): Json<Value>,
) -> Response {
    if !check_auth(&headers, &st.endpoint.api_key) {
        return unauthorized();
    }
    let want_stream = incoming
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let model = incoming
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("deepseek-v4-flash")
        .to_string();

    // 🔴 版本防火墙：本入口只服务一个版本。模型名能**唯一确定**属于另一版时，直接拦下并
    // 说清楚该连哪个端口 —— 放它过去只会拿错版本的号去打上游，换回来一句
    // `11102 only available for authorized users`（实测）。
    //
    // 三种情况**不拦**，交给上游判定：
    // ① 谁都没见过的模型（新模型、还没拉过列表）；
    // ② 两版列表里**都**有的模型 —— 那说明两版都能用（`glm-5.3` / `hy3` / `lite` …）；
    // ③ 用户在本入口 `extra_models` 里**显式声明**的 —— 上游那个列表不完整，
    //    「没列出」≠「不能用」（`deepseek-v4.1-flash` 正是如此）。显式声明优先于自动推断。
    let declared_here = st.endpoint.extra_models.iter().any(|m| m.trim() == model);
    if !declared_here {
        if let Some(want) = model_edition(&model) {
            if want != st.edition {
                let msg = format!(
                    "模型 `{model}` 属于{}，而当前入口是{}（{}）。请改用{}入口（{}）访问。",
                    want.label(),
                    st.edition.label(),
                    st.endpoint.listen,
                    want.label(),
                    st.cfg.endpoint(want).listen,
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": {"message": msg}})),
                )
                    .into_response();
            }
        }
    }

    let Some(acc) = take_account(&st.pool, &st.cfg, Some(st.edition), Some(model.as_str())).await else {
        let message = if st.pool.has_edition(st.edition) {
            format!(
                "{}的账号当前都不可用（冷却中，或需要重新登录）",
                st.edition.label()
            )
        } else {
            format!("参与轮转的账号里没有{}的号", st.edition.label())
        };
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": {"message": message}})),
        )
            .into_response();
    };
    let uid = acc
        .get("uid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let nickname = acc
        .get("nickname")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let meta = UsageMeta {
        uid: uid.clone(),
        nickname,
        model: model.clone(),
        stream: want_stream,
        edition: st.edition,
    };

    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    // 上游不认识的内容块（zcode 等客户端发的 file / document）在这里降级成文本，
    // 否则整条请求会被上游 400：`unsupported content type at index 0: file`。
    let mut degraded: Vec<String> = Vec::new();
    let upstream_body = build_upstream_body(&incoming, st.edition, &mut degraded);
    if !degraded.is_empty() {
        eprintln!(
            "[反代] 内容块降级 {} (model={model})",
            degraded_summary(&degraded)
        );
    }
    // 调试落盘（开关文件控制）：真实客户端报文与我们的复现差在哪，看这个。
    debug_dump(
        "last-request.json",
        &serde_json::to_string_pretty(&json!({
            "edition": st.edition.label(),
            "uid": &uid,
            "model": model,
            "degraded": degraded,
            "body": upstream_body,
        }))
        .unwrap_or_else(|_| "{}".to_string()),
    );

    // 🔴 发请求 + 「渠道校验软拦截」重试，见 MAX_CHANNEL_RETRY。
    // 重试时**重新生成**会话头族（chat_headers 里的 id 是每次调用现算的），
    // 免得同一个 request id 被上游当成重复包直接判掉。
    let mut channel_retry = 0usize;
    let mut transport_retry = 0usize;
    let resp = loop {
        let mut req = client.post(chat_url(&acc));
        for (k, v) in chat_headers(&acc) {
            req = req.header(k, v);
        }
        let r = match req.json(&upstream_body).send().await {
            Ok(r) => r,
            Err(e) => {
                // 🔴 连接层失败：**不冷却账号**（原因见 MAX_TRANSPORT_RETRY）。
                // 只重试一次应对瞬时抖动；仍失败就把 502 如实回给客户端，
                // 由客户端自己决定要不要再发 —— 号本身没坏，别把它冻掉。
                if transport_retry < MAX_TRANSPORT_RETRY {
                    transport_retry += 1;
                    eprintln!(
                        "[反代] 上游连接失败，第 {transport_retry}/{MAX_TRANSPORT_RETRY} 次重试 \
                         (model={model}, uid={uid}): {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    continue;
                }
                debug_dump(
                    "last-failure.txt",
                    &format!(
                        "transport-error\nuid={uid}\nmodel={model}\nedition={}\nretries={transport_retry}\n\n{e}",
                        st.edition.label(),
                    ),
                );
                record_usage(&meta, None, false);
                return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
            }
        };
        if r.status().is_success() {
            break r;
        }
        let raw_status = r.status();
        let text = r.text().await.unwrap_or_default();
        if is_channel_block(&text) && channel_retry < MAX_CHANNEL_RETRY {
            channel_retry += 1;
            // 退避：按次数递增，给上游的间歇判定留出恢复窗口。
            tokio::time::sleep(Duration::from_millis(300 * channel_retry as u64)).await;
            eprintln!(
                "[反代] 上游渠道校验拦下，第 {channel_retry}/{MAX_CHANNEL_RETRY} 次重试 \
                 (model={model}, uid={uid})"
            );
            continue;
        }
        // 重试仍被拦、或本来就不是渠道校验的失败 → 才当账号/上游故障处理。
        debug_dump(
            "last-failure.txt",
            &format!(
                "status={}\nuid={uid}\nmodel={model}\nedition={}\nretries={channel_retry}\n\n{text}",
                raw_status.as_u16(),
                st.edition.label(),
            ),
        );
        // 🔴 只对**账号级**失败冷却号（4xx：鉴权 / 额度 / 渠道校验）。
        // 5xx 是上游自己的问题（502 / 503 / 504 …），拿它去冻号只会把好号冻光 ——
        // 与连接层失败是同一类误判，会让整池无谓地空掉。
        let status = StatusCode::from_u16(raw_status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let account_name = meta.nickname.clone();
        // 🔴 60004 频率超限：按上游声明的重置时刻把**整个账号**暂停出池 ——
        // 120 秒的短冷却只会让它回池后立刻再 429，循环空转。同时给客户端一句
        // 人话：哪个账号、哪个模型、什么时候恢复、怎么继续用，
        // 替代之前那坨双层转义的原始 JSON。
        if let Some(until_ms) = rate_limit_reset_ms(&text) {
            // 长冷却记在**模型**键上：账号的其他模型仍可服务（上游自己也是
            // 这个口径 ——「您也可以切换其他模型继续使用」）。另外给一次账号级
            // 短冷却，把并发请求从「立刻又撞同一个失败号」里拉出来。
            st.pool.mark_model_failed_until(&uid, &model, until_ms);
            st.pool.mark_failed(&uid);
            let reset_at = Local
                .timestamp_millis_opt(until_ms)
                .single()
                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "稍后".to_string());
            eprintln!(
                "[反代] 账号「{account_name}」的模型 {model} 频率超限，已停止对该账号使用该模型至 {reset_at} (uid={uid})"
            );
            record_usage(&meta, None, false);
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": {
                        "message": format!(
                            "账号「{account_name}」的模型 {model} 使用频率已超限，已自动停止对该账号使用这个模型，将于 {reset_at} 自动恢复。该账号的其他模型仍可继续使用 —— 换个模型即可，无需重新登录。"
                        ),
                        "type": "rate_limited",
                        "account": account_name,
                        "model": model,
                        "resetAt": reset_at,
                    }
                })),
            )
                .into_response();
        }
        if raw_status.is_client_error() {
            st.pool.mark_failed(&uid);
            if is_channel_block(&text) {
                eprintln!(
                    "[反代] 渠道校验重试 {channel_retry} 次后仍被拦，冷却该账号 \
                     (model={model}, uid={uid})"
                );
            }
        } else {
            eprintln!(
                "[反代] 上游 {} 失败（上游侧问题，不冷却账号）(model={model}, uid={uid})",
                raw_status.as_u16()
            );
        }
        record_usage(&meta, None, false);
        return (
            status,
            Json(json!({"error": {"message": text, "type": "upstream_error"}})),
        )
            .into_response();
    };

    if want_stream {
        // 不再原样透传：先过一层 SSE 重组器（见 sse_pump 的说明），
        // 否则上游「一两个词一帧」的粒度会让客户端把思考渲染成一片小方块。
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(64);
        // bytes_stream() 返回的 impl Stream 不保证 Unpin，必须 Box::pin 一下
        let upstream = Box::pin(resp.bytes_stream());
        tokio::spawn(async move {
            sse_pump(upstream, tx, meta).await;
        });
        let body = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream; charset=utf-8")
            .header("Cache-Control", "no-cache")
            .header("X-Accel-Buffering", "no")
            .body(axum::body::Body::from_stream(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        with_degraded_header(resp, &degraded)
    } else {
        let resp = match aggregate(Box::pin(resp.bytes_stream()), &model).await {
            Ok(json) => {
                let usage = json.get("usage").cloned();
                record_usage(&meta, usage.as_ref(), true);
                Json(json).into_response()
            }
            Err(e) => {
                record_usage(&meta, None, false);
                (StatusCode::BAD_GATEWAY, e).into_response()
            }
        };
        with_degraded_header(resp, &degraded)
    }
}

/// 把上游 SSE 聚合为一条非流式 OpenAI 响应。
async fn aggregate<S>(mut stream: S, model: &str) -> Result<Value, String>
where
    S: futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut id = String::new();
    let mut usage: Option<Value> = None;

    let mut buf = String::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buf.find('\n') {
            let line = buf[..pos].trim_end_matches('\r').to_string();
            buf = buf[pos + 1..].to_string();
            let Some(payload) = line.strip_prefix("data:") else {
                continue;
            };
            let payload = payload.trim();
            if payload.is_empty() || payload == "[DONE]" {
                continue;
            }
            let Ok(v) = serde_json::from_str::<Value>(payload) else {
                continue;
            };
            if id.is_empty() {
                if let Some(s) = v.get("id").and_then(Value::as_str) {
                    id = s.to_string();
                }
            }
            if let Some(u) = v.get("usage") {
                usage = Some(u.clone());
            }
            if let Some(choices) = v.get("choices").and_then(Value::as_array) {
                if let Some(delta) = choices.first().and_then(|c| c.get("delta")) {
                    if let Some(s) = delta.get("content").and_then(Value::as_str) {
                        content.push_str(s);
                    }
                    if let Some(s) = delta.get("reasoning_content").and_then(Value::as_str) {
                        reasoning.push_str(s);
                    }
                }
            }
        }
    }

    let created = now_ms() / 1000;
    let id = if id.is_empty() { hex_id() } else { id };
    Ok(json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
                "reasoning_content": reasoning
            },
            "finish_reason": "stop"
        }],
        "usage": usage.unwrap_or_else(|| json!(null))
    }))
}

/// `/status` —— 报告**本入口**的情况：它是哪一版、监听在哪、这一版有哪些号。
async fn proxy_status(State(st): State<AppState>) -> impl IntoResponse {
    let allowed = &st.endpoint.accounts;
    let items: Vec<Value> = load_accounts()
        .into_iter()
        .filter(|a| {
            // 只列属于本入口这一版的号 —— 另一版的号在这个端口上用不了。
            edition_of(a) == st.edition
                && (allowed.is_empty()
                    || a.get("uid")
                        .and_then(Value::as_str)
                        .map(|u| allowed.iter().any(|x| x == u))
                        .unwrap_or(false))
        })
        .map(|a| {
            let uid = a
                .get("uid")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let cooling_ms = st.pool.cooling_for(&uid);
            json!({
                "uid": a.get("uid").and_then(Value::as_str),
                "nickname": a.get("nickname").and_then(Value::as_str),
                "needs_relogin": a.get("needs_relogin").and_then(Value::as_bool).unwrap_or(false),
                "expiresAt": a.get("expiresAt").and_then(Value::as_i64),
                // >0 表示该账号正在冷却（如频率超限被暂停），前端显示剩余时间
                "coolingMs": cooling_ms,
            })
        })
        .collect();
    Json(json!({
        "edition": st.edition.key(),
        "editionLabel": st.edition.label(),
        "enabled": st.endpoint.enabled,
        "listen": st.endpoint.listen,
        "hasApiKey": !st.endpoint.api_key.trim().is_empty(),
        "accounts": items
    }))
}

/// 本入口自己的用量统计（**只含本版**，不会把另一版的量混进来）。
async fn usage_stats(State(st): State<AppState>) -> Response {
    Json(load_usage_for(st.edition)).into_response()
}

// ---------------------------------------------------------------- 启动

fn router(st: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/status", get(proxy_status))
        .route("/usage-stats", get(usage_stats))
        // 必须在 with_state 之前挂：layer 只对「它之前注册的路由」生效。
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY))
        .with_state(st)
}

/// 启动一个入口（阻塞，直到监听失败或进程退出）。
///
/// 每个入口一条独立线程 + 独立 Tokio 运行时，所以国内版与国际版互不干扰：
/// 一边起不来不会拖累另一边。但**账号池是共享的**（由调用方传入）。
async fn run_endpoint_until(
    edition: Edition,
    endpoint: Endpoint,
    pool: Arc<Pool>,
    cfg: Arc<ProxyConfig>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    ready: std::sync::mpsc::Sender<Result<(), String>>,
) -> Result<(), String> {
    let addr: SocketAddr = endpoint
        .listen
        .parse()
        .map_err(|_| format!("监听地址无效: {}", endpoint.listen))?;
    // 这一版一个号都没有就别起了 —— 起来了也只会每次请求都 503。
    if !pool.has_edition(edition) {
        let msg = format!(
            "没有可用的{}账号：账号库为空、都需重新登录，或不在参与名单里",
            edition.label()
        );
        let _ = ready.send(Err(msg.clone()));
        return Err(msg);
    }
    let st = AppState {
        cfg,
        pool,
        edition,
        endpoint: Arc::new(endpoint),
    };
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            let msg = format!("监听 {addr} 失败: {e}");
            let _ = ready.send(Err(msg.clone()));
            return Err(msg);
        }
    };
    println!(
        "[反代] {} 已启动 http://{addr}  (POST /v1/chat/completions)",
        edition.label()
    );
    let _ = ready.send(Ok(()));
    axum::serve(listener, router(st))
        .with_graceful_shutdown(async {
            let _ = shutdown.await;
        })
        .await
        .map_err(|e| e.to_string())
}

/// 阻塞式启动**国内版**入口（留给「只用一次、不接 UI」的调用方）。
///
/// 桌面端请走 [`spawn_from_config`] —— 它会按配置把两个入口都起起来。
pub async fn run_proxy_server(cfg: ProxyConfig) -> Result<(), String> {
    let edition = Edition::Domestic;
    let endpoint = cfg.endpoint(edition).clone();
    let pool = Arc::new(Pool::new(eligible_accounts(&cfg)));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, _ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    run_endpoint_until(edition, endpoint, pool, Arc::new(cfg), shutdown_rx, ready_tx).await
}

/// 按配置挑出参与反代的 uid（两个入口的**并集**，供两者共享的账号池使用）。
///
/// 每个版本的号只由**它自己那个入口**的名单决定 —— 国内版入口勾了谁就是谁，
/// 不会受国际版入口勾选的影响。
fn eligible_accounts(cfg: &ProxyConfig) -> Vec<(String, Edition)> {
    let all = load_accounts();
    let mut out: Vec<(String, Edition)> = Vec::new();
    for edition in Edition::ALL {
        let allowed = &cfg.endpoint(edition).accounts;
        for a in &all {
            if a.get("needs_relogin").and_then(Value::as_bool) == Some(true) {
                continue;
            }
            if edition_of(a) != edition {
                continue;
            }
            let Some(uid) = a.get("uid").and_then(Value::as_str) else {
                continue;
            };
            if !allowed.is_empty() && !allowed.iter().any(|x| x == uid) {
                continue;
            }
            out.push((uid.to_string(), edition));
        }
    }
    out
}

/// 供桌面端调用：读配置，把**已启用**的入口都在后台起起来。
pub fn spawn_from_config() {
    let cfg = load_proxy_config();
    if !cfg.any_enabled() {
        return;
    }
    for (edition, outcome) in apply_config(&cfg) {
        if let Err(msg) = outcome {
            eprintln!("[反代] {} 入口启动失败: {msg}", edition.label());
        }
    }
}

// ---------------------------------------------------------------- 生命周期
//
// 说明：Tauri 的 setup() 与同步 command 都跑在**主线程**，那里没有 Tokio 运行时，
// 直接 `tokio::spawn` 会 panic。所以这里**每个入口自己开一条带独立运行时的线程**，
// 无论从启动流程还是从 command 调用都安全。
//
// 国内版与国际版各占一条，互不干扰；账号池由两者共享。

/// 一个入口的运行态。
struct EndpointRuntime {
    /// 记下来是为了状态查询 / 日志能说出「哪个地址在跑」。
    listen: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn runtimes() -> &'static Mutex<HashMap<Edition, EndpointRuntime>> {
    static R: OnceLock<Mutex<HashMap<Edition, EndpointRuntime>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 某个入口是否正在监听。
pub fn is_running(edition: Edition) -> bool {
    runtimes()
        .lock()
        .map(|m| m.contains_key(&edition))
        .unwrap_or(false)
}

/// 有没有**任何**入口在监听。
pub fn proxy_running() -> bool {
    runtimes().lock().map(|m| !m.is_empty()).unwrap_or(false)
}

/// 正在监听的入口 → 它的监听地址。界面用它展示「哪几个在跑」。
pub fn running_endpoints() -> Vec<(Edition, String)> {
    let mut v: Vec<(Edition, String)> = runtimes()
        .lock()
        .map(|m| m.iter().map(|(e, r)| (*e, r.listen.clone())).collect())
        .unwrap_or_default();
    v.sort_by_key(|(e, _)| e.key());
    v
}

/// 起一个入口（非阻塞）。
fn start_endpoint(
    edition: Edition,
    endpoint: Endpoint,
    pool: Arc<Pool>,
    cfg: Arc<ProxyConfig>,
) -> Result<(), String> {
    if is_running(edition) {
        return Err(format!("{}入口已经在运行", edition.label()));
    }
    // 先探地址合法性，避免后台线程里默默失败。
    endpoint
        .listen
        .parse::<SocketAddr>()
        .map_err(|_| format!("监听地址无效: {}", endpoint.listen))?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    let listen = endpoint.listen.clone();

    let thread = std::thread::Builder::new()
        .name(format!("wb-api-proxy-{}", edition.key()))
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(format!("创建运行时失败: {e}")));
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) =
                    run_endpoint_until(edition, endpoint, pool, cfg, shutdown_rx, ready_tx).await
                {
                    eprintln!("[反代] {} 运行结束: {e}", edition.label());
                }
            });
        })
        .map_err(|e| format!("无法启动反代线程: {e}"))?;

    match ready_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {
            if let Ok(mut m) = runtimes().lock() {
                m.insert(
                    edition,
                    EndpointRuntime {
                        listen,
                        shutdown: Some(shutdown_tx),
                        thread: Some(thread),
                    },
                );
            }
            Ok(())
        }
        Ok(Err(e)) => {
            let _ = shutdown_tx.send(());
            let _ = thread.join();
            Err(e)
        }
        Err(_) => {
            let _ = shutdown_tx.send(());
            Err("启动超时：10 秒内没有就绪".to_string())
        }
    }
}

/// 停掉全部入口（阻塞到线程退出）。
pub fn stop_proxy_server() {
    let taken: Vec<(Edition, EndpointRuntime)> = match runtimes().lock() {
        Ok(mut m) => m.drain().collect(),
        Err(_) => return,
    };
    for (edition, mut rt) in taken {
        if let Some(tx) = rt.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(th) = rt.thread.take() {
            let _ = th.join();
        }
        println!("[反代] {} 已停止", edition.label());
    }
}

/// 按配置重建两个入口。返回**每个已启用版本**的启动结果。
///
/// 刻意做成「逐版本返回」而不是一个总的 Result：国际版没账号属于常态，
/// 不该因此让国内版也起不来，更不该让「保存配置」这个动作整体失败。
pub fn apply_config(cfg: &ProxyConfig) -> Vec<(Edition, Result<(), String>)> {
    stop_proxy_server();
    let pool = Arc::new(Pool::new(eligible_accounts(cfg)));
    let cfg = Arc::new(cfg.clone());
    let mut out = Vec::new();
    for edition in Edition::ALL {
        let endpoint = cfg.endpoint(edition).clone();
        if !endpoint.enabled {
            continue; // 未启用 = 不起，也不算失败
        }
        let r = start_endpoint(edition, endpoint, pool.clone(), cfg.clone());
        out.push((edition, r));
    }
    out
}

/// 兼容旧调用名：按当前配置重启。
///
/// 任一入口失败都会汇总进 Err（但**不影响另一个**已经起来的）。
/// 调用方若要逐版本判断，直接用 [`apply_config`]。
pub fn restart_proxy_server() -> Result<(), String> {
    let cfg = load_proxy_config();
    let errs: Vec<String> = apply_config(&cfg)
        .into_iter()
        .filter_map(|(e, r)| r.err().map(|m| format!("{}：{m}", e.label())))
        .collect();
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs.join("；"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 60004 的双层转义 JSON 必须能解析出重置时刻 —— 这是「按重置时间
    /// 暂停账号」而不是「120 秒后回池再 429」的依据。
    #[test]
    fn parses_rate_limit_reset_from_nested_json() {
        let body = r#"{"message":"{\"code\":60004,\"msg\":\"您的使用量已超出频率限制，将在 2026-10-17 11:46:56 UTC+8 重置，您也可以切换其他模型继续使用。\",\"requestId\":\"00000000000000463270d2d3db8ba1\"}","type":"upstream_error"}"#;
        let until = rate_limit_reset_ms(body).expect("60004 应解析出重置时刻");
        let expected = chrono::FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .from_local_datetime(
                &chrono::NaiveDateTime::parse_from_str(
                    "2026-10-17 11:46:56",
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap(),
            )
            .unwrap()
            .timestamp_millis();
        assert_eq!(until, expected);
    }

    /// 非频率超限的上游体不得误判 —— 否则普通 4xx 会被错误地长时间冷却。
    #[test]
    fn non_rate_limit_body_returns_none() {
        assert!(rate_limit_reset_ms(r#"{"message":"{\"code\":11128}"}"#).is_none());
        assert!(rate_limit_reset_ms("plain text").is_none());
        assert!(rate_limit_reset_ms("").is_none());
        // 有 60004 关键字但缺「将在 … 重置」时间串的残缺体也要兜住。
        assert!(rate_limit_reset_ms(r#"{"message":"{\"code\":60004,\"msg\":\"?\"}"}"#).is_none());
    }

    /// system prompt 末尾的环境注入块会被摘掉——上游的渠道校验（11128）正是靠
    /// 识别这段判定「非官方客户端」。见 `strip_env_injection`。
    #[test]
    fn strips_git_env_injection_from_system_prompt() {
        let raw = "You are ZCode.\n\n# Rules\nBe brief.\n\n\
                   gitStatus: clean\n\nCurrent branch: main\n\nGit user: someone\n";
        let cleaned = strip_env_injection(raw).expect("应当能摘出环境块");
        assert_eq!(cleaned, "You are ZCode.\n\n# Rules\nBe brief.");
        assert!(!cleaned.contains("gitStatus:"), "gitStatus 块必须整块消失");
        assert!(!cleaned.contains("Git user:"), "紧跟其后的环境行要一起摘掉");
    }

    /// 没有环境块时原样放行；整条都是环境块时也不动（不能把 system 清空）。
    #[test]
    fn env_strip_is_noop_when_nothing_to_strip() {
        assert!(strip_env_injection("You are WorkBuddy. Be helpful.").is_none());
        assert!(
            strip_env_injection("gitStatus: clean").is_none(),
            "全是要摘的内容就干脆别动，免得 system 变空"
        );
    }

    /// 旧格式（两版混在一起）迁移后必须**按版本分开**：
    /// byAccount / recent 按 uid 精确归位，每档 total 由该档 byAccount 求和。
    #[test]
    fn legacy_usage_splits_by_edition() {
        let mut ed_of = HashMap::new();
        ed_of.insert("u-cn".to_string(), Edition::Domestic);
        ed_of.insert("u-ai".to_string(), Edition::International);

        let legacy = json!({
            "updatedAt": 111,
            // 旧字段（混合），迁移时应被丢弃/不用
            "total": {"requests": 999, "errors": 9, "promptTokens": 9, "completionTokens": 9, "credit": 9.0},
            "byAccount": {
                "u-cn": {"nickname": "国内号", "requests": 3, "errors": 1,
                         "promptTokens": 100, "completionTokens": 10, "credit": 1.5},
                "u-ai": {"nickname": "国际号", "requests": 7, "errors": 2,
                         "promptTokens": 200, "completionTokens": 20, "credit": 2.5},
            },
            "byModel": {"m": {"requests": 10}},
            "recent": [
                {"ts": 2, "uid": "u-ai", "model": "m", "ok": true},
                {"ts": 1, "uid": "u-cn", "model": "m", "ok": false},
            ],
        });

        let out = migrate_legacy_usage_with(legacy, &ed_of);
        assert_eq!(out["updatedAt"], 111);

        let cn = &out["byEdition"]["domestic"];
        let ai = &out["byEdition"]["international"];

        // byAccount 精确归位
        assert_eq!(cn["byAccount"]["u-cn"]["requests"], 3);
        assert!(cn["byAccount"].get("u-ai").is_none(), "国际号不能落到国内档");
        assert_eq!(ai["byAccount"]["u-ai"]["requests"], 7);

        // total 由各自 byAccount 求和，而不是照抄旧的混合 total
        assert_eq!(cn["total"]["requests"], 3);
        assert_eq!(cn["total"]["promptTokens"], 100);
        assert_eq!(cn["total"]["credit"], 1.5);
        assert_eq!(ai["total"]["requests"], 7);
        assert_eq!(ai["total"]["promptTokens"], 200);
        assert_eq!(ai["total"]["credit"], 2.5);

        // recent 按 uid 拆开
        assert_eq!(cn["recent"].as_array().unwrap().len(), 1);
        assert_eq!(cn["recent"][0]["uid"], "u-cn");
        assert_eq!(ai["recent"].as_array().unwrap().len(), 1);
        assert_eq!(ai["recent"][0]["uid"], "u-ai");

        // 拆不开的 byModel 不保留（纯展示项）
        assert!(cn["byModel"].as_object().unwrap().is_empty());
        assert!(ai["byModel"].as_object().unwrap().is_empty());
    }

    /// 已经是新格式时迁移必须是空操作（幂等），且不误判损坏文件。
    #[test]
    fn usage_migration_is_idempotent_and_safe() {
        let mut ed_of = HashMap::new();
        ed_of.insert("u-cn".to_string(), Edition::Domestic);

        let already = json!({
            "updatedAt": 5,
            "byEdition": {
                "domestic": {"total": {"requests": 1}, "byAccount": {}, "byModel": {}, "recent": []},
                "international": {"total": {"requests": 2}, "byAccount": {}, "byModel": {}, "recent": []},
            },
        });
        let out = migrate_legacy_usage_with(already, &ed_of);
        assert_eq!(out["byEdition"]["international"]["total"]["requests"], 2);

        // 空文件 / 损坏文件 → 空结构，不 panic
        let empty = migrate_legacy_usage_with(json!({}), &ed_of);
        assert_eq!(empty["byEdition"]["domestic"]["total"]["requests"], 0);
    }

    /// 渠道校验软拦截（11128）要能被认出来 —— 它是**可重试**的，认错就会白冻一个号。
    ///
    /// 真实响应体长这样（上游把内层 JSON 当字符串塞进 `message`）：
    /// `400: {"message":"{\"code\":11128,\"msg\":\"Illegal API invocation from an
    /// unapproved channel\",...}","type":"upstream_error"}`
    #[test]
    fn detects_channel_block_from_upstream_body() {
        let real = r#"400: {"message":"{\"code\":11128,\"msg\":\"Illegal API invocation from an unapproved channel\",\"requestId\":\"000000000000000046302613c7e69e4d\"}","type":"upstream_error"}"#;
        assert!(is_channel_block(real), "真实响应体必须命中");

        // 只带码、或只带 msg 的变体也要认。
        assert!(is_channel_block(r#"{"code":11128}"#));
        assert!(is_channel_block("Illegal API invocation from an unapproved channel"));

        // 🔴 不能误伤：别的错误码（含恰好包含 11128 之外的数字串）都算普通失败。
        assert!(!is_channel_block(r#"{"code":11102,"msg":"service info not found"}"#));
        assert!(!is_channel_block(r#"{"code":11101,"msg":"unsupported content type"}"#));
        assert!(!is_channel_block(""));
    }

    /// 净化只作用于 system 消息，user / assistant 的内容一个字都不能少。
    #[test]
    fn env_strip_only_touches_system_role() {
        let mut degraded = Vec::new();
        let msgs = json!([
            { "role": "system", "content": "Rules.\n\ngitStatus: clean\n\nGit user: x\n" },
            { "role": "user", "content": "gitStatus: 用户自己写的，必须原样保留" },
            { "role": "assistant", "content": "ok" },
        ]);
        let out = sanitize_messages(&msgs, &mut degraded);
        assert_eq!(out[0]["content"], "Rules.");
        assert_eq!(out[1]["content"], "gitStatus: 用户自己写的，必须原样保留");
        assert_eq!(out[2]["content"], "ok");
        assert_eq!(degraded, vec!["system-env".to_string()]);
    }

    /// 🔴 `role: "developer"` 必须改写成 `system`，否则上游整条 400
    /// `11128 Illegal API invocation from an unapproved channel`。
    ///
    /// 实测单变量对照（国内版 / 国际版各 3/3 稳定）：
    /// `[system, developer, user]` → 11128；把 developer 换成 system → 200。
    /// DSH 这类客户端就是这么发 system prompt 的。
    #[test]
    fn developer_role_is_rewritten_to_system() {
        let mut degraded = Vec::new();
        let msgs = json!([
            { "role": "developer", "content": "You are an AI agent powered by DeepSeek Harness." },
            { "role": "user", "content": "hi" },
        ]);
        let out = sanitize_messages(&msgs, &mut degraded);
        assert_eq!(out[0]["role"], "system", "developer 必须被改成 system");
        assert_eq!(out[0]["content"], "You are an AI agent powered by DeepSeek Harness.",
                   "内容一个字都不能动");
        assert_eq!(out[1]["role"], "user", "user 消息不受影响");
        assert_eq!(degraded, vec!["developer-role".to_string()]);

        // 改写后还要能吃到 system 的环境注入净化（顺序：先改 role 再净化）。
        let mut d2 = Vec::new();
        let msgs2 = json!([
            { "role": "developer", "content": "Rules.\n\ngitStatus: clean\n\nGit user: x\n" },
            { "role": "user", "content": "hi" },
        ]);
        let out2 = sanitize_messages(&msgs2, &mut d2);
        assert_eq!(out2[0]["role"], "system");
        assert_eq!(out2[0]["content"], "Rules.");
        assert_eq!(d2, vec!["developer-role".to_string(), "system-env".to_string()]);

        // 没有 developer 时不能凭空加标记。
        let mut d3 = Vec::new();
        sanitize_messages(&json!([{ "role": "system", "content": "ok" }]), &mut d3);
        assert!(d3.is_empty());
    }

    /// v1 的扁平配置能自动迁移成双入口：旧的 `listen` / `api_key` / `enabled`
    /// 归国内版，国际版保持默认关闭 —— 不擅自替用户开一个以前不存在的监听端口。
    #[test]
    fn legacy_flat_config_migrates_to_two_endpoints() {
        let old: LegacyConfig = serde_json::from_str(
            r#"{"enabled":true,"listen":"127.0.0.1:9999","api_key":"sk-old","accounts":["u1"]}"#,
        )
        .expect("v1 扁平配置应当能解析");
        let cfg = ProxyConfig::from_legacy(old);

        assert!(cfg.domestic.enabled);
        assert_eq!(cfg.domestic.listen, "127.0.0.1:9999");
        assert_eq!(cfg.domestic.api_key, "sk-old");
        assert_eq!(cfg.domestic.accounts, vec!["u1".to_string()]);

        assert!(!cfg.international.enabled, "国际版不该被自动开启");
        assert_eq!(cfg.international.listen, DEFAULT_LISTEN_INTERNATIONAL);
        assert!(cfg.international.api_key.is_empty());
        assert!(cfg.international.accounts.is_empty());

        assert!(cfg.any_enabled());
    }

    /// v2（两个入口 + 顶层共用名单）→ 现行结构：名单同时发给两边，行为等价。
    #[test]
    fn v2_shared_account_list_is_split_to_both() {
        let old: V2Config = serde_json::from_str(
            r#"{"domestic":{"enabled":true,"listen":"127.0.0.1:7863","api_key":"a"},
                "international":{"enabled":true,"listen":"127.0.0.1:7864","api_key":"b"},
                "accounts":["u1","u2"]}"#,
        )
        .expect("v2 配置应当能解析");
        let cfg = ProxyConfig::from_v2(old);

        assert_eq!(cfg.domestic.accounts, vec!["u1".to_string(), "u2".to_string()]);
        assert_eq!(cfg.international.accounts, vec!["u1".to_string(), "u2".to_string()]);
        assert_eq!(cfg.domestic.api_key, "a");
        assert_eq!(cfg.international.api_key, "b");
    }

    /// 监听地址留空时补该版本的默认端口（国内 7863 / 国际 7864）。
    #[test]
    fn empty_listen_falls_back_to_edition_default() {
        let cfg = serde_json::from_str::<ProxyConfig>(r#"{"domestic":{"enabled":true}}"#)
            .expect("新格式应当能解析")
            .resolved();

        assert_eq!(cfg.domestic.listen, DEFAULT_LISTEN_DOMESTIC);
        assert_eq!(cfg.international.listen, DEFAULT_LISTEN_INTERNATIONAL);
        assert!(cfg.endpoint(Edition::Domestic).enabled);
        assert!(!cfg.endpoint(Edition::International).enabled);
    }

    /// 两个入口的 key 与账号名单都互相独立；写回后必须仍被识别为**现行格式**
    /// —— 否则下次读会被当旧格式再迁移一遍（迁移循环）。
    #[test]
    fn two_endpoints_keep_independent_key_and_accounts() {
        let cfg = serde_json::from_str::<ProxyConfig>(
            r#"{"domestic":{"enabled":true,"listen":"127.0.0.1:7863","api_key":"sk-cn","accounts":["u-cn"]},
                "international":{"enabled":true,"listen":"127.0.0.1:7864","api_key":"sk-intl","accounts":["u-intl"]}}"#,
        )
        .expect("现行格式应当能解析")
        .resolved();

        assert_eq!(cfg.domestic.api_key, "sk-cn");
        assert_eq!(cfg.international.api_key, "sk-intl");
        assert_ne!(cfg.domestic.api_key, cfg.international.api_key);
        assert_eq!(cfg.domestic.accounts, vec!["u-cn".to_string()]);
        assert_eq!(cfg.international.accounts, vec!["u-intl".to_string()]);
        assert!(cfg.any_enabled());

        let v = serde_json::to_value(&cfg).expect("应当能序列化");
        assert!(
            v.get("domestic").is_some() && v.get("international").is_some(),
            "写回的必须是现行格式"
        );
        assert!(v.get("listen").is_none(), "不该再写出 v1 的顶层 listen");
        assert!(
            v.get("accounts").is_none(),
            "顶层不该再有 accounts —— 有的话下次读会被当 v2 再迁移一次"
        );
    }

    /// 鉴权只看**本入口**那把 key。
    #[test]
    fn auth_uses_this_endpoint_key_only() {
        let mut h = HeaderMap::new();
        h.insert("authorization", HeaderValue::from_static("Bearer sk-cn"));

        assert!(check_auth(&h, ""), "留空 = 不鉴权");
        assert!(check_auth(&h, "sk-cn"));
        assert!(check_auth(&h, "  sk-cn  "), "容忍两侧空白");
        assert!(!check_auth(&h, "sk-intl"), "另一入口的 key 不该放行");
    }

    /// 版本标识解析：前端传的是 key，也容忍几个常见别名。
    #[test]
    fn edition_key_parsing() {
        assert_eq!(parse_edition_key("domestic"), Some(Edition::Domestic));
        assert_eq!(parse_edition_key(" INTERNATIONAL "), Some(Edition::International));
        assert_eq!(parse_edition_key("intl"), Some(Edition::International));
        assert_eq!(parse_edition_key("国内版"), Some(Edition::Domestic));
        assert_eq!(parse_edition_key("国际版"), Some(Edition::International));
        assert_eq!(parse_edition_key("nope"), None);
        assert_eq!(parse_edition_key(""), None);
    }

    /// 模型归属标签：每个入口只列自己那一版，`owned_by` 要能看出是哪一版。
    ///
    /// （`/v1/models` 以前是「两档合并 + 同名标 workbuddy」，改成按入口分端口之后
    /// 不再合并 —— 连国内版端口就不该在列表里看到国际版专有的模型。）
    #[test]
    fn owner_tag_marks_edition() {
        assert_eq!(owner_tag(Edition::Domestic), "workbuddy-cn");
        assert_eq!(owner_tag(Edition::International), "workbuddy-intl");
    }

    /// 只有一档有的模型 → 判给那一档；**两档都有 → 两类都不判**（否则会互相误拦）。
    #[test]
    fn model_routes_union_not_overwrite() {
        remember_model_routes(Edition::International, &["intl-only-model".to_string()]);
        assert_eq!(
            model_edition("intl-only-model"),
            Some(Edition::International)
        );
        assert_eq!(model_edition("never-seen-model"), None);

        // 🔴 同名模型被两档各记一次后，必须变成「都能用」，而不是被后写的那档覆盖。
        // 实测踩过：国际版列表后拉之后，7863 发 `glm-5.3` 会被误报「属于国际版」。
        remember_model_routes(Edition::Domestic, &["shared-model".to_string()]);
        remember_model_routes(Edition::International, &["shared-model".to_string()]);
        assert_eq!(
            model_edition("shared-model"),
            None,
            "两档都列出过的模型不该被判成某一档，否则另一档的入口会被误拦"
        );
        // 顺序反过来也必须一样
        remember_model_routes(Edition::International, &["shared2".to_string()]);
        remember_model_routes(Edition::Domestic, &["shared2".to_string()]);
        assert_eq!(model_edition("shared2"), None);
    }

    /// `extra_models` 并入 `/v1/models`：追加在末尾、去重、去空白、忽略空项。
    #[test]
    fn extra_models_merge_into_model_list() {
        let up = vec!["a".to_string(), "b".to_string()];
        assert_eq!(merge_extra_models(up.clone(), &[]), up, "没声明时原样返回");

        let out = merge_extra_models(
            up,
            &[
                " deepseek-v4.1-flash ".to_string(),
                "a".to_string(),
                "   ".to_string(),
            ],
        );
        assert_eq!(
            out,
            vec!["a", "b", "deepseek-v4.1-flash"],
            "上游在前；`a` 已存在不重复；空白/空项忽略"
        );
    }

    /// 账号池按版本取号：只要国内版时不该给出国际版的号。
    #[test]
    fn pool_filters_by_edition() {
        let pool = Pool::new(vec![
            ("u-intl".to_string(), Edition::International),
            ("u-dom".to_string(), Edition::Domestic),
        ]);
        assert_eq!(pool.next_uid_of(Some(Edition::Domestic), None).as_deref(), Some("u-dom"));
        assert_eq!(pool.next_uid_of(Some(Edition::International), None).as_deref(), Some("u-intl"));
        assert!(pool.has_edition(Edition::Domestic));
        assert!(pool.has_edition(Edition::International));

        let only_intl = Pool::new(vec![("u-intl".to_string(), Edition::International)]);
        assert_eq!(only_intl.next_uid_of(Some(Edition::Domestic), None), None);
        assert!(!only_intl.has_edition(Edition::Domestic));
        // 不限版本时照常能拿到
        assert_eq!(only_intl.next_uid_of(None, None).as_deref(), Some("u-intl"));
    }

    /// 🔴 60004 只冻**模型**：同账号的其他模型仍能被选中，冷却到期后该模型自动回归。
    /// 账号级长冷却会把还能用的模型一起冻掉，与上游「可切换其他模型继续使用」矛盾。
    #[test]
    fn model_cooldown_only_blocks_that_model() {
        let pool = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        pool.mark_model_failed_until("u1", "model-a", now_ms_i64() + 3_600_000);

        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-a")),
            None,
            "被冻的模型不应再选中该号"
        );
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-b")).as_deref(),
            Some("u1"),
            "同账号的其他模型必须照常可用"
        );
        // 不关心模型时（model = None）不受模型冷却限制
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), None).as_deref(),
            Some("u1")
        );

        // 已过期的冷确不应生效（mark_model_failed_until 会直接忽略过去时刻）
        let fresh = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        fresh.mark_model_failed_until("u1", "model-a", now_ms_i64() - 1_000);
        assert_eq!(
            fresh
                .next_uid_of(Some(Edition::Domestic), Some("model-a"))
                .as_deref(),
            Some("u1")
        );
    }

    /// 复现线上的 400：客户端把附件当 `{"type":"file"}` 发过来，
    /// 上游回 `Parse message failed: unsupported content type at index 0: file`。
    #[test]
    fn file_block_is_downgraded_to_text() {
        let msgs = json!([{
            "role": "user",
            "content": [
                { "type": "file", "file": { "filename": "a.pdf", "file_data": "data:application/pdf;base64,JVBERi0=" } },
                { "type": "text", "text": "看看这个" }
            ]
        }]);
        let mut degraded = Vec::new();
        let out = sanitize_messages(&msgs, &mut degraded);

        let parts = out[0]["content"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "text");
        let note = parts[0]["text"].as_str().unwrap();
        assert!(note.contains("a.pdf"), "{note}");
        assert!(note.contains("已省略"), "{note}");
        // 文本块原地保留（位置不动，索引不变）。
        assert_eq!(parts[1]["text"], "看看这个");
        assert_eq!(degraded_summary(&degraded), "file=1");
    }

    #[test]
    fn plain_string_content_is_untouched() {
        let msgs = json!([{ "role": "user", "content": "你好" }]);
        let mut degraded = Vec::new();
        let out = sanitize_messages(&msgs, &mut degraded);
        assert_eq!(out, msgs);
        assert!(degraded.is_empty());
    }

    #[test]
    fn textual_attachment_is_inlined() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode("hello 世界".as_bytes());
        let msgs = json!([{
            "role": "user",
            "content": [{ "type": "file", "file": {
                "filename": "note.md",
                "file_data": format!("data:text/markdown;base64,{b64}")
            }}]
        }]);
        let mut degraded = Vec::new();
        let out = sanitize_messages(&msgs, &mut degraded);
        let text = out[0]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("hello 世界"), "{text}");
    }

    #[test]
    fn unknown_type_never_survives() {
        // 未知块必须被换掉，否则又是一个 400。
        let msgs = json!([{
            "role": "user",
            "content": [{ "type": "weird_new_thing", "foo": 1 }, { "type": "text", "text": "x" }]
        }]);
        let mut degraded = Vec::new();
        let out = sanitize_messages(&msgs, &mut degraded);
        let parts = out[0]["content"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| p["type"] == "text"));
        assert_eq!(degraded_summary(&degraded), "weird_new_thing=1");
    }

    #[test]
    fn image_and_text_blocks_survive() {
        let msgs = json!([{
            "role": "user",
            "content": [
                { "type": "text", "text": "看图" },
                { "type": "image_url", "image_url": { "url": "https://x/y.png" } }
            ]
        }]);
        let mut degraded = Vec::new();
        let out = sanitize_messages(&msgs, &mut degraded);
        assert_eq!(out, msgs);
        assert!(degraded.is_empty());
    }

    #[test]
    fn upstream_body_keeps_params_and_sanitizes() {
        let incoming = json!({
            "model": "glm-5.3-flash",
            "temperature": 0.3,
            "messages": [{
                "role": "user",
                "content": [{ "type": "file", "file": { "file_id": "f_123" } }]
            }]
        });
        let mut degraded = Vec::new();
        // 用国内版：国际版会额外兜一条 system（见下面的专门测试），断言就对不上了。
        let body = build_upstream_body(&incoming, Edition::Domestic, &mut degraded);
        assert_eq!(body["model"], "glm-5.3-flash");
        assert_eq!(body["stream"], true);
        assert_eq!(body["temperature"], 0.3);
        assert_eq!(body["messages"][0]["content"][0]["type"], "text");
        assert!(body["messages"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("f_123"));
    }

    /// 思考强度类字段必须原样透传 —— 以前被整段丢掉，表现为「客户端调了推理等级、
    /// 上游毫无反应」。同时确认：客户端没发时反代**不臆造**任何思考字段。
    #[test]
    fn upstream_body_passes_through_reasoning_fields() {
        let base = json!({
            "model": "deepseek-v4-pro",
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let body = build_upstream_body(&base, Edition::Domestic, &mut Vec::new());
        for key in REASONING_PASSTHROUGH {
            assert!(
                body.get(*key).is_none(),
                "客户端没发 {key}，反代不该补：{body}"
            );
        }

        let incoming = json!({
            "model": "deepseek-v4-pro",
            "reasoning_effort": "high",
            "thinking": { "type": "enabled" },
            "enable_thinking": true,
            "chat_template_kwargs": { "enable_thinking": true },
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let body = build_upstream_body(&incoming, Edition::Domestic, &mut Vec::new());
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["enable_thinking"], true);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], true);
    }

    /// 国际版必须给「首条不是 system」的请求补一条 —— 不补会被上游整个拦掉
    /// （实测 `11128 first message is not system prompt`）。国内版不补、也不该补。
    #[test]
    fn international_gets_leading_system() {
        let incoming = json!({
            "model": "deepseek-v4.1-flash",
            "messages": [{ "role": "user", "content": "hi" }]
        });

        let mut degraded = Vec::new();
        let intl = build_upstream_body(&incoming, Edition::International, &mut degraded);
        assert_eq!(intl["messages"][0]["role"], "system", "国际版要兜一条 system");
        assert_eq!(intl["messages"][1]["role"], "user", "原来的消息要原样跟在后头");
        assert_eq!(intl["messages"].as_array().unwrap().len(), 2);
        assert!(degraded.contains(&"leading-system".to_string()), "要留痕");

        // 已经有 system 就不要再插
        let with_sys = json!({
            "model": "deepseek-v4.1-flash",
            "messages": [
                { "role": "system", "content": "s" },
                { "role": "user", "content": "hi" }
            ]
        });
        let body2 = build_upstream_body(&with_sys, Edition::International, &mut Vec::new());
        assert_eq!(body2["messages"].as_array().unwrap().len(), 2, "不该重复插");
        assert_eq!(body2["messages"][0]["content"], "s", "原有的 system 要保留");

        // 国内版：一行都不许改
        let dom = build_upstream_body(&incoming, Edition::Domestic, &mut Vec::new());
        assert_eq!(dom["messages"][0]["role"], "user");
        assert_eq!(dom["messages"].as_array().unwrap().len(), 1);
    }

    /// 上游 `data.agents[].models[]` 形态。
    #[test]
    fn parse_model_ids_reads_agents() {
        let v = json!({
            "code": 0,
            "data": { "agents": [
                { "name": "a", "models": ["glm-5.3", "kimi-k3-1"] },
                { "name": "b", "models": ["glm-5.3", "deepseek-v4-pro"] }
            ]}
        });
        // 去重且保序
        assert_eq!(parse_model_ids_raw(&v), vec!["glm-5.3", "kimi-k3-1", "deepseek-v4-pro"]);
    }

    /// 上游的另一种形态：`{data:[{id}]}`（OpenAI 风格）。
    #[test]
    fn parse_model_ids_reads_openai_list() {
        let v = json!({ "object": "list", "data": [ {"id": "auto"}, {"id": "glm-5.3"} ] });
        assert_eq!(parse_model_ids_raw(&v), vec!["auto", "glm-5.3"]);
    }

    /// 拉不到就是空 —— 不再退回内置兜底（那会让客户端以为有这些模型，
    /// 实际发过去照样 404，不如让调用方把失败原因报出来）。
    #[test]
    fn parse_model_ids_raw_returns_empty_when_nothing() {
        assert!(parse_model_ids_raw(&json!({})).is_empty());
        assert!(parse_model_ids_raw(&json!({"data": {"agents": []}})).is_empty());
    }
}
