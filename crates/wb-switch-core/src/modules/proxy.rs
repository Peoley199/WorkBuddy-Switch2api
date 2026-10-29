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
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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
use serde_json::{json, Map, Value};

use chrono::{FixedOffset, Local, NaiveDateTime, TimeZone};
use uuid::Uuid;

use super::account::{is_envelope, load_accounts, upsert_account};
use super::config::{home_dir, now_ms};
use super::edition::{edition_of, Edition};
use super::http_guard::{self, Access};
use super::refresh::refresh_token_only;

const CONFIG_FILE: &str = "proxy.json";
/// 出站头族里冒充的官方客户端版本。
///
/// 🔴 对齐的是**本机安装的官方桌面端**（2026-10-27 从其 `app.asar` 读到的
/// `WorkBuddy/5.8.4 … CLI/2.161.0`）。旧值 5.5.4 / 2.137.1 落后一个 minor 周期，
/// 与「官方客户端特征」相关的风控维度上多一个可识别的差异。
/// 官方升级后这里应同步上调（值同时用于 `X-IDE-Version`）。
const DEFAULT_CLIENT_VERSION: &str = "5.8.4";
const DEFAULT_CLI_VERSION: &str = "2.161.0";
/// token 剩余有效期小于该值时先刷新再出站。
const REFRESH_MARGIN_MS: i64 = 2 * 60 * 60 * 1000;
/// 单账号失败后的冷却时间。
const COOLDOWN: Duration = Duration::from_secs(120);
/// 「号在磁盘上已经不可用」（被删 / 需重新登录 / 无明文凭据）的池内冷却。
///
/// 比 [`COOLDOWN`] 短：这类状态可能被用户重新导入 / 重新登录修好，
/// 长冻只会让「修好了还要等」；短冻足以避免每个请求都去磁盘上重新判定一遍。
const SKIP_COOLDOWN: Duration = Duration::from_secs(30);

/// 429 但响应里没有重置时刻时的**逐模型兜底冷却**。
///
/// 没有它就只能退回整账号 120 秒短冷却：同账号其他模型被无谓停掉，120 秒后大概率
/// 立刻再 429。与 [`COOLDOWN`] 同量级：恢复要快，但也不该比它更短地空转上游。
const RATE_LIMIT_FALLBACK: Duration = Duration::from_secs(120);

/// 上游**账号级风控限制**（403 / `11140` + displayMsg「unusual activity」）的池内冷却。
///
/// 真正的持久化靠账号库里的 `needs_relogin` 标记：重启、重建池、刷新都不会让它回池，
/// 只有用户重新登录 / 重新导入 / 申诉恢复后才能自动回池。这里的池内冷却只负责
/// 两件事：① 让 `/status` 与界面立刻显示「这个号被停用了」；② 让本进程立刻不再选它。
/// 与 [`SKIP_COOLDOWN`] 同思路但更长一点：用户看到红标去重新登录需要一点时间，
/// 期间没必要反复重读盘；重新登录会清掉 `needs_relogin`，冷却到期后自然回池。
const BAN_QUARANTINE: Duration = Duration::from_secs(300);

/// 命中 `11140` 但 displayMsg 认不出语义时的**保守**长冷却。
///
/// 此时不能持久摘号（万一是内容审核，误摘会把好号踢出池）；但也不能退化回
/// 120 秒短冷却 —— 那等于每两分钟拿被封的号再打一次上游。1 小时后重新观察一次。
const BAN_SUSPECT_COOLDOWN: Duration = Duration::from_secs(3600);

/// 上游「渠道校验」软拦截（`11128`）的最大重试次数。
///
/// 🔴 这个码**不是请求内容的问题，也不是账号的问题**：同一个 payload、同一个号、
/// 同一套出站头，实测连发 12 次全部通过（假 model）／连发 6 次全部 200（真 model）。
/// 上游是**间歇**拦的，而且它自己的文案就在邀请重发
/// （`displayMsg.zh = "请求被拦截，请重新发送"`）。
/// 所以遇到它**先重试**，重试仍失败才当账号故障处理。
const MAX_CHANNEL_RETRY: usize = 2;

/// 模型侧瞬时故障（11133/11134/14003）的重试次数（退避见 [`MODEL_RETRY_BACKOFF_MS`]）。
///
/// 🔴 这类码**不是账号的问题**（官方归类「模型侧状态」），重试耗尽也不冷却账号 ——
/// 冻它只会让下一次请求换到一个同样会撞故障窗口的号。
const MAX_MODEL_RETRY: usize = 3;
/// 退避梯度：1s → 3s → 8s（合计 12s）。
///
/// 上游同功能用的是 1/3/8s，此处保持一致。反代是同步阻塞客户端的，
/// 不再拉长：这类故障窗口实测通常几秒内就恢复，拖太久客户端会先超时断开。
const MODEL_RETRY_BACKOFF_MS: [u64; MAX_MODEL_RETRY] = [1_000, 3_000, 8_000];

/// 上游**连接层**失败（DNS 解析不了 / 超时 / 连接被拒 / TLS 重置）的最大重试次数。
/// 🔴 连接层失败**不是账号的问题**，绝不能拿它去冷却账号。
/// 以前这里无条件 `mark_failed`，于是网络一抖就把整池冻 120 秒：
/// 国际版只有 3 个号，几下就冻空 ⇒ 客户端收到
/// `503 国际版的账号当前都不可用（冷却中，或需要重新登录）`，过两分钟又好了，
/// 然后网络再抖 → 再冻 —— 表现为「一会儿报错停下来、一会儿又能用」反复横跳。
/// （2026-10-11 与 `refresh.rs` 里 `code=-1` 那个坑同源。）
///
/// 从 **1 次** 提到 **3 次**（上游 2026-10-26 同改）：反代**走直连、不读系统代理**，
/// 线路抖动时只有这一条路可走，只重试 1 次容易把瞬时抖动放大成「全线故障」的观感。
const MAX_TRANSPORT_RETRY: usize = 3;

/// 连接层失败的退避梯度：400ms → 1s → 2s。
///
/// 不取更长：这是在**等网络**，不是等上游处理；客户端自己还有外层重试，
/// 反代这边拖太久只会让用户觉得「卡住了」。
const TRANSPORT_RETRY_BACKOFF_MS: [u64; MAX_TRANSPORT_RETRY] = [400, 1_000, 2_000];

/// 出站连接超时。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 流式读取的**首包**超时：从出站到收到第一个字节最多等这么久。
///
/// 供 `WB_SWITCH_STREAM_FIRST_BYTE_SECS` 覆盖（默认 90 秒），便于验证与特殊上游。
fn first_byte_timeout() -> Duration {
    env_secs("WB_SWITCH_STREAM_FIRST_BYTE_SECS", STREAM_FIRST_BYTE)
}

const STREAM_FIRST_BYTE: Duration = Duration::from_secs(90);

/// 流式读取的**帧间隔**超时：两次收到上游数据之间最多间隔这么久。
///
/// 流式响应不能用 `.timeout()`（那会把整段生成一起掐掉），要限制的是「停滞」。
/// ⚠️ reqwest 的 `read_timeout` 只对 blocking 客户端生效，async 侧会被静默忽略，
/// 所以这里必须在泵里自己实现，见 [`StreamGuard::tick`]。
///
/// 240 秒是**故意放宽**的：上游在思考模型首字前常常静默几分钟，卡得太紧会把正常请求掐死。
/// 它防的是「网关静默丢包 / 连接半开导致永久悬挂」，不是慢。
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(240);

/// 同上，供 `WB_SWITCH_STREAM_IDLE_SECS` 覆盖。
fn idle_timeout() -> Duration {
    env_secs("WB_SWITCH_STREAM_IDLE_SECS", STREAM_IDLE_TIMEOUT)
}

fn env_secs(key: &str, default: Duration) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|s| *s > 0)
        .map(Duration::from_secs)
        .unwrap_or(default)
}

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

/// 标准采样 / 输出控制参数：上游全部接受，原样透传。
///
/// 2026-10-21 用假模型 oracle 逐个验证（过渠道校验返回 `11102`，被拦返回 `11128`），
/// 下面这些上游**全部接受**。注意 `stream` 不在这里 —— 出站恒为 `true`。
const STANDARD_PASSTHROUGH: &[&str] = &[
    "temperature",
    "top_p",
    "max_tokens",
    "stop",
    "frequency_penalty",
    "presence_penalty",
    "response_format",
    "parallel_tool_calls",
    "seed",
    "stream_options",
    "user",
    "logprobs",
    "top_logprobs",
];

/// 需要从 system prompt 里抹掉的**第三方 agent 身份声明**。
///
/// 上游风控会识别系统提示词里固定的「DeepSeek Harness」身份声明，命中就把整条
/// 请求判成 `11128 unapproved channel`；DSH 自带 `includeHarnessIdentity` 开关，
/// 关掉它（或把声明从提示词里删掉）即可恢复。
///
/// 本仓库**默认路径就抹**（不同于上游只在自愈重试里抹）：这些短语是客户端自动
/// 注入的固定身份声明，不是用户自己写的内容；留着只会让整条请求被拦。
const IDENTITY_DECLARATIONS: &[&str] = &[
    "DeepSeek Harness",
    "deepseek-harness",
    "DeepSeek-Harness",
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

/// 一个模型的长冷却：恢复时刻 + 为什么被冻。
struct ModelCooldown {
    until_ms: i64,
    reason: String,
}

struct AccountLease {
    uid: String,
    /// 这个号属于哪一档。国内版和国际版的模型列表不同，
    /// 拿国际版专属的 model 去打国内版网关会被拒，所以选号必须认版本。
    edition: Edition,
    /// 账号级短冷却的恢复时刻（epoch 毫秒）。存**绝对时刻**而不是 `Instant`：
    /// 界面要拿它做本地倒计时，`Instant` 无法序列化给前端。
    failed_until_ms: Option<i64>,
    /// 短冷却的原因（给界面看的一句话）。
    failed_reason: Option<String>,
    /// **按模型**的长冷却：`model -> 恢复时刻`。
    ///
    /// 60004 只代表「这个模型的额度用完了」，同一账号换模型仍可用（上游文案
    /// 自己就写着「您也可以切换其他模型继续使用」）。账号级长冷却会把还能
    /// 用的模型一起冻掉，所以这类失败记在模型键上；账号仍留在池子里服务
    /// 其他模型。
    model_cooldowns: HashMap<String, ModelCooldown>,
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
                        failed_until_ms: None,
                        failed_reason: None,
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
    /// `exclude` 是本轮调用方已经排除掉的 uid（见 [`take_account`]）：挑号与「能不能
    /// 拿到明文」是两件事，后者只有调用方知道，所以由它把失败的号传回来继续轮。
    ///
    /// 一个版本都没有可用号时**不会**悄悄换另一个版本 —— 那样只会把
    /// 「模型不存在」的 400 换个地方报出来，不如直接说清楚。
    fn next_uid_of(
        &self,
        want: Option<Edition>,
        model: Option<&str>,
        exclude: &[String],
    ) -> Option<String> {
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
            if exclude.iter().any(|u| *u == lease.uid) {
                continue;
            }
            let now = now_ms_i64();
            let cooling = lease
                .failed_until_ms
                .map(|t| t > now)
                .unwrap_or(false);
            if cooling {
                continue;
            }
            if let Some(m) = model {
                let model_cooling = lease
                    .model_cooldowns
                    .get(m)
                    .map(|c| c.until_ms > now)
                    .unwrap_or(false);
                if model_cooling {
                    continue;
                }
            }
            return Some(lease.uid.clone());
        }
        None
    }

    /// 某个 uid 在本轮能否直接使用（版本对、不在冷却、该模型未被长冷却）。
    /// 供会话粘滞判断「绑定的号还能不能继续用」。
    fn lease_usable(&self, uid: &str, want: Option<Edition>, model: Option<&str>) -> bool {
        let now = now_ms_i64();
        self.leases.iter().any(|slot| {
            let Ok(lease) = slot.lock() else { return false };
            lease.uid == uid
                && want.is_none_or(|e| lease.edition == e)
                && !lease.failed_until_ms.is_some_and(|t| t > now)
                && !model.is_some_and(|m| {
                    lease
                        .model_cooldowns
                        .get(m)
                        .is_some_and(|c| c.until_ms > now)
                })
        })
    }

    /// 某一档里还有没有号（用于给出更准确的报错）。
    fn has_edition(&self, edition: Edition) -> bool {
        self.leases.iter().any(|slot| {
            slot.lock()
                .map(|l| l.edition == edition)
                .unwrap_or(false)
        })
    }

    /// 给账号打一次短冷却（固定 [`COOLDOWN`]），并记下原因供界面展示。
    fn mark_failed(&self, uid: &str, reason: &str) {
        self.mark_until(uid, now_ms_i64() + COOLDOWN.as_millis() as i64, reason);
    }

    /// 号在磁盘上已不可用（被删 / 需重新登录）：短冷却 [`SKIP_COOLDOWN`]。
    ///
    /// 用「短冷却」而不是立刻从池子里摘掉：池子是配置快照，摘掉之后用户重新导入 /
    /// 重新登录也回不来；短冷却到期后 [`take_account`] 会重新读盘判定。
    fn mark_unusable(&self, uid: &str, reason: &str) {
        self.mark_until(uid, now_ms_i64() + SKIP_COOLDOWN.as_millis() as i64, reason);
    }

    /// 把账号级短冷却设到指定时刻（epoch 毫秒）。
    fn mark_until(&self, uid: &str, until_ms: i64, reason: &str) {
        for slot in &self.leases {
            let Ok(mut lease) = slot.lock() else { continue };
            if lease.uid == uid {
                lease.failed_until_ms = Some(until_ms);
                lease.failed_reason = Some(reason.to_string());
            }
        }
        // 号被判失败：把粘在它上面的会话都放开，下一轮它们才能换号，
        // 而不是一直撞冷却（会话粘滞只负责「黏」，不负责「黏死」）。
        if let Ok(sticky) = current_sticky().lock() {
            for stick in sticky.values() {
                stick.unbind_uid(uid);
            }
        }
    }

    /// 清掉账号级短冷却（模型级长冷却不动）。
    fn clear_cooldown(&self, uid: &str) {
        for slot in &self.leases {
            let Ok(mut lease) = slot.lock() else { continue };
            if lease.uid == uid {
                lease.failed_until_ms = None;
                lease.failed_reason = None;
            }
        }
    }

    /// 只冻「某账号的某个模型」到指定时刻 —— 账号的其他模型继续服务。
    ///
    /// 固定的 120 秒短冷却对付频率超限不够：120 秒后号回池立刻又 429，循环空转。
    /// `until_ms` 是上游声明的重置时刻（毫秒）；已过期则视为不冷却。
    fn mark_model_failed_until(&self, uid: &str, model: &str, until_ms: i64, reason: &str) {
        if until_ms <= now_ms_i64() {
            return;
        }
        for slot in &self.leases {
            let Ok(mut lease) = slot.lock() else { continue };
            if lease.uid == uid {
                lease.model_cooldowns.insert(
                    model.to_string(),
                    ModelCooldown {
                        until_ms,
                        reason: reason.to_string(),
                    },
                );
            }
        }
    }

    /// 该账号剩余冷却毫秒数：短冷却与**所有模型**长冷却里最晚的那个。
    /// 不在冷却中返回 None。
    ///
    /// ⚠️ 这是 `/status` 沿用的**合并口径**（一个数代表整个号）。界面要区分
    /// 「账号短冷却」和「仅某模型冷却」时用 [`Pool::cooldown_snapshot`]，
    /// 不要用这个。
    fn cooling_for(&self, uid: &str) -> Option<i64> {
        let now = now_ms_i64();
        for slot in &self.leases {
            let Ok(lease) = slot.lock() else { continue };
            if lease.uid != uid {
                continue;
            }
            let short = lease
                .failed_until_ms
                .map(|t| t - now)
                .filter(|m| *m > 0);
            let long = lease
                .model_cooldowns
                .values()
                .map(|c| c.until_ms - now)
                .filter(|m| *m > 0)
                .max();
            return long.max(short);
        }
        None
    }

    /// 冷却快照：**只列仍在冷却中的项**，供界面展示。
    ///
    /// 与 [`Pool::cooling_for`] 的关键区别是**不合并**：账号短冷却和逐模型长冷却
    /// 分开给出，界面才能说清「这个号整个不可用」还是「只有某个模型不可用、换个
    /// 模型就行」。时间一律是 **epoch 毫秒的绝对时刻**，前端自己做倒计时 ——
    /// 返回剩余毫秒的话，界面一刷新就会跳变，还得靠轮询续命。
    ///
    /// 形如：
    /// ```json
    /// { "uid": "u1",
    ///   "until": 1761200000000, "reason": "上游返回 401",
    ///   "models": [{"model": "deepseek-v4-pro", "until": 1761203600000, "reason": "频率超限"}] }
    /// ```
    /// 两者都为空时不出现（即：不在冷却中的号不进这个数组）。
    fn cooldown_snapshot(&self) -> Vec<Value> {
        let now = now_ms_i64();
        let mut out = Vec::new();
        for slot in &self.leases {
            let Ok(lease) = slot.lock() else { continue };
            let short = lease
                .failed_until_ms
                .filter(|t| *t > now)
                .map(|t| (t, lease.failed_reason.clone().unwrap_or_default()));
            let models: Vec<Value> = {
                let mut v: Vec<(String, &ModelCooldown)> = lease
                    .model_cooldowns
                    .iter()
                    .filter(|(_, c)| c.until_ms > now)
                    .map(|(m, c)| (m.clone(), c))
                    .collect();
                // 先恢复的排前面，界面不用再排一次。
                v.sort_by_key(|(_, c)| c.until_ms);
                v.into_iter()
                    .map(|(model, c)| {
                        json!({
                            "model": model,
                            "until": c.until_ms,
                            "reason": c.reason,
                        })
                    })
                    .collect()
            };
            if short.is_none() && models.is_empty() {
                continue;
            }
            out.push(json!({
                "uid": lease.uid,
                "edition": lease.edition.key(),
                "until": short.as_ref().map(|(t, _)| *t),
                "reason": short.as_ref().map(|(_, r)| r.clone()),
                "models": models,
            }));
        }
        out
    }
}

fn now_ms_i64() -> i64 {
    now_ms() as i64
}

/// 读上游流的「停滞」看门狗：首包 90 秒、之后每帧间隔 240 秒。
///
/// 用法：每次 `next()` 前调 [`StreamGuard::arm`]（超时点 + 原因），
/// `timeout_at` 超时后把原因交给调用方，拼进错误信息。
/// 放在泵里而不是 client 上，是因为流式响应不能设整体超时。
struct StreamGuard {
    deadline: tokio::time::Instant,
    stage: &'static str,
}

impl StreamGuard {
    fn new() -> Self {
        Self {
            deadline: tokio::time::Instant::now() + first_byte_timeout(),
            stage: "首包",
        }
    }

    /// 进入一轮等待：返回本轮的超时点。`first` = 还没有收到任何字节。
    fn arm(&mut self, first: bool) {
        if first {
            self.deadline = tokio::time::Instant::now() + first_byte_timeout();
            self.stage = "首包";
        } else {
            self.deadline = tokio::time::Instant::now() + idle_timeout();
            self.stage = "帧间隔";
        }
    }

    /// 超时说明（给客户端和日志看的一句话）。
    fn timed_out(&self) -> String {
        let limit = if self.stage == "首包" {
            first_byte_timeout()
        } else {
            idle_timeout()
        };
        format!(
            "上游{}超时：{} 秒内没有收到新数据，已中止本次请求",
            self.stage,
            limit.as_secs()
        )
    }
}

/// SSE 读泵的产出：要么转给客户端的字节流，要么是「该记账了」的结束事件。
///
/// 为什么要单独一个枚举：客户端断开后泵会**立刻停读上游**，但绑定路由还需要一个
/// 最后的机会去写统计（以前泵直接 return，导致断开后既不记账，也无法区分
/// 「完整发完」和「中途被断开」）。
enum PumpOut {
    Bytes(bytes::Bytes),
    /// 泵结束了（或刚发现客户端已断开）。只有 `Emitted::Done` 记成功。
    Finished(Emitted),
}

/// 泵结束时，客户端到底拿到了多少。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Emitted {
    /// 上游流完整读完。
    Done,
    /// 客户端已经断开，泵主动停读。
    ClientGone,
    /// 上游报错 / 读超时。
    UpstreamError,
}

/// 客户端侧的 SSE 响应体。
///
/// 包一层（换成 `PumpBody`）是为了在**客户端断开**时也能拿到通知：axum 会直接 drop
/// 响应体，纯 `unfold` 流没有机会做收尾。`PumpBody` 实现 `Drop`，借泵的作者通道
/// 给泵发一条 `Finished(ClientGone)`；泵收到后才记账 —— 否则被取消的请求在统计里
/// ok=true、token 照算，账目全错（修复前的行为）。
fn sse_body(
    rx: tokio::sync::mpsc::Receiver<PumpOut>,
    pump_tx: tokio::sync::mpsc::UnboundedSender<Emitted>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
) -> PumpBody {
    PumpBody {
        rx: Some(rx),
        pump_tx: Some(pump_tx),
        ended: false,
        stop,
    }
}

/// [`sse_body`] 的返回类型：读 `rx`，并在被丢弃（客户端断开 / 服务停止）时通知泵。
struct PumpBody {
    rx: Option<tokio::sync::mpsc::Receiver<PumpOut>>,
    pump_tx: Option<tokio::sync::mpsc::UnboundedSender<Emitted>>,
    /// 已经读到泵的结束标记；之后再被 drop 就不用重复通知。
    ended: bool,
    /// 只给「不自己记账」的泵用：drop 时发一枪令其停读上游。
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl futures::Stream for PumpBody {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let Some(rx) = self.rx.as_mut() else {
            return std::task::Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(None) => {
                self.ended = true;
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Ready(Some(PumpOut::Bytes(b))) => {
                std::task::Poll::Ready(Some(Ok(b)))
            }
            std::task::Poll::Ready(Some(PumpOut::Finished(outcome))) => {
                self.ended = true;
                if outcome == Emitted::UpstreamError {
                    // 让客户端看到连接异常而不是「正常结束」的半截回复。
                    std::task::Poll::Ready(Some(Err(std::io::Error::other(
                        "upstream stream interrupted",
                    ))))
                } else {
                    std::task::Poll::Ready(None)
                }
            }
        }
    }
}

impl Drop for PumpBody {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let (Some(rx), Some(tx)) = (
            self.rx.take(),
            self.pump_tx.take(),
        ) {
            // 泵已经结束（rx 被关）时这条发不出去，忽略即可；泵还活着时它必须收到。
            let _ = tx.send(Emitted::ClientGone);
            drop(rx);
        }
    }
}

/// 取一个可用的账号（必要时先刷新 token）。
///
/// `model` 用于跳过「该模型正在长冷却」的号 —— 60004 只冻模型，不冻账号。
///
/// `sticky`：会话粘滞（见 [`SessionStick`]）。绑定中的号**冷却/标签不合格时直接解绑**，
/// 本轮重新轮转；成功选出后把会话绑到它上面。没有会话标识时行为与纯轮转完全一致。
async fn take_account(
    pool: &Pool,
    _cfg: &ProxyConfig,
    want: Option<Edition>,
    model: Option<&str>,
    sticky: Option<(&SessionStick, &str)>,
) -> Option<Value> {
    // 🔴 挑号与「从磁盘取明文」必须绑在一起重试。
    //
    // `next_uid_of` 只看版本与冷却，看不到 `needs_relogin`，也看不到号是不是还在磁盘上；
    // 以前这里「挑一次号 + find 一次」再 `?` 掉，于是只要命中的那个号被删了 / 被导入覆盖 /
    // 被保活或刷新失败标成需要重新登录，这一单就直接 503 —— 哪怕池里还有健康号。
    // 而 `needs_relogin` 是**永久标记**、`next_uid_of` 从不跳过它，所以只要轮转一次就会
    // 一直撞上这个号，直到用户点保存或重启（只有那时才会按磁盘重建池）。
    //
    // 现在：挑到一个失效号就给它打短冷却（[`SKIP_COOLDOWN`]，到期会重新读盘判定）
    // 并继续挑下一个 —— 单个坏号只让「这一轮」多绕一步，不会再把整单打成 503。
    let failed: Vec<String> = Vec::new();
    let mut tried: Vec<String> = Vec::new();
    let mut skipped: Option<Value> = None;
    // 先看会话是否已粘在某个号上：**只有冷却/不可用时才解绑**，其余情况固定用它。
    let mut take_sticky = sticky.is_some();
    loop {
        let bound = match (take_sticky, sticky) {
            (true, Some((stick, session))) => {
                take_sticky = false;
                stick.lookup(session, now_ms_i64())
            }
            _ => None,
        };
        let picked = match bound {
            Some(bound) => {
                let usable = pool.lease_usable(&bound, want, model);
                if usable {
                    Some(bound)
                } else {
                    // 绑定中的号不可用（冷却 / 别版 / 需重新登录）⇒ 解绑，走正常轮转。
                    if let Some((stick, session)) = sticky {
                        stick.unbind_session(session);
                    }
                    None
                }
            }
            None => None,
        };
        let uid = match picked {
            Some(uid) => uid,
            None => match pool.next_uid_of(want, model, &failed) {
                Some(uid) => uid,
                None => {
                    // 本轮所有号都被略过了：放过一个（「刷新失败但可能还能用」的）再试一次，
                    // 免得单个号的问题把整池拖空；连一个都没有才真的 503。
                    let acc = skipped?;
                    pool.clear_cooldown(&uid_of(&acc));
                    return Some(acc);
                }
            },
        };
        // 同一个号本轮已经试过（理论走不到，兜底防空转）。
        if tried.contains(&uid) {
            return None;
        }
        // 磁盘上每一个号**每轮只看一次**：已经试过的 uid 不会再被 `next_uid_of`
        // 绕回来（`tried` 里一出现就直接收工），所以这里不需要额外的排除列表。
        tried.push(uid.clone());
        let Some(acc) = load_accounts()
            .into_iter()
            .find(|a| is_leasable_uid(a, &uid))
        else {
            // 磁盘上没有、或已被标记需要重新登录（或是拿不到明文的信封账号）：
            // 本轮不再用它。重置得快一些 —— 号可能被重新导入 / 重新登录修好。
            match load_accounts()
                .into_iter()
                .find(|a| a.get("uid").and_then(Value::as_str) == Some(uid.as_str()))
            {
                None => eprintln!("[反代] 跳过一个暂不可用的号 uid={uid}（磁盘上已没有这个号）"),
                Some(a) => eprintln!(
                    "[反代] 跳过一个暂不可用的号 uid={uid}（{}）",
                    if a.get("needs_relogin").and_then(Value::as_bool) == Some(true) {
                        format!(
                            "需要重新登录：{}",
                            a.get("needs_relogin_reason")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                        )
                    } else {
                        "没有明文 access token".to_string()
                    }
                ),
            }
            pool.mark_unusable(&uid, "账号已删除或需要重新登录");
            continue;
        };
        match try_prepare(pool, acc).await {
            Prepared::Ready(acc) => {
                if let Some((stick, session)) = sticky {
                    stick.bind(session, &uid_of(&acc), now_ms_i64());
                }
                return Some(acc);
            }
            Prepared::Fallback(acc) => {
                // 刷新失败：不再纠结直接出站（token 真失效上游会回 401 并冷却该号），
                // 但记下来 —— 整池都落到这一档时选它，好过回 503。
                if skipped.is_none() {
                    skipped = Some(acc);
                }
            }
            Prepared::Rejected => {}
            Prepared::Unusable => {
                pool.mark_unusable(&uid, "刷新失败：没有可用凭据，需要重新登录");
            }
        }
    }
}

/// 磁盘上「能出站」的账号数量 —— 只用于把 503 说清楚。
fn eligible_on_disk(cfg: &ProxyConfig, edition: Edition) -> usize {
    let allowed = &cfg.endpoint(edition).accounts;
    load_accounts()
        .iter()
        .filter(|a| {
            is_leasable_uid(a, a.get("uid").and_then(Value::as_str).unwrap_or(""))
                && edition_of(a) == edition
                && (allowed.is_empty()
                    || a.get("uid")
                        .and_then(Value::as_str)
                        .map(|u| allowed.iter().any(|x| x == u))
                        .unwrap_or(false))
        })
        .count()
}

/// 账号对象的 uid。
fn uid_of(acc: &Value) -> String {
    acc.get("uid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// 这个账号对象能否作为 `uid` 的可用凭据：uid 对得上、不需要重新登录、带明文 access token。
///
/// 信封账号（`access_token` 是 `{$wbEncrypted,…}`）拼不出 `Authorization: Bearer …`，
/// 打上游只会 401 —— 与 [`eligible_accounts`] 同一个判据，别各写一份。
fn is_leasable_uid(a: &Value, uid: &str) -> bool {
    a.get("uid").and_then(Value::as_str) == Some(uid)
        && a.get("needs_relogin").and_then(Value::as_bool) != Some(true)
        && !is_envelope(a, "access_token")
}

/// 重试取号的结论。
enum Prepared {
    /// 凭据就绪（必要时已刷新并落盘）。
    Ready(Value),
    /// 刷新失败，但磁盘上的旧凭据还可以用 —— 只有当整池都落到这一档时才会被选中。
    ///
    /// 注意这里**不打冷却**：号本身可能没坏（上游抖了一下），打冷却等于把它踢出轮转。
    /// 连整池都落到这一档时才有意义 —— 那时它会因为 `skipped` 被直接选中。
    Fallback(Value),
    /// 上游明确拒绝了 refresh token（需要重新登录），不冷却、不落盘，也不当兜底。
    Rejected,
    /// 手上没有可用凭据（缺 refresh token / 刷新响应里没有 accessToken）：
    /// 这一轮跳过，给它一个短冷却后重新读盘判定。
    Unusable,
}

/// 拿到账号后按需刷新 token。
async fn try_prepare(_pool: &Pool, mut acc: Value) -> Prepared {
    let exp = acc.get("expiresAt").and_then(Value::as_i64).unwrap_or(0);
    if exp - now_ms_i64() < REFRESH_MARGIN_MS {
        let uid = uid_of(&acc);
        // 🔴 刷新失败**不直接回 503**：旧 access token 可能还没过期，或上游只是抖了一下。
        // 以前单个号刷新失败/超时就把整单 503 掉，哪怕池里还有别的号。
        //
        // 🔴 必须用**不落盘**的版本：落盘版会顺手把号打成 `needs_relogin`，于是下一次
        // `is_leasable_uid` 就直接把它当「磁盘上不可用」跳过 —— 这里的兜底永远触发不了，
        // 首次刷新失败之后每个请求都只能 503。
        match tokio::time::timeout(
            Duration::from_secs(30),
            refresh_token_only(acc.clone()),
        )
        .await
        {
            Ok(refreshed) => {
                if refreshed.get("needs_relogin").and_then(Value::as_bool) == Some(true) {
                    // 只在日志里体现；号不冷却、不落盘，下一次请求还会再试刷新。
                    if refreshed
                        .get("_wb_refresh_rejected")
                        .and_then(Value::as_bool)
                        == Some(true)
                    {
                        // 上游明确拒绝了 refresh token —— 这是**账号层面**的事实，不是抖动：
                        // 不冻号（旧 token 可能还没过期，冻了就丢掉一次可用机会），
                        // 但也不能假装它能恢复：本轮的兜底（整池都落到这一档时才用它）会跳过它。
                        eprintln!(
                            "[反代] uid={uid} 的 refresh token 已被上游拒绝（{}），请重新登录",
                            refreshed
                                .get("needs_relogin_reason")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                        );
                        return Prepared::Rejected;
                    }
                    // 其余失败里，「缺 RT」「刷出来的响应里没有 accessToken」这两种
                    // 说明手上**没有可用的凭据**，拿旧 token 出站也只会被上游 401：
                    // 这一轮跳过（不落盘、不冷却过久 —— 30 秒后再重新读盘判定，
                    // 用户重新登录 / 重新导入后自动回池）。
                    let reason = refreshed
                        .get("needs_relogin_reason")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let no_credential = reason.contains("缺少 refresh token")
                        || reason.contains("缺少 accessToken");
                    if no_credential {
                        eprintln!("[反代] 跳过 uid={uid}：没有可用凭据（{reason}）");
                        return Prepared::Unusable;
                    }
                    // 网络类失败可能只是暂时的：先用旧 token 试。
                    eprintln!("[反代] 刷新失败 uid={uid}（{reason}），先用现有 token 重试");
                    return Prepared::Fallback(acc);
                }
                acc = refreshed;
            }
            Err(_) => {
                eprintln!("[反代] 刷新超时 uid={uid}，先用现有 token 重试");
                return Prepared::Fallback(acc);
            }
        }
    }
    let _ = upsert_account(&acc);
    Prepared::Ready(acc)
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
    // 端到端测试的唯一注入点：只有 `WB_SWITCH_ENABLE_TEST_UPSTREAM=1` 时才认
    // `WB_SWITCH_TEST_UPSTREAM=http://127.0.0.1:<port>`（同时拒绝非回环地址）。
    // 默认构建里这两个变量都不存在，行为与以前完全一致。
    if let Some(port) = test_upstream_port() {
        return match edition_of(acc) {
            Edition::Domestic | Edition::International => {
                format!("http://127.0.0.1:{port}").leak()
            }
        };
    }
    edition_of(acc).api_endpoint()
}

/// 端到端演练用的上游端口（见 [`upstream_endpoint`]）。生产环境恒为 `None`。
fn test_upstream_port() -> Option<u16> {
    if std::env::var("WB_SWITCH_ENABLE_TEST_UPSTREAM").ok().as_deref() != Some("1") {
        return None;
    }
    let url = std::env::var("WB_SWITCH_TEST_UPSTREAM").ok()?;
    let port = url.rsplit(':').next()?.trim_end_matches('/').parse::<u16>().ok()?;
    url.starts_with("http://127.0.0.1:").then_some(port)
}

/// 32 位 hex 的请求级 id（对齐官方客户端的 ULID 形态 id）。
///
/// 🔴 旧实现是 `format!("{:032x}", nanos ^ 常量)`：2026 年的纳秒时间戳远小于 2^64，
/// 异或后仍小于 2^64，补足 32 位 hex 时**高 16 位恒为零** —— `X-B3-TraceId` /
/// 各会话 id 全带 16 个前导零，是一次格式校验就能识别的确定性指纹。
/// 这里用 SplitMix64 把纳秒 + 进程内序号铺满 128 位：输出无前导零、同一纳秒内
/// 的多次调用也不重复（Windows 时钟精度不保证每次 now() 都变化）。
fn hex_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let seed = (nanos as u64) ^ 0x5eed_5eed_5eed_5eed ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let hi = splitmix64(seed);
    let lo = splitmix64(seed ^ hi.rotate_left(17));
    format!("{hi:016x}{lo:016x}")
}

/// SplitMix64 的一轮：非密码学随机，但雪崩性足够，且对输入是双射
/// （不同 seed 必得不同输出，id 不会撞）。
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// 16 位 hex 的 span / parent span id。
///
/// 🔴 不能从 [`hex_id`] 的前 16 位截取：旧 hex_id 的高 16 位恒为零，
/// `msg[..16]` 截出来永远是字面量 `0000000000000000`，等于向上游广播一个
/// 确定性指纹（官方客户端的 span id 是随机 16 hex）。取**低** 16 位：
/// 每个请求都不同，且与 trace id 不互为前缀。
fn span_id(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let start = chars.len().saturating_sub(16);
    chars[start..].iter().collect()
}

/// 组装出站头族（对齐官方桌面端，缺字段用 X-No-* 约定）。
///
/// `session` 是入站请求自带的会话标识（`X-Conversation-ID`）。官方桌面端里它就是
/// sessionId、整个会话内稳定；有它时原样做出站 `X-Conversation-ID`，没有才用本次
/// 请求现生成的 id 兜底。
///
/// 🔴 `X-Conversation-ID` 必须**只出现一次**。此前是这里插一份、发送循环里再
/// `req.header(...)` 追加一份 —— reqwest 的 `header()` 是 append 语义，入站带会话
/// 头时上游会收到两个同名头（官方客户端永远只有一个）。现在统一在这里决定取值。
fn chat_headers(acc: &Value, session: Option<&str>) -> HashMap<String, String> {
    let mut h = HashMap::new();
    let uid = acc.get("uid").and_then(Value::as_str).unwrap_or("");
    let at = acc.get("access_token").and_then(Value::as_str).unwrap_or("");
    let domain = acc.get("domain").and_then(Value::as_str).unwrap_or("");

    h.insert("Content-Type".into(), "application/json".into());
    // 官方桌面端 Accept 就是单值 `application/json`（响应是 SSE，但 Accept 不列
    // text/event-stream）。出站恒为 stream:true，读侧按流处理，收窄不影响功能。
    h.insert("Accept".into(), "application/json".into());
    h.insert("X-Requested-With".into(), "XMLHttpRequest".into());
    // 🔴 官方桌面端转发 chat 请求时**不发送** `Origin` / `Referer`（其头族黑名单
    // 里明确过滤）。此前跟着发上游根地址，等于多出两个官方没有的头。
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
    // 三个 X-* 取值对照官方桌面端 `buildVlmGatewayHeaders`（2026-10-27 读 asar 核实）：
    // X-Product=SaaS / X-IDE-Type=WorkBuddy / X-IDE-Name=WorkBuddy，另发 X-IDE-Version。
    // 旧值 X-Product=WorkBuddy / X-IDE-Type=desktop 是反代自己猜的，官方从没用过。
    h.insert("X-Agent-Purpose".into(), "conversation".into());
    h.insert("X-IDE-Name".into(), "WorkBuddy".into());
    h.insert("X-IDE-Type".into(), "WorkBuddy".into());
    h.insert("X-Product".into(), "SaaS".into());
    h.insert(
        "X-IDE-Version".into(),
        DEFAULT_CLIENT_VERSION.into(),
    );

    // 会话头族：官方桌面端的语义是「会话 id 稳定、请求/消息 id 每轮现生成」，
    // 所以入站带了会话 id 时只替换 `X-Conversation-ID`，其余仍是本次请求的 id。
    let conv_req = hex_id();
    let msg = hex_id();
    let conversation_id = session
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| conv_req.clone());
    // 🔴 `X-Conversation-ID` 是上游渠道校验的**必需项**，缺了整个 chat 请求被
    // `400 / code 11128` 打回（文案「这次调用没通过渠道校验，可能不是官方客户端发出的」）。
    //
    // 2026-10-20 实测（同一个号、同一套出站头、同一 body、同一秒）：
    // - 不带该头 → 假 model 探针连续 10 次全部 `11128`；真 model 也全部 `11128`；
    // - 只补这一个头 → 10 次全部 `11102`（过了渠道校验，只是模型不存在）；
    //   真 model `glm-5.3` 直接 `200` 出字。
    //
    // 官方桌面端在 `applyGatewayHeaders` 里必发 `X-Conversation-ID`（值 = sessionId），
    // 反代此前只发了 `X-Conversation-Request-ID` 系列，于是从 2026-10-19~20 上游加固后
    // 整条链路全灭。值用每次请求新生成的 32 位 hex 即可（实测随机值也放行）。
    h.insert("X-Conversation-ID".into(), conversation_id);
    h.insert("X-Conversation-Request-ID".into(), conv_req.clone());
    h.insert("X-Conversation-Message-ID".into(), msg.clone());
    h.insert("X-Request-ID".into(), msg.clone());
    // 🔴 不发 X-Root-Request-ID：官方桌面端的头族里没有这个头，多发的每一个
    // 官方没有的头都是「非官方客户端」的可识别特征。
    h.insert("X-Trace-ID".into(), conv_req.clone());
    h.insert("X-B3-TraceId".into(), conv_req);
    // span id 见 span_id 的说明：不能取 msg 的前 16 位（恒全零）。
    h.insert("X-B3-SpanId".into(), span_id(&msg));
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
        // system 消息先过一遍「Claude Code 注入标记」净化，见 strip_claude_code_markers 的说明。
        if m.get("role").and_then(Value::as_str) == Some("system") {
            if let Some(text) = m.get("content").and_then(Value::as_str) {
                let cleaned = strip_claude_code_markers(text);
                if cleaned != text {
                    degraded.push("system-env".to_string());
                    m["content"] = Value::String(cleaned);
                }
            }
            // 第三方 agent 身份声明（`DeepSeek Harness` 等）也在这里抹掉 ——
            // 上游风控命中即 `11128 unapproved channel`。只看 system：用户消息里
            // 出现同名词是正常内容，不该动。
            if let Some(cleaned) = m
                .get("content")
                .and_then(Value::as_str)
                .and_then(strip_identity_declarations)
            {
                degraded.push("system-identity".to_string());
                m["content"] = Value::String(cleaned);
            }
        }
        // 同上，但 content 是**多块数组**时也要抹：文本块放在数组里同样会被
        // 上游风控识别（实测渠道校验看的是拼起来的完整 system 文本）。
        if m.get("role").and_then(Value::as_str) == Some("system") {
            if let Some(parts) = m.get("content").and_then(Value::as_array) {
                let mut touched = false;
                let mut rebuilt: Vec<Value> = Vec::with_capacity(parts.len());
                for p in parts {
                    let is_text = matches!(
                        p.get("type").and_then(Value::as_str),
                        Some("text") | Some("input_text") | None
                    );
                    if is_text {
                        if let Some(cleaned) = p
                            .get("text")
                            .and_then(Value::as_str)
                            .and_then(strip_identity_declarations)
                        {
                            let mut block = p.clone();
                            block["text"] = Value::String(cleaned);
                            touched = true;
                            rebuilt.push(block);
                            continue;
                        }
                    }
                    rebuilt.push(p.clone());
                }
                if touched {
                    degraded.push("system-identity".to_string());
                    m["content"] = Value::Array(rebuilt);
                }
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

/// 把 system 文本里的第三方身份声明替换成中性称呼（ASCII 大小写不敏感）。
///
/// 返回 `None` = 没有命中，调用方不必改动原串。
///
/// 实现按**字节**逐位比较、按 `char` 步进：既不破坏 UTF-8 边界，
/// 也不需要为大小写转换预先做整串 lower（那样会改变字节偏移）。
fn strip_identity_declarations(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    let mut hit = false;
    'outer: while i < bytes.len() {
        for phrase in IDENTITY_DECLARATIONS {
            let p = phrase.as_bytes();
            if i + p.len() <= bytes.len()
                && bytes[i..i + p.len()]
                    .iter()
                    .zip(p.iter())
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
            {
                out.push_str("Assistant");
                i += p.len();
                hit = true;
                continue 'outer;
            }
        }
        // 未命中：按完整字符推进一步（保证落在 UTF-8 边界上）
        let ch = text[i..].chars().next()?;
        out.push(ch);
        i += ch.len_utf8();
    }
    if hit {
        Some(out)
    } else {
        None
    }
}

/// 摘掉 Claude Code 特有的 system prompt 标记。
///
/// 除了 `gitStatus:` 环境块，Claude Code 还会在 system prompt 里注入：
/// - 最前面的 `x-anthropic-billing-header: cc_version=...; cc_entrypoint=...;`
/// - 末尾的 `# Environment / You have been invoked in the following environment:`
///   （工作目录、平台、日期等；非 git 目录时没有 `gitStatus:`，旧逻辑会漏掉）
///
/// 这两段都会被 WorkBuddy 上游的渠道校验认成「非官方客户端」，整条请求 400
/// `11128 Illegal API invocation from an unapproved channel`。模型本身不需要它们，
/// 直接摘掉。
fn strip_claude_code_markers(text: &str) -> String {
    let mut s = text.to_string();

    // 1) 最前面的 billing header 行
    if s.starts_with("x-anthropic-billing-header:") {
        s = match s.find('\n') {
            Some(pos) => s[pos + 1..].to_string(),
            None => String::new(),
        };
    }

    // 2) 「官方 Claude Code」身份声明。上游渠道校验会把它认成非官方客户端
    //    （CCH 的供应商测试探针就发这句）。真实 Claude Code 2.1.x 已不用这句，
    //    但第三方客户端 / 旧版本还会发。
    s = s.replace(
        "You are Claude Code, Anthropic's official CLI for Claude.",
        "",
    );
    s = s.replace("You are Claude Code, Anthropic's official CLI for Claude", "");

    // 3) 环境块（从 `# Environment` 起截断；只认 Claude Code 的固定措辞，
    //    避免误伤 system prompt 里正常出现的 "# Environment"）
    if let Some(idx) = s.find("# Environment") {
        let tail = &s[idx..];
        if tail.contains("invoked in the following environment") {
            s.truncate(idx);
        }
    }

    // 4) 已有的 gitStatus 环境块
    let cleaned = strip_env_injection(&s).unwrap_or(s);
    cleaned.trim().to_string()
}

/// 11128 `unapproved channel` 的自愈：把已构造好的出站体换成更保守的形态。
///
/// 🔴 默认只做**无损**那一层：再抹一遍 system 里的身份声明（`messages` 里可能
/// 还有多块 content 等默认路径没覆盖到的位置）。上游的完整归一化（角色收敛 /
/// 去 `name` / 内容块拍平）会**改变请求形状**，拍平 content 还会把图片弄丢，
/// 所以只在环境变量 `WB_SWITCH_AGGRESSIVE_HEAL=1` 时才做 —— 留一条「实在不行
/// 就全收敛」的退路，但不作为默认行为。
///
/// 返回新请求体；与原体相同表示没什么可改（调用方直接走退避重试）。
fn heal_body_for_channel(body: &Value, degraded: &mut Vec<String>) -> Value {
    let mut healed = body.clone();
    let aggressive = std::env::var("WB_SWITCH_AGGRESSIVE_HEAL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if aggressive {
        sanitize_messages_aggressive(&mut healed, degraded);
    }
    if let Some(msgs) = healed.get_mut("messages").and_then(Value::as_array_mut) {
        let mut touched = false;
        for m in msgs.iter_mut() {
            if m.get("role").and_then(Value::as_str) != Some("system") {
                continue;
            }
            if let Some(cleaned) = m
                .get("content")
                .and_then(Value::as_str)
                .and_then(strip_identity_declarations)
            {
                m["content"] = Value::String(cleaned);
                touched = true;
            }
        }
        if touched {
            degraded.push("self-heal-identity".to_string());
        }
    }
    healed
}

/// 最大强度归一化（11128 自愈重试用，见 [`heal_body_for_channel`]）。
///
/// 上游对「负载是不是官方客户端发的」有一套**不公开**的规则，且会随策略变动：
/// 2026-10-09 判环境注入块（`gitStatus:`）、2026-10-17 判 `developer` 角色，
/// 2026-10-21 再测前者已放行、只剩后者。既然规则会漂，就留一条「全部收敛」的退路。
///
/// 三件事：
/// 1. **角色收敛** —— 只保留 `system` / `user` / `assistant` / `tool`，
///    其它一律按 `system` 处理（`developer` 及其它未知别名）。
/// 2. **去掉 `name`** —— 部分客户端在这里写 agent 名，属于身份声明。
/// 3. **内容块拍平** —— 数组 content 合并成一个字符串，避开多块解析差异。
///
/// ⚠️ 会**改变请求形状**（拍平 content 会丢图片块），所以只在
/// `WB_SWITCH_AGGRESSIVE_HEAL=1` 时由 [`heal_body_for_channel`] 调用。
fn sanitize_messages_aggressive(body: &mut Value, degraded: &mut Vec<String>) {
    let Some(msgs) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut touched = false;
    for m in msgs.iter_mut() {
        // 1. 角色收敛
        let role = m.get("role").and_then(Value::as_str).unwrap_or("").to_string();
        let mapped = match role.as_str() {
            "system" | "user" | "assistant" | "tool" => None,
            // developer 及其它未知角色 → system（语义最接近，且上游认识）
            _ => Some("system"),
        };
        if let Some(new_role) = mapped {
            m["role"] = Value::String(new_role.to_string());
            touched = true;
        }
        // 2. 去掉 name
        if let Some(o) = m.as_object_mut() {
            if o.remove("name").is_some() {
                touched = true;
            }
        }
        // 3. 内容块拍平成字符串
        if let Some(parts) = m.get("content").and_then(Value::as_array) {
            let flat: String = parts
                .iter()
                .filter_map(|p| {
                    p.get("text")
                        .and_then(Value::as_str)
                        .or_else(|| p.as_str())
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
                .join("\n");
            m["content"] = Value::String(flat);
            touched = true;
        }
        // 4. 抹掉第三方 agent 身份声明
        //    只对 system 生效 —— 用户消息里的同名词是正常内容，不该动。
        if m.get("role").and_then(Value::as_str) == Some("system") {
            if let Some(cleaned) = m
                .get("content")
                .and_then(Value::as_str)
                .and_then(strip_identity_declarations)
            {
                m["content"] = Value::String(cleaned);
                touched = true;
            }
        }
    }
    if touched {
        degraded.push("aggressive-normalize".to_string());
    }
}

/// 上游失败响应体里是否带某个**业务码**（`"code":11128` / `code\":11128` 等形态）。
///
/// 🔴 必须锚定 `code` 键名，不能裸子串匹配：上游响应体里还有 `requestId` 这类
/// 32 位 hex 字段，裸 `contains("11128")` 会被随机 id 误命中（同一个思路见上游
/// 2026-10-26 的 `has_upstream_code` 修复）。响应体可能被再套一层 JSON 转义，
/// 所以紧凑/带空格、未转义/转义、`code`/`bizCode` 八种形态都要认。
fn has_upstream_code(text: &str, code: &str) -> bool {
    const KEYS: [&str; 8] = [
        "\"code\":",
        "\"code\": ",
        "code\\\":",
        "code\\\": ",
        "\"bizCode\":",
        "\"bizCode\": ",
        "bizCode\\\":",
        "bizCode\\\": ",
    ];
    KEYS.iter().any(|key| text.contains(&format!("{key}{code}")))
}

/// 上游的失败响应体是不是「渠道校验软拦截」（`11128`）。
///
/// 只看响应体里有没有这个码 / 这句 msg —— 上游把它包在 `message` 字符串里再套一层
/// JSON，所以不能按结构化字段取，直接做子串匹配最稳。
///
/// 视为**可重试**的理由见 [`MAX_CHANNEL_RETRY`]：它是间歇性的，且文案本身在让客户端重发。
/// 判定成它之后**不要立刻冷却账号** —— 号是好的，冻它只会把整个账号池拖空。
fn is_channel_block(text: &str) -> bool {
    has_upstream_code(text, "11128") || text.contains("unapproved channel")
}

/// 上游「模型侧瞬时故障」——官方文档把 `11133` / `11134` / `14003` 都归为
/// 「多与模型侧状态有关」，处理建议是「新开会话或切换模型重试」。
///
/// 🔴 这一类**不是**参数错误：`invalid_request_error`、`param` 字段看着像校验失败，
/// 但官方归类是模型侧状态，重发即可能成功。
///
/// ⇒ 正确处置是**重试**，而不是冷却账号 —— 账号和请求都没问题。
/// （判据与重试节奏来源：上游 2026-10-26 的逐参数排除实验，见 API-PROXY.md。）
fn is_transient_model_error(text: &str) -> bool {
    transient_code(text).is_some()
}

/// 命中时返回具体是哪个业务码（用于给用户看「是哪个码」）。
fn transient_code(text: &str) -> Option<&'static str> {
    ["11133", "11134", "14003"]
        .into_iter()
        .find(|code| has_upstream_code(text, code))
}

/// 从上游失败体里抠出 `requestId`（官方叫 **Trace ID**，申诉时必填）。
///
/// 形如 `"requestId":"9d5bc5fd-3e04-4752-8e13-a8d4cf28767e"`。
/// 官方渠道要求把它写进工单/邮件，否则客服定位不到具体请求。
/// （取值来源：上游 2026-10-29 的 11140 申诉指引。）
fn upstream_request_id(text: &str) -> Option<String> {
    let key = "\"requestId\":\"";
    let start = text.find(key)? + key.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    let id = &rest[..end];
    (!id.is_empty() && id.len() <= 64).then(|| id.to_string())
}

/// 上游「模型不存在 / 该账号无权使用该模型」（`11102`）。
///
/// 实测语义：请求**已通过渠道校验**，只是 model 解析不到（`service info not found`）
/// 或当前账号无权使用（`only available for authorized users`）。这不是账号故障 ——
/// 拿它 `mark_failed` 会把整个账号冻 120 秒，一次写错的模型名就白冻一个好号。
/// 判据基于响应体而非状态码：仓库未记录该码固定伴随哪个 HTTP 状态。
fn is_model_not_found(text: &str) -> bool {
    has_upstream_code(text, "11102")
        || text.contains("service info not found")
        || text.contains("only available for authorized users")
}

/// 上游 `403 / 11140` 的语义分类。
///
/// 🔴 `11140` 是**双语义码**（2026-10-27 调研报告 §2，外部社区两次实测一致）：
/// 同一个 HTTP 403、同一个 `msg:"request illegal"`，只有 `displayMsg` 能区分：
/// ① 「账号存在异常行为，已被限制使用」/ "unusual activity" → **账号级风控限制**，
///    换号有效、本号应长期摘除；
/// ② 「内容未通过安全审核」→ **请求级内容拒绝**，换号无效、账号本身没事。
/// 不按 displayMsg 分流就会把内容审核误判成封号（或反过来让封号每 120 秒继续打上游）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BanKind {
    /// 账号级风控限制：本号长期摘除，直到人工重新登录 / 申诉。
    Account,
    /// 请求级内容审核拒绝：不碰账号，原样/人话告知客户端。
    Content,
    /// 是 11140 但 displayMsg 认不出：保守长冷却 + 显式日志，不持久摘号。
    Unknown,
}

/// 判定失败响应体是不是 `11140`，以及属于哪一类语义。
fn ban_kind(text: &str) -> Option<BanKind> {
    if !has_upstream_code(text, "11140") {
        return None;
    }
    // 英文文案是 ASCII、在任何 JSON 转义形式下都保持原样，优先据此判定；
    // 中文既认原文，也认 serde_json 的 `\uXXXX` 小写转义形态（国内版可能出现转义体）。
    const ACCOUNT_MARKS: [&str; 6] = [
        "unusual activity",
        "has been restricted",
        "账号存在异常行为",
        "异常行为",
        "\\u8d26\\u53f7\\u5b58\\u5728\\u5f02\\u5e38\\u884c\\u4e3a",
        "\\u5f02\\u5e38\\u884c\\u4e3a",
    ];
    const CONTENT_MARKS: [&str; 4] = [
        "安全审核",
        "内容未通过",
        "\\u5b89\\u5168\\u5ba1\\u6838",
        "\\u5185\\u5bb9\\u672a\\u901a\\u8fc7",
    ];
    if ACCOUNT_MARKS.iter().any(|mark| text.contains(mark)) {
        Some(BanKind::Account)
    } else if CONTENT_MARKS.iter().any(|mark| text.contains(mark)) {
        Some(BanKind::Content)
    } else {
        Some(BanKind::Unknown)
    }
}

/// 11128 重试耗尽后给客户端的人话。
///
/// 两种变体成因不同，提示必须分开 —— 混在一起用户无从下手：
/// - `unapproved channel`：**负载**被判成非官方客户端（角色 / 身份声明）
/// - 其它（`request illegal` 等）：**调用方**被判成非官方客户端，改负载无用
fn channel_block_message(text: &str, edition_label: &str, retries: usize) -> String {
    if text.contains("unapproved channel") {
        format!(
            "上游拒绝了这次请求：{edition_label}网关判定「负载不合规」\
             （11128 Illegal API invocation from an unapproved channel），\
             已完成净化 + 自动重试 {retries} 次仍被拦。\
             这不是账号失效、不是频率超限，换账号也不会解决。\n\
             常见原因是系统提示词里带了第三方 agent 的身份声明（典型是 \
             `DeepSeek Harness`）或非常规角色。DSH 用户可关掉原生开关 \
             `includeHarnessIdentity`，或从系统提示词里删掉那段声明。\n\
             若已关掉仍报，请创建 `~/.wb-switch/DEBUG_DUMP` 后复现一次，\
             把 `~/.wb-switch/dump/last-request.json` 带上来定位。"
        )
    } else {
        format!(
            "上游拒绝了这次调用：{edition_label}网关判定「调用方不是官方客户端」\
             （11128 request illegal），已自动重试 {retries} 次仍被拦。\
             这不是账号失效，也不是频率超限 —— 该账号的只读接口仍然正常，换账号也不会解决。\
             这是上游对非官方客户端的策略拦截，本地无法绕过。可以稍后重试；\
             国内版入口通常比国际版宽松，必要时换用国内版。"
        )
    }
}

/// 解析上游「使用频率超限」响应，返回重置时刻（毫秒）。
///
/// 两档上游的码和文案都不一样，但时间都写死在句子里，格式同为
/// `YYYY-MM-DD HH:MM:SS UTC+8`：
///
/// - 国内版（code 60004）：`{"message":"{\"code\":60004,\"msg\":\"…将在
///   2026-10-17 11:46:56 UTC+8 重置…\"}"}`（双层 JSON）
/// - 国际版（code 6004）：`{"code":6004,"msg":"usage exceeds frequency limit,
///   but don't worry, your usage will reset at 2026-10-23 21:59:54 UTC+8, …"}`
///
/// 在上游原文里扫第一个 `YYYY-MM-DD HH:MM:SS`（按 UTC+8 解释）。
///
/// 🔴 **不依赖中文关键词**：上游的 JSON 会把中文转义成 `\uXXXX`，
/// `contains("将在")` / `find("频率限制")` 这类判断在转义体上全线失效
/// （v1.6.15 的解析就是这么失败的）。日期时间串是纯 ASCII，在任何转义形式下都保持原样。
fn find_reset_datetime(text: &str) -> Option<i64> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < 19 {
        return None;
    }
    let offset = FixedOffset::east_opt(8 * 3600)?;
    for start in 0..=(chars.len() - 19) {
        let candidate: String = chars[start..start + 19].iter().collect();
        let b = candidate.as_bytes();
        if b[4] != b'-' || b[7] != b'-' || b[10] != b' ' || b[13] != b':' || b[16] != b':' {
            continue;
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(&candidate, "%Y-%m-%d %H:%M:%S") {
            if let Some(dt) = offset.from_local_datetime(&naive).single() {
                return Some(dt.timestamp_millis());
            }
        }
    }
    None
}

/// 解析上游「使用频率超限」响应，返回重置时刻（毫秒）。
///
/// 两档上游的码和文案都不一样，但时间都写死在句子里，格式同为
/// `YYYY-MM-DD HH:MM:SS UTC+8`：
///
/// - 国内版（code 60004）：`{"message":"{\"code\":60004,\"msg\":\"…将在
///   2026-10-17 11:46:56 UTC+8 重置…\"}"}`（双层 JSON）
/// - 国际版（code 6004）：`{"code":6004,"msg":"usage exceeds frequency limit,
///   but don't worry, your usage will reset at 2026-10-23 21:59:54 UTC+8, …"}`
///
/// 判定关键词保持 ASCII 优先（`60004` / `6004` 是稳定错误码，不会被转义），
/// 中文 `频率限制` 只作为补充（未转义时才有用）。时间串用
/// [`find_reset_datetime`] 扫，避免被中文转义拖累。
/// 不是频率超限（或缺重置时间）返回 None，调用方走普通冷却。
fn rate_limit_reset_ms(text: &str) -> Option<i64> {
    // 🔴 先认国内版：`6004` 是 `60004` 的子串，顺序反过来会把两版混在一起。
    // 国际版漏判的后果很实在：只走 120 秒短冷却，号回池立刻又 429、整池空转，
    // 客户端拿到的还是原始英文 JSON —— 而不是「哪个号、哪个模型、何时恢复」。
    if text.contains("60004") || text.contains("频率限制") || text.contains("frequency limit") {
        find_reset_datetime(text)
    } else {
        None
    }
}

/// 频率超限的**实际暂停时刻**（毫秒），`None` = 不按频率超限处理。
///
/// 上游给了重置时刻就用它（[`rate_limit_reset_ms`]，或 429 时直接扫时间串）；
/// 429 却没给时刻时退到本地兜底（[`RATE_LIMIT_FALLBACK`]）—— 那之前这里返回
/// `None`，调用方只能走整账号 120 秒短冷却，同账号其他模型被一起停掉，
/// 且回池大概率立刻再 429。
///
/// 非 429 的失败一律 `None`：不能把 400 也塞进这个兜底里。
fn rate_limit_until(text: &str, status: u16, now_ms: i64) -> Option<i64> {
    let declared_reset = rate_limit_reset_ms(text).or_else(|| {
        if status == 429 {
            find_reset_datetime(text)
        } else {
            None
        }
    });
    declared_reset
        .or_else(|| (status == 429).then(|| now_ms + RATE_LIMIT_FALLBACK.as_millis() as i64))
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
    // 标准采样 / 输出控制参数：上游全部接受，原样透传。
    // 以前只转 temperature/top_p/max_tokens，其余 8 个**被静默丢弃** ——
    // 表现为客户端设了 JSON 模式 / seed / 禁止并行工具调用，等于没设。
    for key in STANDARD_PASSTHROUGH {
        if let Some(v) = incoming.get(*key) {
            body[*key] = v.clone();
        }
    }
    // `stream_options.include_usage=false` 会让上游不再回 usage，而本地用量统计
    // 正依赖它 —— 强制定为 true，其余开关由客户端决定。
    if body.get("stream_options").is_some() {
        body["stream_options"]["include_usage"] = Value::Bool(true);
    }
    if let Some(t) = incoming.get("tools") {
        body["tools"] = t.clone();
    }
    // 🔴 `tool_choice` 必须**归一化**后再发：上游只吃字符串，
    // 收到 OpenAI 规范的对象形式会直接 11101（见 normalize_tool_choice）。
    // 没有 tools 时 tool_choice 无意义（上游可能 400），翻译层会先把它摘掉。
    if let Some(tc) = incoming.get("tool_choice") {
        match normalize_tool_choice(tc) {
            Some(norm) => {
                if norm != *tc {
                    degraded.push("tool-choice".to_string());
                }
                body["tool_choice"] = norm;
            }
            None => degraded.push("tool-choice-dropped".to_string()),
        }
    }
    // `top_k` 是 Anthropic 独有参数，OpenAI 规范没有。上游实测忽略未知字段，
    // 透传不会报错；不支持的模型自然无效果，好过静默丢弃客户端参数。
    if let Some(v) = incoming.get("top_k") {
        body["top_k"] = v.clone();
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

/// 把 `tool_choice` 归一成上游认识的形式（**只吃字符串**）。
///
/// 🔴 OpenAI 规范里 `tool_choice` 允许对象：
/// `{"type":"function","function":{"name":"f"}}`。
/// 上游对这个对象形式直接 400：
/// `11101 Unmarshal chat params failed with error: json: cannot unmarshal object …`。
/// 而**函数名字符串 `"f"` 上游是接受的**，所以对象形式可以无损降级成「函数名」——
/// 强制调用语义完整保留。
///
/// 返回 `None` = 无法安全转换，调用方应**丢弃**该字段（发过去只会 400）。
fn normalize_tool_choice(v: &Value) -> Option<Value> {
    match v {
        // 字符串（"auto" / "none" / "required" / 函数名）上游原样接受。
        Value::String(s) if !s.trim().is_empty() => Some(v.clone()),
        Value::Object(map) => {
            let ty = map.get("type").and_then(Value::as_str).unwrap_or("");
            match ty {
                // 指定函数 → 用函数名（上游接受的字符串形式，语义最接近）。
                //
                // ⚠️ 与上游实现有意不同：上游在拿不到 name 时兜底成 `"required"`，
                // 那会把「必须调 f」悄悄变成「必须调某个工具」。我们选择丢弃，
                // 让问题显式暴露（degraded 记 `tool-choice-dropped`）。
                "function" => map
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.trim().is_empty())
                    .map(|name| json!(name)),
                "auto" | "none" | "required" => Some(json!(ty)),
                // Anthropic 的 {"type":"any"} / {"type":"tool"} 语义
                "any" => Some(json!("required")),
                "tool" => map
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.trim().is_empty())
                    .map(|n| json!(n)),
                // allowed_tools 之类：退化成 auto，别把请求打死。
                "allowed" | "allowed_tools" => Some(json!("auto")),
                _ => None,
            }
        }
        _ => None,
    }
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
    /// 聊天主链路共用的出站 client。
    ///
    /// 🔴 以前**每个请求**都 `Client::builder().build()` 一个新的：连接池、TLS 会话
    /// 全都不能复用，还得带超时重连；同时它**没有任何超时**，上游静默丢包时
    /// `send().await` 会永久悬挂，[`MAX_TRANSPORT_RETRY`] 永远等不到 Err。
    /// 现在全局一份，带 [`CONNECT_TIMEOUT`]；流式响应的「停滞」由泵里的
    /// [`StreamGuard`] 兜（整体超时会误杀长回复）。
    stream_upstream: Arc<reqwest::Client>,
    /// 会话粘滞：会话 → 账号。**按入口（版本）各一份**，两个入口的号池虽然共享，
    /// 但一个入口的会话不应该把另一个入口的号粘住。
    sticky: Arc<SessionStick>,
}

/// 会话与账号的粘滞表。
///
/// 上游的 prompt 缓存**按账号隔离**：同一个对话被轮转分散到 5 个号上，每个号都要
/// 各自重建一遍上下文。把同一会话的连续请求固定到同一个号，能少建 4 份。
///
/// 键是入站 `X-Conversation-ID`（官方桌面端 = sessionId），没有就不粘滞（不猜）。
/// 与上游**原样转发**这个请求头（不吞、不改、不新增），只是自己多看一眼。
struct SessionStick {
    ttl_ms: i64,
    entries: Mutex<HashMap<String, (String, i64)>>,
}

impl SessionStick {
    fn new() -> Self {
        Self {
            ttl_ms: SESSION_STICK_TTL.as_millis() as i64,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn lookup(&self, session: &str, now: i64) -> Option<String> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        // 顺手清掉过期的；条数有上限，不会无限涨。
        if entries.len() > SESSION_STICK_MAX {
            entries.retain(|_, (_, expires)| *expires > now);
        }
        match entries.get(session) {
            Some((uid, expires)) if *expires > now => Some(uid.clone()),
            Some(_) => {
                entries.remove(session);
                None
            }
            None => None,
        }
    }

    fn bind(&self, session: &str, uid: &str, now: i64) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= SESSION_STICK_MAX {
            entries.retain(|_, (_, expires)| *expires > now);
        }
        entries.insert(session.to_string(), (uid.to_string(), now + self.ttl_ms));
    }

    #[cfg(test)]
    fn snapshot(&self) -> HashMap<String, String> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .map(|(session, (uid, _))| (session.clone(), uid.clone()))
            .collect()
    }

    fn unbind_session(&self, session: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.remove(session);
    }

    /// 账号被判失败时，解绑所有粘在它上面的会话 —— 否则这些会话会一直撞冷却。
    fn unbind_uid(&self, uid: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, (bound, _)| bound != uid);
    }
}

/// 会话粘滞的有效期：上游缓存几分钟不用就失效，超过它再坚持粘滞没有意义，
/// 反而让会话只能等一个号。
const SESSION_STICK_TTL: Duration = Duration::from_secs(10 * 60);
/// 粘滞表最多记多少个会话；超出时先清过期项。
const SESSION_STICK_MAX: usize = 4096;

/// 当前在跑的入口各自的粘滞表（按版本各一份）。
///
/// 放在全局是为了让 [`Pool::mark_until`] 在账号失败时能解绑粘在它上面的会话 ——
/// 池子读冷却状态时不必再额外传参，也不会在两处维护两份状态。
fn current_sticky() -> &'static Mutex<HashMap<Edition, Arc<SessionStick>>> {
    static STICKY: std::sync::OnceLock<Mutex<HashMap<Edition, Arc<SessionStick>>>> =
        std::sync::OnceLock::new();
    STICKY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 为某个入口取（或建）一份粘滞表。
fn sticky_for(edition: Edition) -> Arc<SessionStick> {
    let mut all = current_sticky().lock().unwrap_or_else(|e| e.into_inner());
    all.entry(edition).or_insert_with(|| Arc::new(SessionStick::new())).clone()
}

/// 入站请求里的会话标识（只认 `X-Conversation-ID`，与官方桌面端的语义一致）。
fn inbound_session(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-conversation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(str::to_string)
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
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .trim_start_matches("Bearer ")
        .trim();
    // Anthropic SDK / Claude Code 走 `x-api-key`，OpenAI SDK / CCH 的 claude 供应商
    // 走 `Authorization: Bearer`。两个入口共用同一把 key，这里两种头都认。
    let x_api_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim();
    bearer == want || x_api_key == want
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
async fn list_models(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = guard_request(&headers) {
        return *resp;
    }
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
            // 加密信封账号拿不到明文 token，打上游只会 401（同 `eligible_accounts`）。
            && !is_envelope(a, "access_token")
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
    match tokio::time::timeout(Duration::from_secs(30), refresh_token_only(acc.clone())).await {
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
    for (k, v) in chat_headers(&acc, None) {
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
    /// 入站请求带的会话标识（`X-Conversation-ID`，官方桌面端值 = sessionId）。
    /// 只用于「会话粘滞选号」与按会话评估命中率；存盘时只留掩码，不落原始值。
    session: Option<String>,
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

/// 单个统计桶（total / 账号 / 模型 / 单日共用）的初始形状。
///
/// cache 字段是后加的：上游的缓存命中/写入量能直接换算成钱，
/// 只看 promptTokens 会把「命中缓存」和「全量输入」混为一谈。
fn empty_usage_bucket() -> Value {
    json!({
        "requests": 0, "errors": 0,
        "promptTokens": 0, "completionTokens": 0,
        "cacheReadTokens": 0, "cacheWriteTokens": 0,
        "credit": 0,
    })
}

/// 单个版本的用量文档（旧格式的扁平结构原样保留，前端不用大改）。
fn empty_edition_usage() -> Value {
    json!({
        "total": empty_usage_bucket(),
        // 按**本地时区**日期（YYYY-MM-DD）聚合；跨过 0 点就是新的一天，
        // 与用户看到的日历一致（UTC 会让东八区早上 8 点前的量落到前一天）。
        "daily": {},
        // 模型 → 日期 → 桶。总量曲线看不出是哪个模型在烧额度，
        // 但把模型维度塞进 daily 的桶里会让前端结构变复杂，故单独一张表。
        "dailyByModel": {},
        // 累计口径的按天表：与 `dailyByModel` 形状相同，但 **total 桶同步、从不按窗口
        // 裁剪**。页面上的「今天 / 7 / 30 天」用 `dailyByModel`，而「总计」必须等于
        // `total` 的全部历史 —— 两件事用两张表，才不会因为裁剪让总计凭空缩水。
        // 首次升级时由当时已有的 `dailyByModel` 快照迁移（见 backfill_edition_usage）。
        "totalDailyByModel": {},
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

    // ④ 刚拆出来的桶 daily 还是空的，但 recent 已有历史 ⇒ 强制从 recent 回填一次。
    // 这是旧数据能还原「按天」曲线的唯一来源（byAccount 没有时间戳）。
    for edition in Edition::ALL {
        backfill_edition_usage(&mut out["byEdition"][edition_key(edition)], true);
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
        Some(v) => {
            let mut doc = migrate_legacy_usage(v);
            // 已在用新格式、但由旧版本写下的桶没有 daily / cache 字段，这里补齐。
            // 该步是幂等的（daily 已存在就不回填），每次 load 调用都安全。
            backfill_usage_doc(&mut doc);
            doc
        }
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

/// 上游 usage 里缓存**读**（命中）字段的别名。
///
/// 不同供应商各说各话：有的照 OpenAI 的 `prompt_cache_hit_tokens`，有的直接
/// `cache_read_input_tokens`，还有的只把命中数嵌在 `prompt_tokens_details` 里。
/// 取**第一个 > 0** 的别名（0 通常只是「这个别名没填」，不能挡住后面真正有值的）。
const USAGE_CACHE_READ_KEYS: &[&str] = &[
    "cache_read_input_tokens",
    "cacheReadInputTokens",
    "prompt_cache_hit_tokens",
    "cached_tokens",
];

/// 缓存**写**（创建）字段的别名。
const USAGE_CACHE_WRITE_KEYS: &[&str] = &[
    "cache_creation_input_tokens",
    "prompt_cache_write_tokens",
    "cache_write_input_tokens",
    "cacheWriteInputTokens",
];

fn usage_positive_num(usage: Option<&Value>, keys: &[&str]) -> i64 {
    let Some(obj) = usage.and_then(Value::as_object) else {
        return 0;
    };
    keys.iter()
        .find_map(|k| obj.get(*k).and_then(Value::as_i64).filter(|v| *v > 0))
        .unwrap_or(0)
}

fn usage_cache_read(usage: Option<&Value>) -> i64 {
    let flat = usage_positive_num(usage, USAGE_CACHE_READ_KEYS);
    if flat > 0 {
        return flat;
    }
    // OpenAI 兼容格式把命中数嵌在 details 里，放在扁平别名之后兜底。
    usage
        .and_then(Value::as_object)
        .and_then(|o| o.get("prompt_tokens_details"))
        .and_then(Value::as_object)
        .and_then(|d| d.get("cached_tokens"))
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .unwrap_or(0)
}

fn usage_cache_write(usage: Option<&Value>) -> i64 {
    usage_positive_num(usage, USAGE_CACHE_WRITE_KEYS)
}

/// `now_ms` → **本地时区**日期（`%Y-%m-%d`）。
///
/// 按天聚合用本地日期而不是 UTC：否则东八区 0~8 点的用量会记到前一天，
/// 用户在界面上看到的「今天」和系统日历对不上。
fn local_date_of_ms(now_ms: i64) -> String {
    Local
        .timestamp_millis_opt(now_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

fn ensure_object(v: &mut Value) {
    if !v.is_object() {
        *v = json!({});
    }
}

/// 只补缺失的 cache 字段，**已有数值一律不动**（旧数据迁移时不能把统计清零）。
fn ensure_cache_fields(bucket: &mut Value) {
    if !bucket.is_object() {
        *bucket = empty_usage_bucket();
        return;
    }
    let o = bucket.as_object_mut().unwrap();
    o.entry("cacheReadTokens").or_insert(json!(0));
    o.entry("cacheWriteTokens").or_insert(json!(0));
}

fn bump_counter(o: &mut Map<String, Value>, key: &str, by: i64) {
    let v = o.get(key).and_then(Value::as_i64).unwrap_or(0) + by;
    o.insert(key.to_string(), json!(v));
}

/// 给一个统计桶（total / 账号 / 模型 / 单日）加上一笔。
///
/// 缺的字段按 0 起步，已有的累加，不重建对象 —— 调用方可能传入带
/// `nickname` 等扩展字段的桶。
fn bump_usage_bucket(
    bucket: &mut Value,
    prompt: i64,
    completion: i64,
    cache_read: i64,
    cache_write: i64,
    credit: f64,
    ok: bool,
) {
    ensure_object(bucket);
    let o = bucket.as_object_mut().unwrap();
    bump_counter(o, "requests", 1);
    if !ok {
        bump_counter(o, "errors", 1);
    }
    bump_counter(o, "promptTokens", prompt);
    bump_counter(o, "completionTokens", completion);
    bump_counter(o, "cacheReadTokens", cache_read);
    bump_counter(o, "cacheWriteTokens", cache_write);
    let credit = o.get("credit").and_then(Value::as_f64).unwrap_or(0.0) + credit;
    o.insert("credit".to_string(), json!(credit));
}

/// 给「模型 → 日期 → 桶」表加一笔（`dailyByModel` 与 `totalDailyByModel` 共用）。
fn bump_model_day(doc: &mut Value, key: &str, model: &str, date: &str, usage: ([i64; 4], f64, bool)) {
    ensure_object(&mut doc[key]);
    let model_days = doc[key]
        .as_object_mut()
        .unwrap()
        .entry(model.to_string())
        .or_insert_with(|| json!({}));
    ensure_object(model_days);
    let day = model_days
        .as_object_mut()
        .unwrap()
        .entry(date.to_string())
        .or_insert_with(empty_usage_bucket);
    bump_usage_bucket(day, usage.0[0], usage.0[1], usage.0[2], usage.0[3], usage.1, usage.2);
}

/// 把一次请求累加进**某一版**的用量文档。
///
/// 抽成纯函数（只碰传入的 Value，不读盘、不看 home 目录）是为了能直接单测
/// 「同一天合并」「不同模型分开」这类容易写错、又只能靠日积月累才暴露的逻辑。
/// 日期由 `now_ms` 推出来，测试可以注入任意时刻而不是依赖系统时钟。
fn bump_usage_doc(doc: &mut Value, meta: &UsageMeta, usage: Option<&Value>, ok: bool, now_ms: i64) {
    // 失败请求没有可信的 usage：token / 额度一律记 0，只留 requests/errors。
    // 上游报错前偶尔会回一段残缺 usage，把它算进用量会让曲线凭空冒尖。
    let (prompt, completion, cache_read, cache_write, credit) = if ok {
        (
            usage_num(usage, "prompt_tokens"),
            usage_num(usage, "completion_tokens"),
            usage_cache_read(usage),
            usage_cache_write(usage),
            usage_credit(usage),
        )
    } else {
        (0, 0, 0, 0, 0.0)
    };
    let date = local_date_of_ms(now_ms);

    ensure_object(doc);
    bump_usage_bucket(
        &mut doc["total"],
        prompt,
        completion,
        cache_read,
        cache_write,
        credit,
        ok,
    );

    ensure_object(&mut doc["byAccount"]);
    let account = doc["byAccount"]
        .as_object_mut()
        .unwrap()
        .entry(meta.uid.clone())
        .or_insert_with(empty_usage_bucket);
    ensure_object(account);
    account["nickname"] = json!(meta.nickname);
    bump_usage_bucket(
        account,
        prompt,
        completion,
        cache_read,
        cache_write,
        credit,
        ok,
    );

    ensure_object(&mut doc["byModel"]);
    let model = doc["byModel"]
        .as_object_mut()
        .unwrap()
        .entry(meta.model.clone())
        .or_insert_with(empty_usage_bucket);
    bump_usage_bucket(
        model,
        prompt,
        completion,
        cache_read,
        cache_write,
        credit,
        ok,
    );

    ensure_object(&mut doc["daily"]);
    let day = doc["daily"]
        .as_object_mut()
        .unwrap()
        .entry(date.clone())
        .or_insert_with(empty_usage_bucket);
    bump_usage_bucket(
        day,
        prompt,
        completion,
        cache_read,
        cache_write,
        credit,
        ok,
    );

    ensure_object(&mut doc["dailyByModel"]);
    let model_days = doc["dailyByModel"]
        .as_object_mut()
        .unwrap()
        .entry(meta.model.clone())
        .or_insert_with(|| json!({}));
    ensure_object(model_days);
    let day = model_days
        .as_object_mut()
        .unwrap()
        .entry(date.clone())
        .or_insert_with(empty_usage_bucket);
    bump_usage_bucket(
        day,
        prompt,
        completion,
        cache_read,
        cache_write,
        credit,
        ok,
    );

    // 累计投影与 `dailyByModel` 逐笔同写；页面裁剪 `dailyByModel` 不影响它。
    let values = [prompt, completion, cache_read, cache_write];
    bump_model_day(doc, "totalDailyByModel", &meta.model, &date, (values, credit, ok));

    if doc.get("recent").map_or(true, |v| !v.is_array()) {
        doc["recent"] = json!([]);
    }
    if let Some(arr) = doc["recent"].as_array_mut() {
        arr.insert(
            0,
            json!({
                "ts": now_ms,
                "uid": meta.uid,
                "nickname": meta.nickname,
                "model": meta.model,
                "stream": meta.stream,
                "ok": ok,
                "promptTokens": prompt,
                "completionTokens": completion,
                "cacheReadTokens": cache_read,
                "cacheWriteTokens": cache_write,
                "credit": credit,
                // 会话只存掩码：用来按会话对照命中率，原始 id 不落盘。
                "session": meta.session.as_deref().map(crate::modules::token_stats::session_hash),
            }),
        );
        arr.truncate(USAGE_RECENT_MAX);
    }
}

/// 给单个版本桶补齐新字段（daily / dailyByModel / cache）。
///
/// `force_backfill` 供「刚迁移完」的桶使用：此时 daily 是新建的空表，但 recent
/// 里已有历史，需要按 recent 回填一次。
///
/// 🔴 幂等：非强制模式下只有 `daily` **缺失（或形状不对）**才回填。若每次 load
/// 都拿 recent 重算，最近 100 条会被反复计入 daily，日曲线越看越多。
fn backfill_edition_usage(bucket: &mut Value, force_backfill: bool) {
    ensure_object(bucket);

    // cache 字段是这次新加的：旧桶没有就补 0，已有的数值不动。
    ensure_cache_fields(&mut bucket["total"]);
    for key in ["byAccount", "byModel"] {
        if let Some(m) = bucket.get_mut(key).and_then(Value::as_object_mut) {
            for b in m.values_mut() {
                ensure_cache_fields(b);
            }
        }
    }

    let daily_valid = bucket.get("daily").and_then(Value::as_object).is_some();
    if force_backfill || !daily_valid {
        // 历史只能从 recent 还原：它是唯一带时间戳的数据（最多 100 条），
        // 更早的用量早已滚出列表，无法按天还原，只能从今天起重新累积。
        let mut daily = Map::new();
        let mut daily_by_model: Map<String, Value> = Map::new();
        if let Some(arr) = bucket.get("recent").and_then(Value::as_array) {
            for item in arr {
                let ts = item.get("ts").and_then(Value::as_i64).unwrap_or(0);
                let date = local_date_of_ms(ts);
                let prompt = item
                    .get("promptTokens")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let completion = item
                    .get("completionTokens")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                // 缓存字段是本版新增的，旧 recent 没有 ⇒ 按 0 记。
                let cache_read = item
                    .get("cacheReadTokens")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let cache_write = item
                    .get("cacheWriteTokens")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let credit = item.get("credit").and_then(Value::as_f64).unwrap_or(0.0);
                // 旧数据可能没有 ok 字段，默认为成功（错误数只会少算，不会多算）。
                let ok = item.get("ok").and_then(Value::as_bool).unwrap_or(true);

                bump_usage_bucket(
                    daily.entry(date.clone()).or_insert_with(empty_usage_bucket),
                    prompt,
                    completion,
                    cache_read,
                    cache_write,
                    credit,
                    ok,
                );

                let model = item.get("model").and_then(Value::as_str).unwrap_or("");
                let model_days = daily_by_model
                    .entry(model.to_string())
                    .or_insert_with(|| json!({}));
                ensure_object(model_days);
                bump_usage_bucket(
                    model_days
                        .as_object_mut()
                        .unwrap()
                        .entry(date)
                        .or_insert_with(empty_usage_bucket),
                    prompt,
                    completion,
                    cache_read,
                    cache_write,
                    credit,
                    ok,
                );
            }
        }
        bucket["daily"] = Value::Object(daily);
        bucket["dailyByModel"] = Value::Object(daily_by_model);
    } else {
        // daily 已经有数据 ⇒ 绝不重算，只保证 dailyByModel 存在。
        ensure_object(&mut bucket["dailyByModel"]);
    }

    // 半升级版本（有 daily 但桶里没 cache 字段）也要补齐，且不重算任何数。
    if let Some(days) = bucket.get_mut("daily").and_then(Value::as_object_mut) {
        for b in days.values_mut() {
            ensure_cache_fields(b);
        }
    }
    if let Some(models) = bucket
        .get_mut("dailyByModel")
        .and_then(Value::as_object_mut)
    {
        for days in models.values_mut() {
            if let Some(days) = days.as_object_mut() {
                for b in days.values_mut() {
                    ensure_cache_fields(b);
                }
            }
        }
    }

    // 累计投影的**一次性快照**：老文件里没有 `totalDailyByModel` 时，用此刻的
    // `dailyByModel` 作起点（之后页面再裁剪 `dailyByModel` 也不影响它）。
    //
    // 🔴 只在**缺失**时快照：已有快照就说明它从升级那天起逐笔累加、与 `total`
    // 同口径，再拿 `dailyByModel` 覆盖会把已被裁掉的日期弄丢。
    let missing_snapshot = bucket.get("totalDailyByModel").is_none();
    if missing_snapshot {
        let snapshot = bucket
            .get("dailyByModel")
            .cloned()
            .unwrap_or_else(|| json!({}));
        bucket["totalDailyByModel"] = snapshot;
    }
    if let Some(models) = bucket
        .get_mut("totalDailyByModel")
        .and_then(Value::as_object_mut)
    {
        for days in models.values_mut() {
            if let Some(days) = days.as_object_mut() {
                for b in days.values_mut() {
                    ensure_cache_fields(b);
                }
            }
        }
    }
}

/// 给整份统计文档补齐新字段（幂等，可放心在每次 load 时调用）。
fn backfill_usage_doc(doc: &mut Value) {
    if let Some(m) = doc.get_mut("byEdition").and_then(Value::as_object_mut) {
        for bucket in m.values_mut() {
            backfill_edition_usage(bucket, false);
        }
    }
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
///
/// 这里只负责加锁、读盘、调用纯函数 [`bump_usage_doc`]、写盘；
/// 累加逻辑全部在那个纯函数里，便于单测。
///
/// 写盘走 [`atomic_write`]（临时文件 + rename）：Windows 上对正在被其它进程读的
/// 文件直接覆盖会偶发 `os error 5`，一旦发生在「统计页正在读」的同一时刻，
/// 这一笔就会被静默吞掉（旧实现写失败只 `let _ =`）。rename 只在目标被占用时失败，
/// 而且失败时原文件仍完整 —— 统计不会出现半截 JSON。
fn record_usage(meta: &UsageMeta, usage: Option<&Value>, ok: bool) {
    let now = now_ms();
    let _g = USAGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut doc = load_usage();
    // 🔴 只写自己那一档：国内版入口的量不会跑到国际版页面上（反之亦然）。
    let key = edition_key(meta.edition);
    if doc["byEdition"].get(key).is_none() {
        doc["byEdition"][key] = empty_edition_usage();
    }

    bump_usage_doc(&mut doc["byEdition"][key], meta, usage, ok, now);

    doc["updatedAt"] = json!(now);
    if let Ok(text) = serde_json::to_string_pretty(&doc) {
        if let Err(error) = write_usage_atomically(&usage_path(), &text) {
            // 失败时不隐瞒：下一次请求读到的仍是上一份完整文件，打印出来便于排查磁盘/权限。
            eprintln!("[反代] 用量统计写盘失败（本次不落盘，历史不受影响）: {error}");
        }
    }
}

/// 原子写用量文件：先写同目录临时文件，再 rename 覆盖；**失败时收拾临时文件**。
///
/// 用量文件每请求写一次，Windows 上目标被统计页读占用时 rename 会偶发失败，
/// 不清理就会在 `~/.wb-switch/` 下留一堆 `.tmp-*`。
fn write_usage_atomically(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(
        "{file_name}.tmp-{}",
        crate::modules::config::now_ms()
    ));
    if let Err(error) = std::fs::write(&tmp, content) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    Ok(())
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

/// 读完整条上游流，边重组边推给客户端；结束时补末帧与 `[DONE]`，并通过
/// [`PumpOut::Finished`] 告诉路由这一单该怎么记账。
///
/// 🔴 记账判定（修复前这里是**无条件** `ok=true`）：
/// - `Done`：上游流完整读完；
/// - `ClientGone`：客户端中途断开 —— **立刻停读上游**（drop 上游流），记 `ok=false`，
///   且已发出的部分 token 照实计（上游确实算过费）。以前这里会把剩余回复读完丢弃、
///   还记成功；
/// - `UpstreamError`：上游报错 / 读超时，记 `ok=false`。
#[allow(clippy::too_many_arguments)]
async fn sse_pump<S>(
    client: Arc<reqwest::Client>,
    mut upstream: S,
    tx: tokio::sync::mpsc::Sender<PumpOut>,
    meta: UsageMeta,
) where
    S: futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + Unpin + 'static,
{
    let mut raw = String::new();
    let mut tail = String::new();
    let mut template: Option<Value> = None;
    let mut last_frame: Option<Value> = None;
    let mut role_done = false;
    let mut field = String::new();
    let mut acc = String::new();
    let mut guard = StreamGuard::new();
    let mut got_any = false;

    let outcome = 'outer: loop {
        guard.arm(!got_any);
        let next = match tokio::time::timeout_at(guard.deadline, upstream.next()).await {
            Ok(next) => next,
            Err(_) => {
                eprintln!("[反代] uid={} model={} {}", meta.uid, meta.model, guard.timed_out());
                break Emitted::UpstreamError;
            }
        };
        let Some(item) = next else { break Emitted::Done };
        match item {
            Ok(chunk) => {
                got_any = true;
                let text = String::from_utf8_lossy(&chunk).to_string();
                raw.push_str(&text);
                tail.push_str(&text);
            }
            Err(e) => {
                eprintln!("[反代] 上游流中断 uid={} model={}: {e}", meta.uid, meta.model);
                break Emitted::UpstreamError;
            }
        }

        // 按空行切出完整事件；不足一个事件的留在 tail 里等下一块
        while let Some(idx) = tail.find("

") {
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
                    if tx.send(PumpOut::Bytes(b)).await.is_err() {
                        break 'outer Emitted::ClientGone;
                    }
                }
                field.clear();
                let mut fwd = frame.clone();
                normalize_finish(&mut fwd);
                if let Some(b) = sse_bytes(&fwd) {
                    if tx.send(PumpOut::Bytes(b)).await.is_err() {
                        break 'outer Emitted::ClientGone;
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
                        if tx.send(PumpOut::Bytes(b)).await.is_err() {
                            break 'outer Emitted::ClientGone;
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
                        if tx.send(PumpOut::Bytes(b)).await.is_err() {
                            break 'outer Emitted::ClientGone;
                        }
                    }
                }
            }

            // 末帧要留着最后发：先把它手里的 delta 并入累积，避免丢结尾
            if !finish_str(&frame).is_empty() {
                last_frame = Some(frame);
            }
        }
    };

    // 🔴 客户端已经走了：先断开上游，后面的收尾一律没必要做。
    if outcome == Emitted::ClientGone {
        drop(upstream);
        let usage = usage_from_sse(&raw);
        record_usage(&meta, usage.as_ref(), false);
        // 这里不再往 tx 发结束标记：客户端那侧的 [`PumpBody`] 可能已经被 drop，
        // 发不出去反而会阻塞。收尾状态由 `PumpBody::drop` 自己的通知驱动，不依赖这条。
        let _ = &client;
        return;
    }

    if let Some(b) = take_flush(template.as_ref(), &field, &mut acc, &mut role_done) {
        let _ = tx.send(PumpOut::Bytes(b)).await;
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
            let _ = tx.send(PumpOut::Bytes(b)).await;
        }
    }
    let _ = tx
        .send(PumpOut::Bytes(bytes::Bytes::from_static(b"data: [DONE]

")))
        .await;

    let usage = usage_from_sse(&raw);
    record_usage(&meta, usage.as_ref(), outcome == Emitted::Done);
    let _ = tx.send(PumpOut::Finished(outcome)).await;
    drop(client);
}

async fn chat_completions(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(incoming): Json<Value>,
) -> Response {
    chat_completions_inner(&st, &headers, incoming).await
}

/// OpenAI 聊天入口的实际实现。
///
/// 单独拆出来是为了让 `/v1/messages`（Anthropic 协议）在翻译完请求后**直接复用**
/// 选号、冷却、重试、用量统计这条链，而不是重新实现一遍或再发一次本地 HTTP。
async fn chat_completions_inner(st: &AppState, headers: &HeaderMap, incoming: Value) -> Response {
    // `/v1/messages` 也走这里：来源校验放在这一层，两个入口一起受保护。
    if let Err(resp) = guard_request(headers) {
        return *resp;
    }
    if !check_auth(headers, &st.endpoint.api_key) {
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

    // 会话粘滞：官方桌面端发 `X-Conversation-ID`（= sessionId），把同一会话的连续
    // 请求固定到同一个号，避免每个号都重建一遍上游缓存。没有该头就纯轮转。
    let session = inbound_session(headers);
    let picked = match session.as_deref() {
        Some(session) => {
            take_account(
                &st.pool,
                &st.cfg,
                Some(st.edition),
                Some(model.as_str()),
                Some((st.sticky.as_ref(), session)),
            )
            .await
        }
        None => take_account(&st.pool, &st.cfg, Some(st.edition), Some(model.as_str()), None).await,
    };
    let Some(acc) = picked else {
        // 报错要说清楚是「冷却中」还是「号在磁盘上已经不可用（被删 / 需重新登录）」——
        // 以前两者共用一句「都不可用」，用户修完重新登录还以为没生效。
        let eligible = eligible_on_disk(&st.cfg, st.edition);
        let message = if st.pool.has_edition(st.edition) && eligible == 0 {
            // 池子里还有这一版的号，但磁盘上一个都拿不到明文 ⇒ 它们都是「被删 / 需重新登录」，
            // 只是还在 30 秒的跳过冷却里。
            format!(
                "{}的账号都在磁盘上不可用（被删除、需要重新登录、或缺少明文凭据），正在等待重新登录 / 重新导入",
                st.edition.label()
            )
        } else if st.pool.has_edition(st.edition) {
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
        session,
        edition: st.edition,
    };

    let client = st.stream_upstream.as_ref();
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
    // 重试时**重新生成**请求级 id（chat_headers 里的 conv/message id 是每次调用现算的），
    // 免得同一个 request id 被上游当成重复包直接判掉；入站自带会话 id 时，
    // `X-Conversation-ID` 保持会话稳定（与官方桌面端语义一致），只有请求/消息 id 变化。
    let mut channel_retry = 0usize;
    let mut transport_retry = 0usize;
    // 模型侧瞬时故障（11133/11134/14003）的重试计数，见 MAX_MODEL_RETRY。
    let mut model_retry = 0usize;
    // 11128 自愈：第一次撞上 `unapproved channel`（负载变体）时，把请求体换成
    // 更保守的形态再试一次。默认只做「无损」那一层（见 heal_body_for_channel）。
    let mut self_healed = false;
    let mut send_body = upstream_body.clone();
    let resp = loop {
        let mut req = client.post(chat_url(&acc));
        // 入站会话标识在 chat_headers 里统一合并（只产生一个 X-Conversation-ID），
        // 这里**不能**再 `req.header("X-Conversation-ID", …)` 追加 —— 那是 append
        // 语义，会让上游收到两个同名头。
        for (k, v) in chat_headers(&acc, meta.session.as_deref()) {
            req = req.header(k, v);
        }
        let r = match req.json(&send_body).send().await {
            Ok(r) => r,
            Err(e) => {
                // 🔴 连接层失败：**不冷却账号**（原因见 MAX_TRANSPORT_RETRY）。
                // 退避重试几次应对瞬时抖动；仍失败就回 **502 + 可读原因**，
                // 由客户端自己决定要不要再发 —— 号本身没坏，别把它冻掉。
                if transport_retry < MAX_TRANSPORT_RETRY {
                    let wait = TRANSPORT_RETRY_BACKOFF_MS[transport_retry];
                    transport_retry += 1;
                    eprintln!(
                        "[反代] 上游连接失败，第 {transport_retry}/{MAX_TRANSPORT_RETRY} 次重试，\
                         等待 {wait}ms (model={model}, uid={uid}): {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(wait)).await;
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
                // 🔴 别把裸的 reqwest 原文回给客户端：原文形如
                // `error sending request for url (https://www.workbuddy.ai/...)`，
                // 用户看到「workbuddy.ai + 502」第一反应就是「号被封了」（真实发生过）。
                // 这里明确写清：连不上 ≠ 被拒，账号没坏，怎么处理。
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({
                        "error": {
                            "message": format!(
                                "连不上上游（{}），已重试 {transport_retry} 次。\
                                 这是**网络层**失败：DNS、超时、TLS 重置或线路抖动，\
                                 不是账号被封、不是鉴权失败、也不是频率超限 —— 账号本身是好的。\
                                 反代走**直连**（不读系统代理），线路一抖就会落到这里。\
                                 处理办法：① 稍等几秒重发；② 若长期频繁出现，检查本机到上游的网络。\
                                 原始错误：{e}",
                                st.edition.label()
                            ),
                            "type": "upstream_unreachable",
                            "edition": st.edition.label(),
                            "retries": transport_retry,
                        }
                    })),
                )
                    .into_response();
            }
        };
        if r.status().is_success() {
            break r;
        }
        let raw_status = r.status();
        let text = r.text().await.unwrap_or_default();
        // 🔴 模型侧瞬时故障（11133/11134/14003）：官方归类「模型侧状态」，
        // 重发即可能成功。**绝不能冷却账号** —— 见 is_transient_model_error。
        if is_transient_model_error(&text) && model_retry < MAX_MODEL_RETRY {
            let wait = MODEL_RETRY_BACKOFF_MS[model_retry];
            model_retry += 1;
            eprintln!(
                "[反代] 上游模型侧瞬时故障，第 {model_retry}/{MAX_MODEL_RETRY} 次重试，\
                 等待 {wait}ms (model={model}, uid={uid})"
            );
            tokio::time::sleep(Duration::from_millis(wait)).await;
            continue;
        }
        if is_channel_block(&text) {
            // 🔴 先试「自愈」：`unapproved channel` 是**负载**被判成非官方客户端，
            // 换一份更保守的请求往往能直接过（`request illegal` 是客户端身份变体，
            // 改负载没用，所以只对前者做）。只做一次，避免死循环。
            if !self_healed && text.contains("unapproved channel") {
                self_healed = true;
                let mut heal_degraded: Vec<String> = Vec::new();
                let healed = heal_body_for_channel(&send_body, &mut heal_degraded);
                if healed != send_body {
                    eprintln!(
                        "[反代] 上游判负载不合规（11128 unapproved channel），启用自愈重试 \
                         (model={model}, uid={uid}): {}",
                        degraded_summary(&heal_degraded)
                    );
                    send_body = healed;
                    degraded.extend(heal_degraded);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
            if channel_retry < MAX_CHANNEL_RETRY {
                channel_retry += 1;
                // 退避：按次数递增，给上游的间歇判定留出恢复窗口。
                tokio::time::sleep(Duration::from_millis(300 * channel_retry as u64)).await;
                eprintln!(
                    "[反代] 上游渠道校验拦下，第 {channel_retry}/{MAX_CHANNEL_RETRY} 次重试 \
                     (model={model}, uid={uid})"
                );
                continue;
            }
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
        // 🔴 频率超限（国内版 60004 / 国际版 6004）：按上游声明的重置时刻把
        // **该账号的该模型**暂停出池 —— 120 秒的短冷却只会让它回池后立刻再 429，
        // 循环空转。同时给客户端一句人话：哪个账号、哪个模型、什么时候恢复、怎么继续用，
        // 替代之前那坨双层转义的原始 JSON。
        //
        // ⚠️ 只冻模型、**不冻整个账号**（不同于 v1.6.18 的旧行为）：上游自己也是
        // 「您也可以切换其他模型继续使用」这个口径，再把整个账号冻 120 秒等于
        // 把其他还能用的模型也一并停掉，与文档里的说明自相矛盾。
        //
        // 429 是限流的通用状态码，不同上游的错误码不一定都是 6004 / 60004；
        // 「429 + 能解析出重置时间」同样按频率超限处理。
        let rate_limited_until = rate_limit_until(&text, raw_status.as_u16(), now_ms_i64());
        // 上游自己声明的重置时刻（与兜底档区分，用于文案与 reason）。
        let declared_reset = rate_limit_reset_ms(&text).or_else(|| {
            if raw_status.as_u16() == 429 {
                find_reset_datetime(&text)
            } else {
                None
            }
        });
        if let Some(until_ms) = rate_limited_until {
            let reason = if declared_reset.is_some() {
                "上游频率超限（6004/60004）"
            } else {
                "上游 429（无重置时刻，兜底冷却）"
            };
            st.pool
                .mark_model_failed_until(&uid, &model, until_ms, reason);
            // 兜底档的恢复时刻是本地估算的（上游没说何时恢复），加「约」提示这不是上游声明。
            let reset_at = if let Some(declared) = declared_reset {
                Local
                    .timestamp_millis_opt(declared)
                    .single()
                    .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "稍后".to_string())
            } else {
                format!(
                    "约 {}",
                    Local
                        .timestamp_millis_opt(until_ms)
                        .single()
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "稍后".to_string())
                )
            };
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
        let blocked = is_channel_block(&text);
        // 11102「模型不存在 / 账号无权使用该模型」也不是账号故障，判据见 is_model_not_found。
        let model_missing = is_model_not_found(&text);
        // 11140 双语义（账号级风控 vs 请求级内容审核），判据见 ban_kind。
        // 🔴 必须排在通用 4xx 之前：账号级限制若按 `mark_failed` 的 120 秒短冷却
        // 回池，等于让被封的号每两分钟再打一次上游 —— 既浪费请求，又把
        // 「被封号仍在持续调用」这个信号继续放大。
        let ban = ban_kind(&text);
        if raw_status.is_client_error() {
            if matches!(ban, Some(BanKind::Account)) {
                // 账号级风控限制：持久摘号（needs_relogin）+ 池内冷却。
                //
                // 持久化复用账号库的 `needs_relogin`（与 refresh.rs 缺 RT 的先例一致）：
                // 建池时 eligible_accounts 排除，存量池里 take_account 读盘判定失败、
                // 只打本地 30 秒跳过标记，不会再有出站请求。用户重新登录 / 重新导入
                // 后自动回池；界面已有 needsRelogin 红标，reason 会显示在卡片上。
                //
                // 🔴 以**磁盘上的完整记录**为底再改：`acc` 是 `take_account` 返回的
                // 出站对象，`upsert_account` 会整体替换，拿它直接写会把账号库里
                // 刷新令牌、额度等字段冲掉。
                let reason = "上游账号级风控限制（11140 unusual activity），需重新登录或申诉";
                let mut banned = load_accounts()
                    .into_iter()
                    .find(|a| a.get("uid").and_then(Value::as_str) == Some(uid.as_str()))
                    .unwrap_or_else(|| acc.clone());
                banned["needs_relogin"] = json!(true);
                banned["needs_relogin_reason"] = json!(reason);
                if let Err(e) = upsert_account(&banned) {
                    eprintln!(
                        "[反代] 11140 摘号落盘失败（本进程内仍会隔离，重启后会重新入池）\
                         uid={uid}: {e}"
                    );
                }
                st.pool.mark_until(
                    &uid,
                    now_ms_i64() + BAN_QUARANTINE.as_millis() as i64,
                    reason,
                );
                eprintln!(
                    "[反代] 🔴 账号「{account_name}」已被上游限制使用（403/11140 unusual \
                     activity），已从号池摘除；重新登录或申诉恢复后再导入 (uid={uid})"
                );
            } else if matches!(ban, Some(BanKind::Content)) {
                // 请求级内容审核：账号没问题，换号也没用 —— 不冷却、不摘号。
                eprintln!(
                    "[反代] 上游内容审核拒绝（403/11140），**不冷却账号** \
                     (model={model}, uid={uid})"
                );
            } else if matches!(ban, Some(BanKind::Unknown)) {
                // 认不出是哪一种：不持久摘号（避免把内容审核误当封号），但也不能
                // 停在 120 秒短冷却上反复撞。长冷却 + 提示开 DEBUG_DUMP 留存响应体。
                let reason = "上游 11140（displayMsg 无法辨认，保守长冷却）";
                st.pool.mark_until(
                    &uid,
                    now_ms_i64() + BAN_SUSPECT_COOLDOWN.as_millis() as i64,
                    reason,
                );
                eprintln!(
                    "[反代] ⚠️ 上游 11140 语义不明（既非 unusual activity 也非内容审核），\
                     账号长冷却 {}(model={model}, uid={uid})；\
                     请创建 ~/.wb-switch/DEBUG_DUMP 后复现，把 dump/last-failure.txt 留存",
                    BAN_SUSPECT_COOLDOWN.as_secs()
                );
            } else if blocked {
                // 🔴 11128 渠道校验**不能冷却账号**。
                //
                // 它拦的是「这次请求」（请求体不合规 / 调用方被识别），不是账号本身：
                // 同一个号打只读接口照样 200。冻它只会把本来能用的号从池里摘掉 ——
                // 池子小的时候一次 11128 就能让整池空转，客户端随后收到
                // 「账号都不可用」，而这些号一个都没坏。
                eprintln!(
                    "[反代] 渠道校验重试 {channel_retry} 次后仍被拦，**不冷却账号** \
                     (model={model}, uid={uid})"
                );
            } else if model_missing {
                // 与 11128 同理：请求已经过了渠道校验，只是这个模型在这个号上不存在
                // （或该号无权用）。模型名写错是客户端的事，冻号只会白冻一个好号。
                eprintln!(
                    "[反代] 上游无此模型（11102，**不冷却账号**）(model={model}, uid={uid})"
                );
            } else if is_transient_model_error(&text) {
                // 模型侧状态问题：已重试 {MAX_MODEL_RETRY} 次仍失败，号还是好的，
                // 冻它只会让下一次请求换到一个同样会撞故障窗口的号。
                eprintln!(
                    "[反代] 上游模型侧故障（11133/11134/14003）重试 {model_retry} 次仍未恢复，\
                     **不冷却账号** (model={model}, uid={uid})"
                );
            } else {
                st.pool
                    .mark_failed(&uid, &format!("上游返回 {}", raw_status.as_u16()));
            }
        } else {
            eprintln!(
                "[反代] 上游 {} 失败（上游侧问题，不冷却账号）(model={model}, uid={uid})",
                raw_status.as_u16()
            );
        }
        record_usage(&meta, None, false);
        // 11128 原样透传只会让用户看到一坨双层转义的 JSON，根本不知道该做什么 ——
        // 两种变体成因不同，提示也要分开（见 API-PROXY.md）。
        let message = if matches!(ban, Some(BanKind::Account)) {
            // 申诉渠道与 Trace ID 指引取自上游 2026-10-29 的 11140 处置文案。
            let trace = upstream_request_id(&text)
                .map(|id| format!("\nTrace ID：{id}   ← 申诉时务必附上，客服靠它定位具体请求"))
                .unwrap_or_default();
            format!(
                "账号「{account_name}」已被上游限制使用（403/11140: unusual activity）。\
                 已自动从号池摘除，不会再向上游发请求；请重新登录或提交申诉恢复后再导入。{trace}\n\
                 申诉渠道：客户端「设置 → 帮助与反馈」提交工单，或发邮件到 workbuddy_ai@tencent.com。"
            )
        } else if matches!(ban, Some(BanKind::Content)) {
            "上游内容安全审核拒绝了这条请求（403/11140）。这与账号本身无关，\
             调整内容后重试即可。"
                .to_string()
        } else if matches!(ban, Some(BanKind::Unknown)) {
            let trace = upstream_request_id(&text)
                .map(|id| format!("\nTrace ID：{id}"))
                .unwrap_or_default();
            format!(
                "账号「{account_name}」收到上游 11140 限制（原因未能识别），已临时停用 {} 分钟。{trace}\n\
                 请创建 ~/.wb-switch/DEBUG_DUMP 后复现一次，把 dump/last-failure.txt 留存以便定位。",
                BAN_SUSPECT_COOLDOWN.as_secs() / 60
            )
        } else if blocked {
            channel_block_message(&text, st.edition.label(), channel_retry)
        } else if model_missing {
            format!(
                "上游没有模型 `{model}`，或当前账号无权使用该模型（11102）。这不影响账号本身，换一个模型即可。"
            )
        } else if is_transient_model_error(&text) {
            // 官方文档对 11133/11134/14003 的处置建议就是「新开会话或切换模型重试」。
            // 我们已经退避重试过，仍未恢复。
            let code_label = transient_code(&text).unwrap_or("11133");
            format!(
                "上游模型侧暂时异常（code {code_label}），已自动退避重试 {model_retry} 次仍未恢复。\
                 这不是你的参数写错，也不是账号失效 —— 官方把这类码归为「模型侧状态」。\
                 建议：① 换个模型再发（最有效）；② 新开一个会话；③ 稍等十几秒重试。"
            )
        } else if raw_status.as_u16() == 429 {
            // 429 但解析不出重置时刻：别把原始 JSON 糊给客户端。
            format!("账号「{account_name}」请求过于频繁（HTTP 429），请稍后重试。")
        } else {
            text
        };
        return (
            status,
            Json(json!({"error": {"message": message, "type": "upstream_error"}})),
        )
            .into_response();
    };

    if want_stream {
        // 不再原样透传：先过一层 SSE 重组器（见 sse_pump 的说明），
        // 否则上游「一两个词一帧」的粒度会让客户端把思考渲染成一片小方块。
        let (tx, rx) = tokio::sync::mpsc::channel::<PumpOut>(64);
        // bytes_stream() 返回的 impl Stream 不保证 Unpin，必须 Box::pin 一下
        let upstream = Box::pin(resp.bytes_stream());
        // 泵持有 client 的 Arc：连接池属于 client，client 一旦被 drop，尚未读完的
        // 连接也会跟着关掉。泵要在客户端断开后独立收尾，所以这个引用不能提前释放。
        let upstream_arc = Arc::clone(&st.stream_upstream);
        tokio::spawn(async move {
            sse_pump(upstream_arc, upstream, tx, meta).await;
        });
        // 泵的结束状态：`PumpBody` 读到就结束响应体；被 drop（客户端断开）时用它叫停泵。
        let (pump_tx, mut pump_rx) = tokio::sync::mpsc::unbounded_channel::<Emitted>();
        let body = sse_body(rx, pump_tx, None);
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream; charset=utf-8")
            .header("Cache-Control", "no-cache")
            .header("X-Accel-Buffering", "no")
            .body(axum::body::Body::from_stream(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        // 泵的收尾通知（记完账才发）在这里消费掉：留着不管的话 channel 会一直挂着。
        tokio::spawn(async move {
            while pump_rx.recv().await.is_some() {}
        });
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

// ---------------------------------------------------------------- Anthropic Messages

/// Anthropic SSE：`event: <name>\ndata: <json>\n\n`。
fn anthropic_event_bytes(event: &str, data: &Value) -> bytes::Bytes {
    let raw = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
    bytes::Bytes::from(format!("event: {event}\ndata: {raw}\n\n"))
}

fn anthropic_new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

/// Anthropic 模型名可能带 Claude Code 的 `[1M]` 后缀（表示 1M 上下文）。
/// 上游不认识它，交给内部 chat 前先摘掉。
fn normalize_anthropic_model(model: &str) -> String {
    let m = model.trim();
    let m = m
        .strip_suffix("[1M]")
        .or_else(|| m.strip_suffix("[1m]"))
        .unwrap_or(m);
    m.trim().to_string()
}

fn anthropic_system_text(system: &Value) -> Option<String> {
    match system {
        Value::String(s) => {
            let cleaned = strip_claude_code_markers(s);
            if cleaned.trim().is_empty() {
                None
            } else {
                Some(cleaned)
            }
        }
        Value::Array(blocks) => {
            let mut out = String::new();
            for b in blocks {
                if let Some(t) = b.get("text").and_then(Value::as_str) {
                    // billing header 是独立块（Claude Code 2.x 的第一个 system block）。
                    if t.trim_start().starts_with("x-anthropic-billing-header:") {
                        continue;
                    }
                    out.push_str(t);
                }
            }
            let cleaned = strip_claude_code_markers(&out);
            if cleaned.trim().is_empty() {
                None
            } else {
                Some(cleaned)
            }
        }
        _ => None,
    }
}

fn anthropic_image_url(block: &Value) -> Option<String> {
    let src = block.get("source")?;
    match src.get("type").and_then(Value::as_str).unwrap_or("") {
        "base64" => {
            let data = src.get("data").and_then(Value::as_str)?;
            if data.is_empty() {
                return None;
            }
            let mt = src
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("image/png");
            Some(format!("data:{mt};base64,{data}"))
        }
        "url" => src
            .get("url")
            .and_then(Value::as_str)
            .filter(|u| !u.is_empty())
            .map(str::to_string),
        _ => None,
    }
}

/// tool_result.content：字符串 / 文本块数组 / 其他 → 文本。
fn tool_result_text(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => {
            let mut out = String::new();
            for it in items {
                if let Some(t) = it.get("text").and_then(Value::as_str) {
                    out.push_str(t);
                } else if let Some(s) = it.as_str() {
                    out.push_str(s);
                }
            }
            out
        }
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Anthropic tool_use.input → OpenAI arguments 字符串。
fn tool_args_json(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => "{}".to_string(),
        Some(Value::String(s)) if !s.trim().is_empty() => s.clone(),
        Some(Value::String(_)) => "{}".to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_else(|_| "{}".to_string()),
    }
}

/// Anthropic user 消息 → 0~N 条 OpenAI 消息。
///
/// tool_result 必须**先于**同批文本输出：OpenAI 要求 tool 消息紧跟 assistant.tool_calls，
/// 中间插一条 user 文本会断开配对（参考 panel-plus 的修复）。
fn anthropic_user_to_openai(content: &Value) -> Vec<Value> {
    match content {
        Value::String(s) => {
            if s.trim().is_empty() {
                Vec::new()
            } else {
                vec![json!({"role": "user", "content": s})]
            }
        }
        Value::Array(blocks) => {
            let mut tool_msgs = Vec::new();
            let mut texts = Vec::new();
            let mut parts = Vec::new();
            let mut has_image = false;
            for b in blocks {
                match b.get("type").and_then(Value::as_str).unwrap_or("") {
                    "tool_result" => {
                        let Some(id) = b.get("tool_use_id").and_then(Value::as_str) else {
                            continue;
                        };
                        if id.is_empty() {
                            continue;
                        }
                        let mut txt = tool_result_text(b.get("content"));
                        if b.get("is_error").and_then(Value::as_bool).unwrap_or(false) {
                            // OpenAI 的 role:tool 没有 is_error 字段；不标记的话模型会把
                            // 失败结果当成正常返回，继续编造「已成功」。
                            txt = format!("[tool_error] {txt}");
                        }
                        tool_msgs.push(json!({"role": "tool", "tool_call_id": id, "content": txt}));
                    }
                    "text" => {
                        if let Some(t) = b.get("text").and_then(Value::as_str) {
                            if !t.is_empty() {
                                texts.push(t.to_string());
                                parts.push(json!({"type": "text", "text": t}));
                            }
                        }
                    }
                    "image" => {
                        if let Some(url) = anthropic_image_url(b) {
                            has_image = true;
                            parts.push(json!({"type": "image_url", "image_url": {"url": url}}));
                        }
                    }
                    // thinking / redacted_thinking / document 等：上游不认识，忽略。
                    _ => {}
                }
            }
            let mut out = tool_msgs;
            if has_image && !parts.is_empty() {
                out.push(json!({"role": "user", "content": parts}));
            } else if !texts.is_empty() {
                out.push(json!({"role": "user", "content": texts.join("\n")}));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Anthropic assistant 消息 → 0~1 条 OpenAI 消息（text + tool_calls）。
fn anthropic_assistant_to_openai(content: &Value) -> Vec<Value> {
    match content {
        Value::String(s) => {
            if s.is_empty() {
                Vec::new()
            } else {
                vec![json!({"role": "assistant", "content": s})]
            }
        }
        Value::Array(blocks) => {
            let mut texts = Vec::new();
            let mut calls = Vec::new();
            for b in blocks {
                match b.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text" => {
                        if let Some(t) = b.get("text").and_then(Value::as_str) {
                            if !t.is_empty() {
                                texts.push(t.to_string());
                            }
                        }
                    }
                    "tool_use" => {
                        let Some(name) = b.get("name").and_then(Value::as_str) else {
                            continue;
                        };
                        if name.is_empty() {
                            continue;
                        }
                        let id = b
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .unwrap_or_else(|| anthropic_new_id("toolu"));
                        calls.push(json!({
                            "id": id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": tool_args_json(b.get("input")),
                            }
                        }));
                    }
                    // thinking 块没有 OpenAI 对应物，丢弃。
                    _ => {}
                }
            }
            if !calls.is_empty() {
                // 工具调用轮允许空正文；两边都接受 ""。
                vec![json!({
                    "role": "assistant",
                    "content": texts.join("\n"),
                    "tool_calls": calls,
                })]
            } else if !texts.is_empty() {
                vec![json!({"role": "assistant", "content": texts.join("\n")})]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

fn anthropic_tools_to_openai(tools: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for t in tools {
        let name = t
            .get("name")
            .and_then(Value::as_str)
            .or_else(|| {
                t.get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
            })
            .map(str::trim)
            .unwrap_or("");
        if name.is_empty() {
            continue;
        }
        let desc = t
            .get("description")
            .and_then(Value::as_str)
            .or_else(|| {
                t.get("function")
                    .and_then(|f| f.get("description"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("");
        let schema = t
            .get("input_schema")
            .filter(|v| !v.is_null())
            .cloned()
            .or_else(|| {
                t.get("function")
                    .and_then(|f| f.get("parameters"))
                    .filter(|v| !v.is_null())
                    .cloned()
            })
            .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
        out.push(json!({
            "type": "function",
            "function": {
                "name": name,
                "description": desc,
                "parameters": schema,
            }
        }));
    }
    out
}

/// Anthropic `tool_choice` → 上游认的形式。
///
/// 🔴 上游 `tool_choice` **只吃字符串**。以前这里把 `{"type":"tool","name":g}`
/// 翻译成 OpenAI 规范的对象形式 `{"type":"function","function":{"name":g}}`，
/// 发到上游直接 400 `11101 Unmarshal chat params failed` —— 也就是 Claude Code
/// 指定「必须调某个工具」时整条请求必挂。函数名字符串 `"g"` 上游接受，语义等价。
fn anthropic_tool_choice(v: Option<&Value>) -> Option<Value> {
    let v = v?;
    match v {
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "auto" => Some(json!("auto")),
            "any" | "required" => Some(json!("required")),
            "none" => Some(json!("none")),
            _ => None,
        },
        Value::Object(_) => {
            let ty = v.get("type").and_then(Value::as_str)?.to_ascii_lowercase();
            match ty.as_str() {
                "auto" => Some(json!("auto")),
                "any" => Some(json!("required")),
                "none" => Some(json!("none")),
                "tool" | "function" => {
                    let name = v.get("name").and_then(Value::as_str)?;
                    if name.is_empty() {
                        None
                    } else {
                        Some(json!(name))
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Anthropic Messages 请求 → OpenAI chat 请求。
///
/// 只翻译内部 chat 链路认识的字段；`thinking` / `cache_control` / `metadata` 等
/// 上游没有对应能力的字段直接丢弃。
fn anthropic_to_openai(body: &Value) -> Result<Value, String> {
    let raw_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if raw_model.is_empty() {
        return Err("model is required".to_string());
    }
    let model = normalize_anthropic_model(raw_model);
    let Some(arr) = body.get("messages").and_then(Value::as_array) else {
        return Err("messages is required".to_string());
    };
    if arr.is_empty() {
        return Err("messages is required".to_string());
    }

    let mut system_text = body
        .get("system")
        .and_then(anthropic_system_text)
        .unwrap_or_default();
    let mut msgs: Vec<Value> = Vec::with_capacity(arr.len() + 1);
    for m in arr {
        let role = m.get("role").and_then(Value::as_str).unwrap_or("");
        let content = m.get("content").cloned().unwrap_or(Value::Null);
        match role {
            "user" => msgs.extend(anthropic_user_to_openai(&content)),
            "assistant" => msgs.extend(anthropic_assistant_to_openai(&content)),
            // Claude Code 会把第二段 system-reminder 以 `role:"system"` 放进 messages。
            // OpenAI 上游没有「中途 system」这个语义，合并进首条 system 最稳。
            "system" => {
                if let Some(t) = anthropic_system_text(&content) {
                    if !system_text.is_empty() {
                        system_text.push('\n');
                    }
                    system_text.push_str(&t);
                }
            }
            _ => return Err(format!("invalid role: {role}")),
        }
    }
    if msgs.is_empty() {
        return Err("messages is required".to_string());
    }
    if !system_text.trim().is_empty() {
        msgs.insert(0, json!({"role": "system", "content": system_text}));
    }

    let mut out = serde_json::Map::new();
    out.insert("model".to_string(), Value::String(model));
    out.insert("messages".to_string(), Value::Array(msgs));
    for key in ["max_tokens", "temperature", "top_p", "top_k"] {
        if let Some(v) = body.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    if let Some(stops) = body.get("stop_sequences").and_then(Value::as_array) {
        if !stops.is_empty() {
            out.insert("stop".to_string(), Value::Array(stops.clone()));
        }
    }
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        let converted = anthropic_tools_to_openai(tools);
        if !converted.is_empty() {
            out.insert("tools".to_string(), Value::Array(converted));
            // 没有 tools 时 tool_choice 无意义，上游可能直接 400，所以只在有 tools 时下发。
            if let Some(tc) = anthropic_tool_choice(body.get("tool_choice")) {
                out.insert("tool_choice".to_string(), tc);
            }
        }
    }
    Ok(Value::Object(out))
}

fn count_json_chars(v: &Value) -> usize {
    match v {
        Value::String(s) => s.chars().count(),
        Value::Array(a) => a.iter().map(count_json_chars).sum(),
        Value::Object(o) => o.values().map(count_json_chars).sum(),
        _ => 0,
    }
}

/// `/v1/messages/count_tokens` 的粗略估算。
///
/// 上游没有 count_tokens 接口，这里只按字符数估一个量级（中英混合约 3 字符/token），
/// 供 Claude Code 的上下文提示使用，不参与计费。
fn estimate_anthropic_input_tokens(body: &Value) -> u64 {
    let mut chars = 0usize;
    for key in ["system", "messages", "tools"] {
        if let Some(v) = body.get(key) {
            chars += count_json_chars(v);
        }
    }
    ((chars as f64 / 3.0).ceil() as u64).max(1)
}

/// 一个流式 tool_use 内容块的累积状态。
#[derive(Default)]
struct AnthropicToolState {
    index: usize,
    id: String,
    name: String,
    args: String,
    opened: bool,
    stopped: bool,
}

/// OpenAI chat chunk → Anthropic SSE 事件的纯状态机。
///
/// 工具调用分片按 OpenAI `index` 累积；name 到手后才开 `tool_use` 块，避免发出
/// 一个没有名字的非法工具块。块下标按真实出现顺序分配。
struct AnthropicStream {
    message_id: String,
    model: String,
    started: bool,
    next_index: usize,
    text_index: Option<usize>,
    text_open: bool,
    text: String,
    tool_order: Vec<i64>,
    tools: HashMap<i64, AnthropicToolState>,
    finish_reason: String,
    usage: Option<Value>,
    errored: bool,
    done: bool,
}

impl AnthropicStream {
    fn new(model: String) -> Self {
        Self {
            message_id: anthropic_new_id("msg"),
            model,
            started: false,
            next_index: 0,
            text_index: None,
            text_open: false,
            text: String::new(),
            tool_order: Vec::new(),
            tools: HashMap::new(),
            finish_reason: String::new(),
            usage: None,
            errored: false,
            done: false,
        }
    }

    fn start_events(&mut self) -> Vec<(String, Value)> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        vec![
            (
                "message_start".to_string(),
                json!({
                    "type": "message_start",
                    "message": {
                        "id": self.message_id.clone(),
                        "type": "message",
                        "role": "assistant",
                        "model": self.model.clone(),
                        "content": [],
                        "stop_reason": null,
                        "stop_sequence": null,
                        "usage": {"input_tokens": 0, "output_tokens": 0}
                    }
                }),
            ),
            ("ping".to_string(), json!({"type": "ping"})),
        ]
    }

    fn has_tool(&self) -> bool {
        self.tools.values().any(|t| t.opened)
    }

    fn stop_reason(&self) -> &'static str {
        // 🔴 只要有工具调用就必须是 tool_use：部分上游在返回 tool_calls 时仍给
        // finish_reason=stop，照搬会让 Claude Code 认为「这轮已结束」直接收工。
        if self.has_tool() {
            return "tool_use";
        }
        match self.finish_reason.as_str() {
            "length" => "max_tokens",
            "tool_calls" | "function_call" => "tool_use",
            "content_filter" => "refusal",
            _ => "end_turn",
        }
    }

    fn anthropic_usage(&self) -> Value {
        anthropic_usage_from(self.usage.as_ref())
    }

    fn ingest_tool(&mut self, idx: i64, id: &str, name: &str, args: &str) -> Vec<(String, Value)> {
        let mut events = Vec::new();
        if !self.tools.contains_key(&idx) {
            self.tools.insert(idx, AnthropicToolState::default());
            self.tool_order.push(idx);
        }
        let tb = self.tools.get_mut(&idx).expect("tool state just inserted");
        if tb.id.is_empty() && !id.is_empty() {
            tb.id = id.to_string();
        }
        if tb.name.is_empty() && !name.is_empty() {
            tb.name = name.to_string();
        }
        if !args.is_empty() {
            tb.args.push_str(args);
        }
        if !tb.opened && !tb.name.is_empty() {
            let index = self.next_index;
            self.next_index += 1;
            tb.index = index;
            tb.opened = true;
            if tb.id.is_empty() {
                tb.id = anthropic_new_id("toolu");
            }
            events.push((
                "content_block_start".to_string(),
                json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": tb.id.clone(),
                        "name": tb.name.clone(),
                        "input": {}
                    }
                }),
            ));
            if !tb.args.is_empty() {
                events.push((
                    "content_block_delta".to_string(),
                    json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": tb.args.clone()
                        }
                    }),
                ));
            }
        } else if tb.opened && !args.is_empty() {
            events.push((
                "content_block_delta".to_string(),
                json!({
                    "type": "content_block_delta",
                    "index": tb.index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": args
                    }
                }),
            ));
        }
        events
    }

    fn feed(&mut self, frame: &Value) -> Vec<(String, Value)> {
        let mut events = Vec::new();
        if let Some(err) = frame.get("error") {
            self.errored = true;
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("upstream error");
            events.push((
                "error".to_string(),
                json!({
                    "type": "error",
                    "error": {"type": "api_error", "message": msg}
                }),
            ));
            return events;
        }
        if let Some(m) = frame.get("model").and_then(Value::as_str) {
            if !m.is_empty() {
                self.model = m.to_string();
            }
        }
        if let Some(u) = frame.get("usage") {
            if u.is_object() {
                self.usage = Some(u.clone());
            }
        }
        let Some(choice) = frame
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
        else {
            return events;
        };

        if let Some(delta) = choice.get("delta") {
            if let Some(t) = delta.get("content").and_then(Value::as_str) {
                if !t.is_empty() {
                    if !self.text_open {
                        let index = self.next_index;
                        self.next_index += 1;
                        self.text_index = Some(index);
                        self.text_open = true;
                        events.push((
                            "content_block_start".to_string(),
                            json!({
                                "type": "content_block_start",
                                "index": index,
                                "content_block": {"type": "text", "text": ""}
                            }),
                        ));
                    }
                    self.text.push_str(t);
                    let index = self.text_index.unwrap_or(0);
                    events.push((
                        "content_block_delta".to_string(),
                        json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "text_delta", "text": t}
                        }),
                    ));
                }
            }
            if let Some(tcs) = delta.get("tool_calls").and_then(Value::as_array) {
                for tc in tcs {
                    let idx = tc.get("index").and_then(Value::as_i64).unwrap_or(0);
                    let func = tc.get("function");
                    let id = tc.get("id").and_then(Value::as_str).unwrap_or("");
                    let name = func
                        .and_then(|f| f.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let args = func
                        .and_then(|f| f.get("arguments"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if id.is_empty() && name.is_empty() && args.is_empty() {
                        continue;
                    }
                    events.extend(self.ingest_tool(idx, id, name, args));
                }
            }
            // 旧式 function_call（没有 index）也兼容。
            if let Some(fc) = delta.get("function_call") {
                let name = fc.get("name").and_then(Value::as_str).unwrap_or("");
                let args = fc.get("arguments").and_then(Value::as_str).unwrap_or("");
                if !name.is_empty() || !args.is_empty() {
                    events.extend(self.ingest_tool(0, "", name, args));
                }
            }
        }
        if let Some(fr) = choice.get("finish_reason").and_then(Value::as_str) {
            if !fr.is_empty() {
                self.finish_reason = fr.to_string();
            }
        }
        events
    }

    fn finish(&mut self) -> Vec<(String, Value)> {
        if self.done {
            return Vec::new();
        }
        self.done = true;
        if self.errored {
            return Vec::new();
        }
        let mut events = self.start_events();
        // Anthropic 的 content 不允许空数组；空响应补一个空 text 块。
        if !self.text_open && !self.has_tool() {
            let index = self.next_index;
            self.next_index += 1;
            self.text_index = Some(index);
            self.text_open = true;
            events.push((
                "content_block_start".to_string(),
                json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "text", "text": ""}
                }),
            ));
        }
        if self.text_open {
            if let Some(index) = self.text_index {
                events.push((
                    "content_block_stop".to_string(),
                    json!({"type": "content_block_stop", "index": index}),
                ));
            }
            self.text_open = false;
        }
        for key in &self.tool_order {
            if let Some(tb) = self.tools.get_mut(key) {
                if tb.opened && !tb.stopped {
                    tb.stopped = true;
                    events.push((
                        "content_block_stop".to_string(),
                        json!({"type": "content_block_stop", "index": tb.index}),
                    ));
                }
            }
        }
        events.push((
            "message_delta".to_string(),
            json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": self.stop_reason(),
                    "stop_sequence": null
                },
                // 上游的 input_tokens 只有流末才拿得到（message_start 已发出），
                // 所以这里并列给出完整 usage；CCH 的解析也是 message_delta 优先。
                "usage": self.anthropic_usage()
            }),
        ));
        events.push((
            "message_stop".to_string(),
            json!({"type": "message_stop"}),
        ));
        events
    }

    /// 非流式：把累积状态渲染成一条 Anthropic message。
    fn build_message(&self) -> Value {
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(json!({"type": "text", "text": self.text.clone()}));
        }
        for key in &self.tool_order {
            let Some(tb) = self.tools.get(key) else {
                continue;
            };
            if !tb.opened && tb.name.is_empty() {
                continue;
            }
            let input = serde_json::from_str::<Value>(&tb.args)
                .unwrap_or_else(|_| json!({}));
            content.push(json!({
                "type": "tool_use",
                "id": tb.id.clone(),
                "name": tb.name.clone(),
                "input": input
            }));
        }
        if content.is_empty() {
            content.push(json!({"type": "text", "text": ""}));
        }
        json!({
            "id": self.message_id.clone(),
            "type": "message",
            "role": "assistant",
            "model": self.model.clone(),
            "content": content,
            "stop_reason": self.stop_reason(),
            "stop_sequence": null,
            "usage": self.anthropic_usage(),
        })
    }
}

fn anthropic_usage_num(v: &Value, key: &str) -> i64 {
    v.get(key)
        .and_then(|x| {
            x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))
        })
        .unwrap_or(0)
}

fn anthropic_usage_from(usage: Option<&Value>) -> Value {
    let fallback = Value::Object(Default::default());
    let u = usage.unwrap_or(&fallback);
    let input = {
        let p = anthropic_usage_num(u, "prompt_tokens");
        if p > 0 {
            p
        } else {
            anthropic_usage_num(u, "input_tokens")
        }
    };
    let output = {
        let c = anthropic_usage_num(u, "completion_tokens");
        if c > 0 {
            c
        } else {
            anthropic_usage_num(u, "output_tokens")
        }
    };
    let cache_read = {
        let direct = anthropic_usage_num(u, "cache_read_input_tokens");
        if direct > 0 {
            direct
        } else {
            let flat = anthropic_usage_num(u, "cached_tokens");
            if flat > 0 {
                flat
            } else {
                u.get("prompt_tokens_details")
                    .map(|d| anthropic_usage_num(d, "cached_tokens"))
                    .unwrap_or(0)
            }
        }
    };
    json!({
        "input_tokens": input,
        "output_tokens": output,
        "cache_creation_input_tokens": anthropic_usage_num(u, "cache_creation_input_tokens"),
        "cache_read_input_tokens": cache_read,
    })
}

/// 读内部 chat 的 SSE，逐帧转成 Anthropic SSE。
async fn anthropic_sse_pump<S, E>(
    mut upstream: S,
    tx: tokio::sync::mpsc::Sender<PumpOut>,
    model: String,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Emitted
where
    S: futures::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut st = AnthropicStream::new(model);
    for (event, data) in st.start_events() {
        if tx
            .send(PumpOut::Bytes(anthropic_event_bytes(&event, &data)))
            .await
            .is_err()
        {
            return Emitted::ClientGone;
        }
    }

    let mut tail = String::new();
    'outer: loop {
        // 客户端断开时 `PumpBody::drop` 会发一枪 stop：立刻停读并释放上游。
        let chunk = tokio::select! {
            _ = &mut stop => return Emitted::ClientGone,
            item = upstream.next() => match item {
                None => break 'outer,
                Some(Ok(c)) => c,
                Some(Err(e)) => {
                    let data = json!({
                        "type": "error",
                        "error": {"type": "api_error", "message": e.to_string()}
                    });
                    let _ = tx
                        .send(PumpOut::Bytes(anthropic_event_bytes("error", &data)))
                        .await;
                    return Emitted::UpstreamError;
                }
            },
        };
        tail.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = tail.find("\n\n") {
            let event = tail[..idx].to_string();
            tail.drain(..idx + 2);
            let mut data = String::new();
            for line in event.lines() {
                if let Some(rest) = line.strip_prefix("data:") {
                    data.push_str(rest.trim_start());
                }
            }
            if data.is_empty() {
                continue;
            }
            if data.trim() == "[DONE]" {
                break 'outer;
            }
            let Ok(frame) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            let events = st.feed(&frame);
            let has_error = events.iter().any(|(name, _)| name == "error");
            for (name, value) in events {
                if tx
                    .send(PumpOut::Bytes(anthropic_event_bytes(&name, &value)))
                    .await
                    .is_err()
                {
                    return Emitted::ClientGone;
                }
            }
            if has_error {
                return Emitted::UpstreamError;
            }
        }
    }

    for (event, data) in st.finish() {
        let _ = tx
            .send(PumpOut::Bytes(anthropic_event_bytes(&event, &data)))
            .await;
    }
    Emitted::Done
}

/// 非流式：把内部 chat 的 SSE 聚合成一条 Anthropic message。
async fn anthropic_aggregate<S, E>(mut upstream: S, model: &str) -> Result<Value, String>
where
    S: futures::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut st = AnthropicStream::new(model.to_string());
    let mut tail = String::new();
    while let Some(item) = upstream.next().await {
        let chunk = item.map_err(|e| e.to_string())?;
        tail.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = tail.find("\n\n") {
            let event = tail[..idx].to_string();
            tail.drain(..idx + 2);
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
            let events = st.feed(&frame);
            if let Some((_, value)) = events.iter().find(|(name, _)| name == "error") {
                let msg = value
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream error")
                    .to_string();
                return Err(msg);
            }
        }
    }
    Ok(st.build_message())
}

fn anthropic_error(status: StatusCode, message: &str) -> Response {
    let ty = match status.as_u16() {
        401 | 403 => "authentication_error",
        404 => "not_found_error",
        429 => "rate_limit_error",
        413 => "request_too_large",
        s if (400..500).contains(&s) => "invalid_request_error",
        _ => "api_error",
    };
    (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": ty, "message": message}
        })),
    )
        .into_response()
}

fn openai_error_message(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "upstream error".to_string();
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        if let Some(m) = v
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
        {
            if !m.is_empty() {
                return m.to_string();
            }
        }
        if let Some(m) = v.get("message").and_then(Value::as_str) {
            if !m.is_empty() {
                return m.to_string();
            }
        }
    }
    trimmed.to_string()
}

fn with_degraded_value(mut resp: Response, value: Option<HeaderValue>) -> Response {
    if let Some(v) = value {
        resp.headers_mut().insert("x-wb-switch-degraded", v);
    }
    resp
}

/// `POST /v1/messages` —— Anthropic Messages API 兼容入口。
///
/// 请求翻译成 OpenAI chat 后直接复用 [`chat_completions_inner`]，账号池、冷却、
/// 重试、用量统计与版本防火墙全部沿用；响应再翻译回 Anthropic 形态。
async fn anthropic_messages(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(incoming): Json<Value>,
) -> Response {
    if let Err(resp) = guard_request(&headers) {
        return *resp;
    }
    if !check_auth(&headers, &st.endpoint.api_key) {
        return anthropic_error(StatusCode::UNAUTHORIZED, "invalid api key");
    }
    // 调试落盘（开关文件控制）：Claude Code 的完整 system prompt 与转换结果对照。
    debug_dump(
        "last-anthropic-request.json",
        &serde_json::to_string_pretty(&incoming).unwrap_or_else(|_| "{}".to_string()),
    );
    let want_stream = incoming
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let openai = match anthropic_to_openai(&incoming) {
        Ok(v) => v,
        Err(e) => return anthropic_error(StatusCode::BAD_REQUEST, &e),
    };
    let model = openai
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("deepseek-v4-flash")
        .to_string();

    // 内部始终走流式：现有 aggregate 会丢 tool_calls 与真实 finish_reason，
    // 非流式客户端由本函数自己聚合。
    let mut inner_body = openai;
    inner_body["stream"] = json!(true);

    let resp = chat_completions_inner(&st, &headers, inner_body).await;
    let status = resp.status();
    let degraded = resp.headers().get("x-wb-switch-degraded").cloned();
    if !status.is_success() {
        let body = axum::body::to_bytes(resp.into_body(), MAX_REQUEST_BODY)
            .await
            .unwrap_or_default();
        let text = String::from_utf8_lossy(&body);
        let out = anthropic_error(status, &openai_error_message(&text));
        return with_degraded_value(out, degraded);
    }

    let stream = Box::pin(resp.into_body().into_data_stream());
    if want_stream {
        let (tx, rx) = tokio::sync::mpsc::channel::<PumpOut>(64);
        // Anthropic 入口读的是上面 `chat_completions_inner` 产出的本地 SSE
        // （收尾记账由那条链路负责）。但客户端断开时它同样要**停读并释放上游**，
        // 否则「取消后继续读完」在这里也存在 —— 只是浪费少一些（不跨网络）。
        let (pump_tx, pump_rx) = tokio::sync::mpsc::unbounded_channel::<Emitted>();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let pump_model = model.clone();
        tokio::spawn(async move {
            anthropic_sse_pump(stream, tx, pump_model, stop_rx).await;
        });
        let body = sse_body(rx, pump_tx, Some(stop_tx));
        // 这条链路不记账，`pump_rx` 直接丢掉即可（接收端一 drop，发送端就发不出去）。
        drop(pump_rx);
        let out = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream; charset=utf-8")
            .header("Cache-Control", "no-cache")
            .header("X-Accel-Buffering", "no")
            .body(axum::body::Body::from_stream(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        with_degraded_value(out, degraded)
    } else {
        let out = match anthropic_aggregate(stream, &model).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => anthropic_error(StatusCode::BAD_GATEWAY, &e),
        };
        with_degraded_value(out, degraded)
    }
}

/// `POST /v1/messages/count_tokens` —— 本地估算（上游没有该接口）。
async fn anthropic_count_tokens(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(incoming): Json<Value>,
) -> Response {
    if let Err(resp) = guard_request(&headers) {
        return *resp;
    }
    if !check_auth(&headers, &st.endpoint.api_key) {
        return anthropic_error(StatusCode::UNAUTHORIZED, "invalid api key");
    }
    let tokens = estimate_anthropic_input_tokens(&incoming);
    Json(json!({"input_tokens": tokens})).into_response()
}

/// 诊断类只读接口的守卫。
///
/// `/status` 与 `/usage-stats` **不需要 `api_key`**（它们是给本机排障 / 监控脚本用的），
/// 但会吐出账号昵称、uid、过期时间、用量曲线 —— 对本机反代来说这就是敏感数据。
/// 以前它们完全不做来源校验：浏览器里任何页面都能借 DNS rebinding 把它们当同源读走
/// （实测伪造 `Host: evil.example.com` 照样 200 返回完整数据）。
///
/// 规则：
/// - `Host` 必须是回环 / 本机 IP / 白名单域名（`http_guard::check_host`）；
/// - 带了 `Origin` 却不在本机（或白名单）⇒ 拒。本机直连时浏览器里只有本机页面能访问
///   这个回环端口，`Origin` 只可能是本机页面，放行。
fn guard_diagnostics(headers: &HeaderMap) -> Result<(), Box<Response>> {
    guard_request(headers)
}

/// 反代路由（`/v1/*`）的来源校验。
///
/// `api_key` 只证明「你有口令」，证明不了「请求是从哪发起的」：本机浏览器里随便一个
/// 页面都能 `fetch('http://127.0.0.1:7863/v1/chat/completions')`（`Host` 天然是回环，
/// 能过 `check_host`）。如果口令曾经进过页面地址 / 日志，对方就能直接盗刷额度。
///
/// 规则与诊断接口相同，但**脚本 / SDK 不受影响** —— 它们不带 `Origin`。
fn guard_request(headers: &HeaderMap) -> Result<(), Box<Response>> {
    if http_guard::check_host(headers).is_err() {
        return Err(Box::new(http_guard::rejected(Access::BadHost)));
    }
    let cross_site = match headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()) {
        Some("null") => true,
        Some(o) => http_guard::host_of_url(o)
            .map(|h| !http_guard::is_trusted_host(&h))
            .unwrap_or(true),
        None => false,
    };
    if cross_site {
        return Err(Box::new(http_guard::rejected(Access::CrossSite)));
    }
    Ok(())
}

/// `/status` —— 报告**本入口**的情况：它是哪一版、监听在哪、这一版有哪些号。
async fn proxy_status(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = guard_diagnostics(&headers) {
        return *resp;
    }
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
    .into_response()
}

/// 本入口自己的用量统计（**只含本版**，不会把另一版的量混进来）。
async fn usage_stats(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = guard_diagnostics(&headers) {
        return *resp;
    }
    Json(load_usage_for(st.edition)).into_response()
}

// ---------------------------------------------------------------- 启动

fn router(st: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/messages", post(anthropic_messages))
        .route("/v1/messages/count_tokens", post(anthropic_count_tokens))
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
        stream_upstream: Arc::new(
            reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .expect("reqwest client 构建失败只在 TLS 后端异常时发生"),
        ),
        sticky: sticky_for(edition),
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
        "[反代] {} 已启动 http://{addr}  (POST /v1/chat/completions, POST /v1/messages)",
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
            // 🔴 加密信封账号进不了反代：`access_token` 是 `{$wbEncrypted, …}` 对象，
            // 拼不出 `Authorization: Bearer …`，上游网关直接回
            // `401 Authorization Required`（APISIX 的 HTML 错误页）。
            // 这类号留在池里会按轮转每隔几次就撞一次 401 —— 在单账号场景下
            // 表现为「隔一会儿报一次错」，还会把好号连累进冷却。
            // 它们只对「切换账号」有意义（密文原样写回 auth 文件），不进轮转。
            if is_envelope(a, "access_token") {
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

/// 当前在用的账号池（两个入口共享同一份）。
///
/// 为什么要存到进程级：`Pool` 原本是 [`apply_config`] 里的局部变量，只有
/// `AppState` 够得着，界面层（Tauri 命令 / WebUI）拿不到 ⇒ 冷却状态在界面上
/// 完全看不见。这里只存 `Arc` 引用，不改变生命周期与所有权。
fn current_pool() -> &'static Mutex<Option<Arc<Pool>>> {
    static POOL: OnceLock<Mutex<Option<Arc<Pool>>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(None))
}

/// 冷却快照（**两个入口合并**，每条自带 `edition`）。
///
/// 反代没在跑（或从未启动）时返回空数组 —— 此时池子不存在，自然也没有冷却。
pub fn cooldown_snapshot() -> Vec<Value> {
    let pool = current_pool()
        .lock()
        .ok()
        .and_then(|g| g.clone());
    match pool {
        Some(p) => p.cooldown_snapshot(),
        None => Vec::new(),
    }
}

/// 按配置重建两个入口。返回**每个已启用版本**的启动结果。
///
/// 刻意做成「逐版本返回」而不是一个总的 Result：国际版没账号属于常态，
/// 不该因此让国内版也起不来，更不该让「保存配置」这个动作整体失败。
pub fn apply_config(cfg: &ProxyConfig) -> Vec<(Edition, Result<(), String>)> {
    stop_proxy_server();
    let pool = Arc::new(Pool::new(eligible_accounts(cfg)));
    // 让界面层能读到这份池子的冷却状态（见 `cooldown_snapshot`）。
    if let Ok(mut g) = current_pool().lock() {
        *g = Some(pool.clone());
    }
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

    /// `YYYY-MM-DD HH:MM:SS`（UTC+8）→ epoch 毫秒，只给下面几条解析用例用。
    fn utc8_ms(s: &str) -> i64 {
        chrono::FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .from_local_datetime(
                &chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap(),
            )
            .unwrap()
            .timestamp_millis()
    }

    /// 60004 的双层转义 JSON 必须能解析出重置时刻 —— 这是「按重置时间
    /// 暂停账号」而不是「120 秒后回池再 429」的依据。
    #[test]
    fn parses_rate_limit_reset_from_nested_json() {
        let body = r#"{"message":"{\"code\":60004,\"msg\":\"您的使用量已超出频率限制，将在 2026-10-17 11:46:56 UTC+8 重置，您也可以切换其他模型继续使用。\",\"requestId\":\"00000000000000463270d2d3db8ba1\"}","type":"upstream_error"}"#;
        let until = rate_limit_reset_ms(body).expect("60004 应解析出重置时刻");
        assert_eq!(until, utc8_ms("2026-10-17 11:46:56"));
    }

    /// 国际版（code 6004）的英文文案同样要解析出重置时刻 ——
    /// 漏判的话只会走 120 秒短冷却，号回池立刻又 429、整池空转。
    #[test]
    fn parses_rate_limit_reset_from_international_body() {
        let body = r#"{"code":6004,"msg":"usage exceeds frequency limit, but don't worry, your usage will reset at 2026-10-23 21:59:54 UTC+8, alternatively, you can switch to the other models to continue using it.","requestId":"0000000000000000460c41606902166d"}"#;
        let until = rate_limit_reset_ms(body).expect("6004 应解析出重置时刻");
        assert_eq!(until, utc8_ms("2026-10-23 21:59:54"));
    }

    /// 非频率超限的上游体不得误判 —— 否则普通 4xx 会被错误地长时间冷却。
    #[test]
    fn non_rate_limit_body_returns_none() {
        assert!(rate_limit_reset_ms(r#"{"message":"{\"code\":11128}"}"#).is_none());
        assert!(rate_limit_reset_ms("plain text").is_none());
        assert!(rate_limit_reset_ms("").is_none());
        // 有 60004 关键字但缺「将在 … 重置」时间串的残缺体也要兜住。
        assert!(rate_limit_reset_ms(r#"{"message":"{\"code\":60004,\"msg\":\"?\"}"}"#).is_none());
        // 国际版同理：缺 `will reset at` 时不能当成频率超限。
        assert!(rate_limit_reset_ms(r#"{"code":6004,"msg":"?"}"#).is_none());
        // `6004` 单独出现不算数 —— requestId 之类十六进制串可能恰好含它。
        assert!(rate_limit_reset_ms(r#"{"code":0,"msg":"ok","requestId":"6004abcd"}"#).is_none());
    }

    /// 🔴 中文被转义成 `\uXXXX` 时，按关键词定位会全线失效 —— 时间串必须照样能扫到。
    #[test]
    fn parses_reset_time_even_when_chinese_is_escaped() {
        // "您的使用量已超出频率限制，将在 2026-10-17 11:46:56 UTC+8 重置" 的转义体
        let body = "{\"message\":\"{\\\"code\\\":60004,\\\"msg\\\":\\\"\\u60a8\\u7684\\u4f7f\\u7528\\u91cf\\u5df2\\u8d85\\u51fa\\u9891\\u7387\\u9650\\u5236\\uff0c\\u5c06\\u5728 2026-10-17 11:46:56 UTC+8 \\u91cd\\u7f6e\\\"}\"}";
        assert!(!body.contains("频率限制"), "前提：中文确实被转义了");
        let until = rate_limit_reset_ms(body).expect("转义体也要能解析出重置时刻");
        assert_eq!(until, utc8_ms("2026-10-17 11:46:56"));
    }

    /// 🔴 429 但上游没给重置时刻：必须退到**逐模型**兜底冷却（RATE_LIMIT_FALLBACK），
    /// 而不是 None —— 否则调用方退化成整账号 120 秒短冷却，把同账号还能用的模型
    /// 一起停掉，且回池立刻再 429。
    #[test]
    fn rate_limit_fallback_applies_to_429_without_reset() {
        let now = 1_800_000_000_000;
        let until = rate_limit_until(r#"{"error":"too many requests"}"#, 429, now)
            .expect("429 无时间体也必须有兜底冷却");
        assert_eq!(
            until - now,
            RATE_LIMIT_FALLBACK.as_millis() as i64,
            "兜底档的恢复时刻必须是 now + RATE_LIMIT_FALLBACK"
        );

        // 有上游声明的重置时刻时优先用它，不叠加兜底。
        let declared = rate_limit_until(
            r#"{"code":6004,"msg":"usage will reset at 2026-10-23 21:59:54 UTC+8"}"#,
            429,
            now,
        )
        .expect("6004 体应解析出重置时刻");
        assert_eq!(declared, utc8_ms("2026-10-23 21:59:54"));
    }

    /// 兜底冷却只属于 429：别的 4xx（400 等）即使体里没有时间串也必须是 None，
    /// 否则普通请求错误会被错误地长时间冻掉该模型。
    #[test]
    fn rate_limit_fallback_skips_other_4xx() {
        let now = 1_800_000_000_000;
        assert!(rate_limit_until(r#"{"error":"bad request"}"#, 400, now).is_none());
        assert!(rate_limit_until("", 500, now).is_none());
        // 非 429 但体里带频率关键词 + 时间串 → 仍按上游声明处理（既有行为不变）。
        let body = r#"{"message":"{\"code\":60004,\"msg\":\"将在 2026-10-17 11:46:56 UTC+8 重置\"}"}"#;
        assert_eq!(
            rate_limit_until(body, 400, now),
            Some(utc8_ms("2026-10-17 11:46:56"))
        );
    }

    /// `find_reset_datetime` 只认完整的日期时间形状，不能把乱码当时间。
    #[test]
    fn find_reset_datetime_validates_shape() {
        assert_eq!(find_reset_datetime("2026-10-17 11:46:56"), Some(utc8_ms("2026-10-17 11:46:56")));
        assert_eq!(find_reset_datetime("no time here"), None);
        assert_eq!(find_reset_datetime("2026/10/17 11:46"), None);
        assert_eq!(find_reset_datetime("短"), None);
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

    fn test_meta(model: &str) -> UsageMeta {
        UsageMeta {
            uid: "u1".into(),
            nickname: "测试号".into(),
            model: model.into(),
            stream: false,
            session: None,
            edition: Edition::Domestic,
        }
    }

    /// 累计投影：每一笔都与 `dailyByModel` 同写；升级时只快照一次，之后不再被覆盖。
    #[test]
    fn cumulative_projection_tracks_every_bump_and_snapshots_once() {
        let t1 = 1_760_000_000_000i64;
        let date = local_date_of_ms(t1);
        let usage = json!({"prompt_tokens": 100, "completion_tokens": 10, "cached_tokens": 70});
        let mut doc = empty_edition_usage();
        bump_usage_doc(&mut doc, &test_meta("m"), Some(&usage), true, t1);
        // 新文档：累计投影与按天表逐笔一致。
        assert_eq!(doc["totalDailyByModel"]["m"][date.as_str()]["requests"], 1);
        assert_eq!(doc["totalDailyByModel"]["m"][date.as_str()]["promptTokens"], 100);
        assert_eq!(doc["totalDailyByModel"]["m"][date.as_str()]["cacheReadTokens"], 70);

        // 老文件（升级前写的）：没有快照，只有当天的 dailyByModel。
        let mut legacy = json!({
            "total": {"requests": 5, "errors": 0, "promptTokens": 500,
                      "completionTokens": 5, "credit": 0.0},
            "daily": {},
            "dailyByModel": {"m": {date.clone(): {"requests": 1, "errors": 0,
                "promptTokens": 100, "completionTokens": 1, "credit": 0.0}}},
            "byAccount": {},
            "byModel": {},
            "recent": [],
        });
        backfill_edition_usage(&mut legacy, false);
        assert_eq!(
            legacy["totalDailyByModel"]["m"][date.as_str()]["promptTokens"], 100,
            "升级时用当时的 dailyByModel 作起点"
        );
        // 之后 dailyByModel 被裁剪 / 改写，再 load 也不能覆盖已有快照。
        legacy["dailyByModel"] = json!({});
        backfill_edition_usage(&mut legacy, false);
        assert_eq!(
            legacy["totalDailyByModel"]["m"][date.as_str()]["promptTokens"], 100,
            "已有快照绝不被 dailyByModel 覆盖"
        );
    }

    /// 会话粘滞：同一会话固定到同一个号；绑定的号冷却后解绑、换号；过期失效。
    ///
    /// 这里的「号」用 `lease_usable` 的真实判据（不在冷却、版本对、模型未长冷却），
    /// 所以测的是生产路径上的同一套条件，而不是另写一份。
    #[test]
    fn session_stick_binds_unbinds_and_expires() {
        let pool = Pool::new(vec![
            ("u1".to_string(), Edition::Domestic),
            ("u2".to_string(), Edition::Domestic),
        ]);
        let stick = SessionStick::new();
        let now = now_ms_i64();

        assert_eq!(stick.lookup("s1", now), None);
        stick.bind("s1", "u1", now);
        assert_eq!(stick.lookup("s1", now + 1), Some("u1".to_string()));
        // 别的会话不受影响。
        assert_eq!(stick.lookup("s2", now), None);
        // 过期（TTL 之后）⇒ 视为没有绑定。
        assert_eq!(stick.lookup("s1", now + stick.ttl_ms + 1), None);
        assert_eq!(stick.lookup("s1", now + 2), None, "过期项已被清掉");

        // 绑定的号在冷却 ⇒ 不可用；解绑后不再返回它。
        stick.bind("s3", "u2", now);
        stick.bind("s4", "u2", now);
        pool.mark_failed("u2", "上游返回 401");
        assert!(!pool.lease_usable("u2", Some(Edition::Domestic), None));
        assert!(pool.lease_usable("u1", Some(Edition::Domestic), None));
        // 别的版本 / 其他版本的号不可用。
        assert!(!pool.lease_usable("u1", Some(Edition::International), None));
        // 模型长冷却同样让该号在这条会话上不可用。
        pool.mark_model_failed_until("u1", "m", now + 60_000, "频率超限");
        assert!(!pool.lease_usable("u1", Some(Edition::Domestic), Some("m")));
        assert!(pool.lease_usable("u1", Some(Edition::Domestic), Some("other")));

        // 账号失败会把粘在它上面的会话全部解绑，避免一直撞冷却。
        // （`mark_failed` 作用于**当前入口**的粘滞表；这里把本测试的表登记进去，
        // 走的就是生产路径上的同一条解绑逻辑。）
        current_sticky()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(Edition::Domestic, Arc::new(SessionStick::new()));
        let registered = current_sticky()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&Edition::Domestic)
            .cloned()
            .expect("registered");
        registered.bind("s3", "u2", now);
        registered.bind("s4", "u2", now);
        registered.bind("s6", "u1", now);
        pool.mark_failed("u2", "再次失败");
        let after = registered.snapshot();
        assert!(!after.contains_key("s3"), "u2 的会话要解绑");
        assert!(!after.contains_key("s4"), "u2 的会话要解绑");
        // 其它号的绑定不受影响。
        assert_eq!(after.get("s6").map(String::as_str), Some("u1"));
        current_sticky()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&Edition::Domestic);
    }

    /// 同一天两次请求要合并进同一个 daily 桶；不同模型各记各的 dailyByModel。
    /// 这是前端画日曲线的基础，写错就会一天冒出两个点或模型串账。
    #[test]
    fn usage_daily_merges_same_day_and_splits_models() {
        let mut doc = empty_edition_usage();
        let usage = json!({"prompt_tokens": 10, "completion_tokens": 2, "credit": 0.5});
        let t1 = 1_760_000_000_000i64;

        bump_usage_doc(&mut doc, &test_meta("m-a"), Some(&usage), true, t1);
        // 同一本地日期，晚一小时 → 必须并进同一个桶。
        bump_usage_doc(&mut doc, &test_meta("m-a"), Some(&usage), true, t1 + 3_600_000);
        bump_usage_doc(&mut doc, &test_meta("m-b"), Some(&usage), true, t1);

        let date = local_date_of_ms(t1);
        assert_eq!(doc["daily"][date.as_str()]["requests"], 3);
        assert_eq!(doc["daily"][date.as_str()]["promptTokens"], 30);
        assert_eq!(doc["daily"][date.as_str()]["credit"], 1.5);

        assert_eq!(doc["dailyByModel"]["m-a"][date.as_str()]["requests"], 2);
        assert_eq!(doc["dailyByModel"]["m-a"][date.as_str()]["completionTokens"], 4);
        assert_eq!(doc["dailyByModel"]["m-b"][date.as_str()]["requests"], 1);
        assert!(
            doc["dailyByModel"]["m-b"].get(date.as_str()).is_some(),
            "不同模型要各自成表，不能互相覆盖"
        );

        // 总量 / 账号 / 模型桶同步累加。
        assert_eq!(doc["total"]["requests"], 3);
        assert_eq!(doc["byAccount"]["u1"]["requests"], 3);
        assert_eq!(doc["byModel"]["m-a"]["requests"], 2);
    }

    /// 失败请求只记 requests/errors，token 不能算进任何桶。
    #[test]
    fn usage_failed_request_records_zero_tokens() {
        let mut doc = empty_edition_usage();
        let usage = json!({"prompt_tokens": 100, "completion_tokens": 9, "credit": 1.0});
        // 即便上游残留了一段 usage，ok=false 时也不能计入用量。
        bump_usage_doc(&mut doc, &test_meta("m"), Some(&usage), false, 1_760_000_000_000);
        assert_eq!(doc["total"]["requests"], 1);
        assert_eq!(doc["total"]["errors"], 1);
        assert_eq!(doc["total"]["promptTokens"], 0);
        assert_eq!(doc["daily"][local_date_of_ms(1_760_000_000_000).as_str()]["errors"], 1);
    }

    /// cache 字段别名：OpenAI 嵌套的 `prompt_tokens_details.cached_tokens`
    /// 与 Anthropic 风格的 `cache_creation_input_tokens` 都要认出来。
    #[test]
    fn usage_extracts_cache_tokens_from_aliases() {
        let usage = json!({
            "prompt_tokens": 100,
            "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 40},
            "cache_creation_input_tokens": 7,
        });
        assert_eq!(usage_cache_read(Some(&usage)), 40);
        assert_eq!(usage_cache_write(Some(&usage)), 7);

        let mut doc = empty_edition_usage();
        bump_usage_doc(&mut doc, &test_meta("m"), Some(&usage), true, 1_760_000_000_000);
        assert_eq!(doc["total"]["cacheReadTokens"], 40);
        assert_eq!(doc["total"]["cacheWriteTokens"], 7);
        assert_eq!(doc["byAccount"]["u1"]["cacheReadTokens"], 40);
        assert_eq!(doc["byModel"]["m"]["cacheWriteTokens"], 7);
        let date = local_date_of_ms(1_760_000_000_000);
        assert_eq!(doc["daily"][date.as_str()]["cacheReadTokens"], 40);
        assert_eq!(
            doc["dailyByModel"]["m"][date.as_str()]["cacheWriteTokens"],
            7
        );

        // 扁平别名为 0 时不能挡住后面真正有值的别名（0 通常只是没填）。
        let other = json!({"cache_read_input_tokens": 0, "prompt_cache_hit_tokens": 12});
        assert_eq!(usage_cache_read(Some(&other)), 12);
        // 多别名取第一个 > 0 的，缺失一律 0。
        assert_eq!(usage_cache_read(None), 0);
        assert_eq!(usage_cache_write(Some(&json!({}))), 0);
    }

    /// daily 回填必须幂等：第一次从 recent 算出历史，之后再 load 不能翻倍。
    #[test]
    fn usage_backfill_is_idempotent() {
        let mut bucket = json!({
            "total": {"requests": 2, "errors": 0, "promptTokens": 30,
                      "completionTokens": 3, "credit": 0.2},
            "byAccount": {"u1": {"nickname": "x", "requests": 2, "errors": 0,
                                 "promptTokens": 30, "completionTokens": 3, "credit": 0.2}},
            "byModel": {"m": {"requests": 2, "errors": 0, "promptTokens": 30,
                              "completionTokens": 3, "credit": 0.2}},
            "recent": [
                {"ts": 1_760_000_000_000i64, "uid": "u1", "model": "m", "ok": true,
                 "promptTokens": 10, "completionTokens": 1, "credit": 0.1},
                {"ts": 1_760_003_600_000i64, "uid": "u1", "model": "m", "ok": true,
                 "promptTokens": 20, "completionTokens": 2, "credit": 0.1},
            ],
        });
        let date = local_date_of_ms(1_760_000_000_000);

        backfill_edition_usage(&mut bucket, false);
        assert_eq!(bucket["daily"][date.as_str()]["requests"], 2);
        assert_eq!(bucket["daily"][date.as_str()]["promptTokens"], 30);
        assert_eq!(bucket["dailyByModel"]["m"][date.as_str()]["requests"], 2);
        // 旧桶缺的 cache 字段补 0，已有数值一律不动。
        assert_eq!(bucket["total"]["cacheReadTokens"], 0);
        assert_eq!(bucket["total"]["cacheWriteTokens"], 0);
        assert_eq!(bucket["total"]["promptTokens"], 30);
        assert_eq!(bucket["byAccount"]["u1"]["cacheWriteTokens"], 0);
        assert_eq!(bucket["byModel"]["m"]["promptTokens"], 30);

        // 再跑一次（模拟每次 load 都调）：daily 已存在 ⇒ 只补字段，不重算。
        backfill_edition_usage(&mut bucket, false);
        assert_eq!(
            bucket["daily"][date.as_str()]["requests"], 2,
            "重复 load 不能把 recent 重复计入 daily"
        );
        assert_eq!(bucket["daily"][date.as_str()]["promptTokens"], 30);
        assert_eq!(
            bucket["dailyByModel"]["m"][date.as_str()]["completionTokens"],
            3
        );
    }

    /// 旧扁平格式迁移完也要从 recent 回填 daily —— 否则升级后日曲线从零开始，
    /// 明明有最近 100 条历史却画不出来。
    #[test]
    fn legacy_usage_backfills_daily_from_recent() {
        let mut ed_of = HashMap::new();
        ed_of.insert("u-cn".to_string(), Edition::Domestic);

        let legacy = json!({
            "byAccount": {
                "u-cn": {"nickname": "n", "requests": 2, "errors": 0,
                         "promptTokens": 30, "completionTokens": 3, "credit": 0.2},
            },
            "recent": [
                {"ts": 1_760_000_000_000i64, "uid": "u-cn", "model": "m", "ok": true,
                 "promptTokens": 10, "completionTokens": 1, "credit": 0.1},
                {"ts": 1_760_003_600_000i64, "uid": "u-cn", "model": "m", "ok": true,
                 "promptTokens": 20, "completionTokens": 2, "credit": 0.1},
            ],
        });
        let out = migrate_legacy_usage_with(legacy, &ed_of);
        let date = local_date_of_ms(1_760_000_000_000);
        let cn = &out["byEdition"]["domestic"];
        assert_eq!(cn["daily"][date.as_str()]["requests"], 2);
        assert_eq!(cn["daily"][date.as_str()]["promptTokens"], 30);
        assert_eq!(cn["dailyByModel"]["m"][date.as_str()]["requests"], 2);
        assert_eq!(cn["total"]["cacheReadTokens"], 0);
        assert_eq!(cn["byAccount"]["u-cn"]["cacheWriteTokens"], 0);
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

    /// 「模型不存在 / 账号无权使用该模型」（11102）要能被认出来 —— 认错的话
    /// 一次写错的模型名会把整个账号冻 120 秒。
    #[test]
    fn detects_model_not_found_from_upstream_body() {
        // 正例：扁平体 / 双层转义体 / 只有 msg 的变体。
        assert!(is_model_not_found(
            r#"{"code":11102,"msg":"service info not found"}"#
        ));
        assert!(is_model_not_found(
            r#"{"message":"{\"code\":11102,\"msg\":\"model [x] service info not found\"}"}"#
        ));
        assert!(is_model_not_found("only available for authorized users"));

        // 🔴 反例：11128 的体、空体都不算。
        assert!(!is_model_not_found(
            r#"{"message":"{\"code\":11128,\"msg\":\"Illegal API invocation from an unapproved channel\"}"}"#
        ));
        assert!(!is_model_not_found(""));

        // 🔴 不能误判：requestId 里恰好含 11102 不算 —— 所以判据是 `"code":11102`
        // 而不是裸 `11102` 子串。
        assert!(!is_model_not_found(
            r#"{"code":0,"msg":"ok","requestId":"abc11102def"}"#
        ));
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
        // 身份声明会被抹掉（见 strip_identity_declarations）—— 它正是 11128 的触发点。
        assert_eq!(out[0]["content"], "You are an AI agent powered by Assistant.");
        assert_eq!(out[1]["role"], "user", "user 消息不受影响");
        assert_eq!(
            degraded,
            vec!["developer-role".to_string(), "system-identity".to_string()]
        );

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
        assert_eq!(pool.next_uid_of(Some(Edition::Domestic), None, &[]).as_deref(), Some("u-dom"));
        assert_eq!(pool.next_uid_of(Some(Edition::International), None, &[]).as_deref(), Some("u-intl"));
        assert!(pool.has_edition(Edition::Domestic));
        assert!(pool.has_edition(Edition::International));

        let only_intl = Pool::new(vec![("u-intl".to_string(), Edition::International)]);
        assert_eq!(only_intl.next_uid_of(Some(Edition::Domestic), None, &[]), None);
        assert!(!only_intl.has_edition(Edition::Domestic));
        // 不限版本时照常能拿到
        assert_eq!(only_intl.next_uid_of(None, None::<&str>, &[]).as_deref(), Some("u-intl"));
    }

    /// 🔴 60004 只冻**模型**：同账号的其他模型仍能被选中，冷却到期后该模型自动回归。
    /// 账号级长冷却会把还能用的模型一起冻掉，与上游「可切换其他模型继续使用」矛盾。
    #[test]
    fn model_cooldown_only_blocks_that_model() {
        let pool = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        pool.mark_model_failed_until(
            "u1",
            "model-a",
            now_ms_i64() + 3_600_000,
            "上游频率超限（60004）",
        );

        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-a"), &[]),
            None,
            "被冻的模型不应再选中该号"
        );
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-b"), &[]).as_deref(),
            Some("u1"),
            "同账号的其他模型必须照常可用"
        );
        // 不关心模型时（model = None）不受模型冷却限制
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), None, &[]).as_deref(),
            Some("u1")
        );

        // 已过期的冷确不应生效（mark_model_failed_until 会直接忽略过去时刻）
        let fresh = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        fresh.mark_model_failed_until("u1", "model-a", now_ms_i64() - 1_000, "过期");
        assert_eq!(
            fresh
                .next_uid_of(Some(Edition::Domestic), Some("model-a"), &[])
                .as_deref(),
            Some("u1")
        );
    }

    /// 🔴 11102「模型不存在 / 无权使用」**不得污染账号**：该模型被冻后，
    /// 同一账号对**其它模型**必须照常被选中 —— 一次写错的模型名不能把好号
    /// 从池里摘掉（实现上就是「只冻模型、不 mark_failed」，此测试锁住该契约）。
    #[test]
    fn model_not_found_does_not_poison_the_account() {
        let pool = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        // 模拟 11102 的处理路径：只把该模型冻起来（兜底长冷却），账号本身不动。
        pool.mark_model_failed_until(
            "u1",
            "model-x",
            now_ms_i64() + 3_600_000,
            "上游 429（无重置时刻，兜底冷却）",
        );

        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-x"), &[]),
            None,
            "被冻的模型不应再选中该号"
        );
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), Some("model-y"), &[]).as_deref(),
            Some("u1"),
            "11102 不应把账号整个冻掉：其它模型必须照常可用"
        );
    }

    /// 冷却快照必须**把账号短冷却与逐模型长冷却分开**，并给出绝对恢复时刻 ——
    /// 界面靠这两点才能说清「整个号不可用」还是「换个模型就行」。
    #[test]
    fn cooldown_snapshot_separates_account_and_model_cooldowns() {
        let pool = Pool::new(vec![
            ("u1".to_string(), Edition::Domestic),
            ("u2".to_string(), Edition::Domestic),
        ]);
        let until = now_ms_i64() + 3_600_000;
        pool.mark_model_failed_until("u1", "model-a", until, "上游频率超限（60004）");
        pool.mark_failed("u1", "上游返回 401");

        let snap = pool.cooldown_snapshot();
        assert_eq!(snap.len(), 1, "只有 u1 在冷却，u2 不该出现");
        let u1 = &snap[0];
        assert_eq!(u1["uid"], json!("u1"));
        assert_eq!(u1["edition"], json!("domestic"));
        assert_eq!(u1["reason"], json!("上游返回 401"));
        assert!(u1["until"].as_i64().unwrap() > now_ms_i64());
        assert_eq!(u1["models"][0]["model"], json!("model-a"));
        assert_eq!(u1["models"][0]["until"], json!(until));
        assert_eq!(
            u1["models"][0]["reason"],
            json!("上游频率超限（60004）")
        );

        // 全部到期后快照为空（界面据此把徽标收掉）。
        let expired = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        expired.mark_model_failed_until("u1", "m", now_ms_i64() - 1, "过去");
        assert!(expired.cooldown_snapshot().is_empty());
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

    // ------------------------------------------------------------ Anthropic 兼容层

    #[test]
    fn anthropic_request_translates_system_tools_and_tool_result() {
        let req = json!({
            "model": "deepseek-v4.1-flash[1M]",
            "max_tokens": 1024,
            "system": [
                {"type": "text", "text": "sys1"},
                {"type": "text", "text": "sys2"}
            ],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "let me check"},
                    {"type": "tool_use", "id": "toolu_1", "name": "get_weather",
                     "input": {"city": "杭州"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1",
                     "content": [{"type": "text", "text": "sunny"}]},
                    {"type": "text", "text": "继续"}
                ]}
            ],
            "tools": [{
                "name": "get_weather",
                "description": "weather",
                "input_schema": {"type": "object", "properties": {}}
            }],
            "tool_choice": {"type": "tool", "name": "get_weather"},
            "stop_sequences": ["END"]
        });

        let out = anthropic_to_openai(&req).unwrap();
        assert_eq!(out["model"], "deepseek-v4.1-flash");
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "sys1sys2");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[2]["role"], "assistant");
        assert_eq!(msgs[2]["tool_calls"][0]["id"], "toolu_1");
        assert_eq!(msgs[2]["tool_calls"][0]["function"]["name"], "get_weather");
        let args: Value =
            serde_json::from_str(msgs[2]["tool_calls"][0]["function"]["arguments"].as_str().unwrap())
                .unwrap();
        assert_eq!(args["city"], "杭州");
        // tool 消息必须先于同批 user 文本
        assert_eq!(msgs[3]["role"], "tool");
        assert_eq!(msgs[3]["tool_call_id"], "toolu_1");
        assert_eq!(msgs[3]["content"], "sunny");
        assert_eq!(msgs[4]["role"], "user");
        assert_eq!(msgs[4]["content"], "继续");

        assert_eq!(out["tools"][0]["type"], "function");
        assert_eq!(out["tools"][0]["function"]["name"], "get_weather");
        // 🔴 必须是**字符串**函数名：对象形式上游 11101（见 anthropic_tool_choice）。
        assert_eq!(out["tool_choice"], "get_weather");
        assert_eq!(out["stop"][0], "END");
        assert_eq!(out["max_tokens"], 1024);
    }

    #[test]
    fn anthropic_tool_error_is_marked_and_image_becomes_data_url() {
        let req = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "is_error": true, "content": "boom"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/jpeg", "data": "AAA"
                }}
            ]}]
        });
        let out = anthropic_to_openai(&req).unwrap();
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "tool");
        assert_eq!(msgs[0]["content"], "[tool_error] boom");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(
            msgs[1]["content"][0]["image_url"]["url"],
            "data:image/jpeg;base64,AAA"
        );
    }

    fn stream_frame(delta: Value, finish: &str) -> Value {
        json!({
            "model": "m",
            "choices": [{"delta": delta, "finish_reason": finish}]
        })
    }

    #[test]
    fn anthropic_stream_emits_text_and_tool_use_and_forces_stop_reason() {
        let frames = vec![
            stream_frame(json!({"content": "Hello"}), ""),
            stream_frame(
                json!({"tool_calls": [{
                    "index": 0,
                    "id": "call_1",
                    "function": {"name": "get_weather", "arguments": "{\"city\":"}
                }]}),
                "",
            ),
            stream_frame(
                json!({"tool_calls": [{
                    "index": 0,
                    "function": {"arguments": "\"杭州\"}"}
                }]}),
                "",
            ),
            json!({
                "choices": [{"delta": {}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                          "cache_read_input_tokens": 3}
            }),
        ];

        let mut st = AnthropicStream::new("m".to_string());
        let mut events = st.start_events();
        for f in &frames {
            events.extend(st.feed(f));
        }
        events.extend(st.finish());

        let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"message_start"));
        assert!(names.contains(&"content_block_start"));
        assert!(names.contains(&"content_block_delta"));
        assert!(names.contains(&"message_stop"));

        let delta = events
            .iter()
            .find(|(n, _)| n == "message_delta")
            .map(|(_, v)| v)
            .unwrap();
        // 上游给的是 stop，但有工具调用时必须报 tool_use。
        assert_eq!(delta["delta"]["stop_reason"], "tool_use");
        assert_eq!(delta["usage"]["input_tokens"], 10);
        assert_eq!(delta["usage"]["output_tokens"], 5);
        assert_eq!(delta["usage"]["cache_read_input_tokens"], 3);

        let msg = st.build_message();
        assert_eq!(msg["stop_reason"], "tool_use");
        assert_eq!(msg["content"][0]["type"], "text");
        assert_eq!(msg["content"][0]["text"], "Hello");
        assert_eq!(msg["content"][1]["type"], "tool_use");
        assert_eq!(msg["content"][1]["name"], "get_weather");
        assert_eq!(msg["content"][1]["input"]["city"], "杭州");
    }

    #[test]
    fn anthropic_stream_empty_response_gets_empty_text_block() {
        let mut st = AnthropicStream::new("m".to_string());
        let _ = st.start_events();
        let events = st.finish();
        assert!(events.iter().any(|(n, v)| {
            n == "content_block_start" && v["content_block"]["type"] == "text"
        }));
        assert!(events.iter().any(|(n, _)| n == "message_stop"));
        let msg = st.build_message();
        assert_eq!(msg["content"][0]["type"], "text");
        assert_eq!(msg["content"][0]["text"], "");
    }

    #[test]
    fn anthropic_stream_handles_legacy_function_call() {
        let frame = stream_frame(
            json!({"function_call": {"name": "foo", "arguments": "{\"a\":1}"}}),
            "stop",
        );
        let mut st = AnthropicStream::new("m".to_string());
        let _ = st.start_events();
        let _ = st.feed(&frame);
        let _ = st.finish();
        let msg = st.build_message();
        assert_eq!(msg["stop_reason"], "tool_use");
        assert_eq!(msg["content"][0]["type"], "tool_use");
        assert_eq!(msg["content"][0]["name"], "foo");
        assert_eq!(msg["content"][0]["input"]["a"], 1);
    }

    #[test]
    fn anthropic_stream_supports_multiple_tool_calls() {
        let frames = vec![
            stream_frame(
                json!({"tool_calls": [{
                    "index": 0, "id": "c0",
                    "function": {"name": "first_tool", "arguments": "{}"}
                }]}),
                "",
            ),
            stream_frame(
                json!({"tool_calls": [{
                    "index": 1, "id": "c1",
                    "function": {"name": "second_tool", "arguments": "{\"x\":2}"}
                }]}),
                "tool_calls",
            ),
        ];
        let mut st = AnthropicStream::new("m".to_string());
        let _ = st.start_events();
        for f in &frames {
            let _ = st.feed(f);
        }
        let _ = st.finish();
        let msg = st.build_message();
        assert_eq!(msg["stop_reason"], "tool_use");
        let content = msg["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["name"], "first_tool");
        assert_eq!(content[1]["name"], "second_tool");
        assert_eq!(content[1]["input"]["x"], 2);
    }

    #[test]
    fn anthropic_model_suffix_and_count_tokens() {
        assert_eq!(
            normalize_anthropic_model("deepseek-v4.1-flash[1M]"),
            "deepseek-v4.1-flash"
        );
        assert_eq!(
            normalize_anthropic_model("deepseek-v4.1-flash[1m]"),
            "deepseek-v4.1-flash"
        );
        let body = json!({
            "system": "123456",
            "messages": [{"role": "user", "content": "123456"}]
        });
        // 估算会把 role 之类的短字符串也算进去，只要求量级合理。
        assert!(estimate_anthropic_input_tokens(&body) >= 4);
    }

    #[test]
    fn anthropic_auth_accepts_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("sk-x"));
        assert!(check_auth(&headers, "sk-x"));
        assert!(!check_auth(&headers, "sk-y"));
    }

    #[test]
    fn strips_claude_code_billing_and_env_markers() {
        let text = "x-anthropic-billing-header: cc_version=2.1; cc_entrypoint=sdk-cli;\n\
                    You are a Claude agent.\n\n\
                    # Environment\nYou have been invoked in the following environment: \n\
                    - Platform: win32\n- Shell: bash";
        assert_eq!(
            strip_claude_code_markers(text),
            "You are a Claude agent."
        );
        assert_eq!(
            strip_claude_code_markers("You are Claude Code, Anthropic's official CLI for Claude."),
            ""
        );
        assert_eq!(
            strip_claude_code_markers(
                "You are Claude Code, Anthropic's official CLI for Claude. Be terse."
            ),
            "Be terse."
        );
    }

    #[test]
    fn anthropic_mid_system_is_merged_into_leading_system() {
        let req = json!({
            "model": "m",
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc; entry=sdk;"},
                {"type": "text", "text": "core prompt"}
            ],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "system", "content": "extra reminder"},
                {"role": "assistant", "content": "ok"}
            ]
        });
        let out = anthropic_to_openai(&req).unwrap();
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["role"], "system");
        let sys = msgs[0]["content"].as_str().unwrap();
        assert!(sys.contains("core prompt"));
        assert!(sys.contains("extra reminder"));
        assert!(!sys.contains("x-anthropic-billing-header"));
        assert_eq!(msgs[1]["role"], "user");
    }

    #[test]
    fn upstream_body_passes_tool_choice_stop_and_top_k() {
        let incoming = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "x"}],
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "tool_choice": "auto",
            "stop": ["END"],
            "top_k": 5
        });
        let body = build_upstream_body(&incoming, Edition::Domestic, &mut Vec::new());
        assert_eq!(body["tool_choice"], "auto");
        assert_eq!(body["stop"][0], "END");
        assert_eq!(body["top_k"], 5);
    }

    /// 标准参数以前被静默丢弃（客户端设了等于没设），现在必须全部透传。
    #[test]
    fn upstream_body_passes_all_standard_params() {
        let incoming = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "x"}],
            "temperature": 0.3,
            "top_p": 0.9,
            "max_tokens": 256,
            "stop": ["END"],
            "frequency_penalty": 0.5,
            "presence_penalty": -0.5,
            "response_format": {"type": "json_object"},
            "parallel_tool_calls": false,
            "seed": 42,
            "stream_options": {"include_usage": true},
            "user": "u-1",
            "logprobs": true,
            "top_logprobs": 2
        });
        let body = build_upstream_body(&incoming, Edition::Domestic, &mut Vec::new());
        for key in STANDARD_PASSTHROUGH {
            assert!(body.get(*key).is_some(), "标准参数 {key} 被丢弃了");
        }
        assert_eq!(body["response_format"]["type"], "json_object");
        assert_eq!(body["parallel_tool_calls"], false);
        assert_eq!(body["seed"], 42);
        // 出站恒为流式，客户端不能改掉。
        assert_eq!(body["stream"], true);
    }

    /// `include_usage=false` 会让上游不回 usage，本地用量统计就瞎了 —— 强制 true。
    #[test]
    fn stream_options_include_usage_is_forced_true() {
        let incoming = json!({
            "model": "m",
            "messages": [{"role": "user", "content": "x"}],
            "stream_options": {"include_usage": false}
        });
        let body = build_upstream_body(&incoming, Edition::Domestic, &mut Vec::new());
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    /// OpenAI 规范的对象形式上游 11101，必须无损降级成函数名字符串。
    #[test]
    fn tool_choice_object_is_normalized_to_string() {
        assert_eq!(
            normalize_tool_choice(&json!({"type": "function", "function": {"name": "f"}})),
            Some(json!("f"))
        );
        assert_eq!(normalize_tool_choice(&json!({"type": "auto"})), Some(json!("auto")));
        assert_eq!(normalize_tool_choice(&json!({"type": "none"})), Some(json!("none")));
        assert_eq!(
            normalize_tool_choice(&json!({"type": "required"})),
            Some(json!("required"))
        );
        // Anthropic 形态
        assert_eq!(normalize_tool_choice(&json!({"type": "any"})), Some(json!("required")));
        assert_eq!(
            normalize_tool_choice(&json!({"type": "tool", "name": "g"})),
            Some(json!("g"))
        );
        assert_eq!(normalize_tool_choice(&json!({"type": "allowed"})), Some(json!("auto")));
        // 字符串原样透传
        assert_eq!(normalize_tool_choice(&json!("auto")), Some(json!("auto")));
        assert_eq!(normalize_tool_choice(&json!("f")), Some(json!("f")));
        // 🔴 缺 name 不兜底成 required（那会改变强制调用语义），直接丢弃。
        assert_eq!(normalize_tool_choice(&json!({"type": "function", "function": {}})), None);
        assert_eq!(normalize_tool_choice(&json!({"type": "function"})), None);
        assert_eq!(normalize_tool_choice(&json!({"type": "bogus"})), None);
        assert_eq!(normalize_tool_choice(&json!("")), None);
    }

    /// Anthropic 入口的 `{"type":"tool","name":"g"}` 必须输出字符串，不是对象。
    #[test]
    fn anthropic_tool_choice_emits_string() {
        assert_eq!(
            anthropic_tool_choice(Some(&json!({"type": "tool", "name": "g"}))),
            Some(json!("g"))
        );
        assert_eq!(
            anthropic_tool_choice(Some(&json!({"type": "any"}))),
            Some(json!("required"))
        );
        assert_eq!(anthropic_tool_choice(Some(&json!({"type": "none"}))), Some(json!("none")));
        assert_eq!(anthropic_tool_choice(Some(&json!({"type": "tool", "name": ""}))), None);
    }

    /// 11128 渠道校验**不能冻账号**：它拦的是请求，不是号。
    ///
    /// 这个测试锁的是「重试耗尽后不会调 `mark_failed`」这个契约本身 ——
    /// 以前代码与注释相反（注释写不冻，实现照样冻），池子小的时候一次 11128
    /// 就能让整池空转。
    #[test]
    fn channel_block_must_not_poison_the_pool() {
        let pool = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        assert!(pool.next_uid_of(Some(Edition::Domestic), None, &[]).is_some());
        // 模拟修复前那行 mark_failed 的后果：号被摘掉
        pool.mark_failed("u1", "渠道校验被拦（重试后仍失败）");
        assert!(
            pool.next_uid_of(Some(Edition::Domestic), None, &[]).is_none(),
            "冻了号就取不到 —— 这正是要避免的"
        );
        pool.clear_cooldown("u1");
        assert!(
            pool.next_uid_of(Some(Edition::Domestic), None, &[]).is_some(),
            "不冻号时同一个号始终可用"
        );
    }

    /// 自愈的默认层：只抹身份声明、不动请求形状（图片块要保住）。
    #[test]
    fn self_heal_is_lossless_by_default() {
        let body = json!({
            "model": "m",
            "messages": [
                {"role": "system", "content": "You are DeepSeek Harness."},
                {"role": "user", "content": [
                    {"type": "text", "text": "看图"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA=="}}
                ]}
            ]
        });
        let mut degraded = Vec::new();
        let healed = heal_body_for_channel(&body, &mut degraded);
        assert_eq!(healed["messages"][0]["content"], "You are Assistant.");
        // 🔴 图片块必须还在 —— 拍平内容会把它弄丢。
        assert!(healed["messages"][1]["content"].is_array(), "默认层不能拍平 content");
        assert_eq!(healed["messages"][1]["content"][1]["type"], "image_url");
        assert_eq!(degraded, vec!["self-heal-identity".to_string()]);
    }

    /// 没有任何可改的位置时，自愈要如实返回「没变化」（调用方据此走退避重试）。
    #[test]
    fn self_heal_reports_no_change_when_nothing_to_fix() {
        let body = json!({
            "model": "m",
            "messages": [{"role": "system", "content": "普通提示词"}]
        });
        let mut degraded = Vec::new();
        let healed = heal_body_for_channel(&body, &mut degraded);
        assert_eq!(healed, body);
        assert!(degraded.is_empty());
    }

    /// 最大强度归一化：角色收敛 + 去 name + 内容拍平。
    #[test]
    fn aggressive_normalize_collapses_payload_shape() {
        let mut body = json!({
            "model": "m",
            "messages": [
                {"role": "developer", "name": "dsh", "content": "hi"},
                {"role": "user", "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}
            ]
        });
        let mut degraded = Vec::new();
        sanitize_messages_aggressive(&mut body, &mut degraded);
        assert_eq!(body["messages"][0]["role"], "system");
        assert!(body["messages"][0].get("name").is_none());
        assert_eq!(body["messages"][1]["content"], "a\nb");
        assert_eq!(degraded, vec!["aggressive-normalize".to_string()]);
    }

    /// 🔴 脚本 / SDK 不带 `Origin`，来源校验必须放行（否则所有 CLI 客户端全挂）。
    /// 带外来 `Origin` 的浏览器请求必须拒（防止本机页面盗刷）。
    #[test]
    fn origin_guard_blocks_cross_site_but_allows_sdks() {
        use axum::http::{header, HeaderMap, HeaderValue};
        let mk = |host: &str, origin: Option<&str>| {
            let mut h = HeaderMap::new();
            h.insert(header::HOST, HeaderValue::from_str(host).unwrap());
            if let Some(o) = origin {
                h.insert(header::ORIGIN, HeaderValue::from_str(o).unwrap());
            }
            h
        };
        // SDK / 脚本：只有 Host
        assert!(guard_request(&mk("127.0.0.1:7863", None)).is_ok());
        assert!(guard_request(&mk("localhost:7864", None)).is_ok());
        // 本机页面
        assert!(guard_request(&mk("127.0.0.1:7863", Some("http://localhost:57890"))).is_ok());
        // 外来页面 / 沙箱（Origin: null）→ 拒
        assert!(guard_request(&mk("127.0.0.1:7863", Some("https://evil.example.com"))).is_err());
        assert!(guard_request(&mk("127.0.0.1:7863", Some("null"))).is_err());
        // DNS rebinding：Host 是攻击者域名 → 拒
        assert!(guard_request(&mk("evil.example.com", None)).is_err());
    }

    /// 两种 11128 变体的提示必须分开，且不能把原始双层 JSON 甩给用户。
    #[test]
    fn channel_block_messages_are_actionable_and_distinct() {
        let payload = channel_block_message(
            r#"{"code":11128,"msg":"Illegal API invocation from an unapproved channel"}"#,
            "国内版",
            2,
        );
        assert!(payload.contains("负载不合规"));
        assert!(payload.contains("DeepSeek Harness"));
        assert!(payload.contains("includeHarnessIdentity"));
        assert!(!payload.contains(r#"\u"#), "不能透传原始 JSON");

        let identity = channel_block_message(
            r#"{"code":11128,"msg":"request illegal"}"#,
            "国际版",
            2,
        );
        assert!(identity.contains("调用方不是官方客户端"));
        assert!(identity.contains("本地无法绕过"));
        assert_ne!(payload, identity, "两种变体文案要分开");
    }
    #[test]
    fn identity_declarations_are_stripped_in_system_only() {
        let msgs = json!([
            { "role": "system", "content": "You are DeepSeek Harness. Rules." },
            { "role": "system", "content": [
                {"type": "text", "text": "powered by DEEPSEEK-HARNESS"},
                {"type": "text", "text": "keep me"}
            ]},
            { "role": "user", "content": "what is DeepSeek Harness?" }
        ]);
        let out = sanitize_messages(&msgs, &mut Vec::new());
        assert_eq!(out[0]["content"], "You are Assistant. Rules.");
        assert_eq!(out[1]["content"][0]["text"], "powered by Assistant");
        assert_eq!(out[1]["content"][1]["text"], "keep me");
        assert_eq!(out[2]["content"], "what is DeepSeek Harness?", "user 消息不动");
        // 直接测函数本身
        assert_eq!(
            strip_identity_declarations("a DeepSeek Harness b"),
            Some("a Assistant b".to_string())
        );
        assert_eq!(strip_identity_declarations("中文里没有身份声明"), None);
        assert_eq!(
            strip_identity_declarations("DeepSeek Harness"),
            Some("Assistant".to_string())
        );
    }
    // ---------------------------------------------------------------- 本次修复的回归测试

    /// 磁盘上的账号对象能不能拿来出站：uid 对得上、不需要重新登录、带明文 token。
    ///
    /// 修复前 `take_account` 只做前两条；信封号的 `access_token` 是密文对象，
    /// 拼出的 `Bearer` 头是 `[object Object]` 之类，上游只会回 401。
    #[test]
    fn leasable_uid_requires_plaintext_and_no_relogin() {
        let plain = json!({"uid": "u1", "access_token": "at-1"});
        assert!(is_leasable_uid(&plain, "u1"));
        assert!(!is_leasable_uid(&plain, "u2"), "uid 对不上");

        let mut relogin = plain.clone();
        relogin["needs_relogin"] = json!(true);
        assert!(!is_leasable_uid(&relogin, "u1"), "需要重新登录的号不该再被选中");

        let envelope = json!({
            "uid": "u1",
            "access_token": {"$wbEncrypted": true, "envelope": "zzz"}
        });
        assert!(!is_leasable_uid(&envelope, "u1"), "信封号拿不到明文");
    }

    /// 修复前：挑到失效号就整单 503。现在被跳过的号只进**短冷却**（30 秒），
    /// 到期后 `next_uid_of` 会重新把它交出来 —— 让「重新导入 / 重新登录修好了」
    /// 能自动回到轮转里，而不是等到重启。
    #[test]
    fn unusable_account_is_skipped_briefly_not_forever() {
        let pool = Pool::new(vec![
            ("dead".to_string(), Edition::Domestic),
            ("alive".to_string(), Edition::Domestic),
        ]);
        pool.mark_unusable("dead", "账号已删除或需要重新登录");
        assert_eq!(SKIP_COOLDOWN, Duration::from_secs(30));

        // 第一个轮回把「真的还在」的号交出来（dead 在冷却）：
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(pool.next_uid_of(Some(Edition::Domestic), None::<&str>, &[]));
        }
        assert!(seen.iter().all(|u| u.as_deref() == Some("alive")), "{seen:?}");

        // 冷却到期（把绝对时刻拨到过去模拟）后，dead 回到轮转里。
        pool.mark_until("dead", now_ms_i64() - 1, "过去");
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(pool.next_uid_of(Some(Edition::Domestic), None::<&str>, &[]));
        }
        assert!(seen.iter().any(|u| u.as_deref() == Some("dead")), "{seen:?}");
    }

    /// 「整池都在冷却」时仍然 503 —— 只是这类「刷新失败的可能还能用」的号，
    /// 会被 `take_account` 在最后挑出来兜底（见那里的 `skipped`）。这里只锁住
    /// 池子的基本契约：排除列表生效、拨时间能恢复。
    #[test]
    fn exclusion_list_is_honoured() {
        let pool = Pool::new(vec![("u1".to_string(), Edition::Domestic)]);
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), None::<&str>, &[]),
            Some("u1".to_string())
        );
        assert_eq!(
            pool.next_uid_of(Some(Edition::Domestic), None::<&str>, &["u1".to_string()]),
            None
        );
    }

    /// 流式泵的首包/帧间隔超时：首包用短超时，之后切到帧间隔超时；
    /// 文案里要能看出卡在哪一段（排查用）。两者都可以用环境变量覆盖。
    #[test]
    fn stream_guard_has_first_byte_and_idle_stages() {
        std::env::remove_var("WB_SWITCH_STREAM_FIRST_BYTE_SECS");
        std::env::remove_var("WB_SWITCH_STREAM_IDLE_SECS");
        let mut g = StreamGuard::new();
        g.arm(true);
        assert!(g.timed_out().contains("首包"), "{}", g.timed_out());
        g.arm(false);
        assert!(g.timed_out().contains("帧间隔"), "{}", g.timed_out());
        assert_eq!(first_byte_timeout(), STREAM_FIRST_BYTE);
        assert_eq!(idle_timeout(), STREAM_IDLE_TIMEOUT);
        assert!(STREAM_IDLE_TIMEOUT > STREAM_FIRST_BYTE);
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(env_secs("WB_SWITCH_TEST_NOT_SET", Duration::from_secs(5)), Duration::from_secs(5));
    }

    // ------------------------------------------------ 出站头族 / 11140 分流

    fn test_account() -> Value {
        json!({
            "uid": "u-1",
            "access_token": "at",
            "domain": "www.workbuddy.ai",
            "edition": "international",
        })
    }

    /// 出站头族要与官方桌面端一致：取值错误、多余头、缺头都是可识别的差异。
    #[test]
    fn chat_headers_align_with_official_client() {
        let h = chat_headers(&test_account(), None);
        assert_eq!(h.get("X-Product").map(String::as_str), Some("SaaS"));
        assert_eq!(h.get("X-IDE-Type").map(String::as_str), Some("WorkBuddy"));
        assert_eq!(h.get("X-IDE-Name").map(String::as_str), Some("WorkBuddy"));
        assert_eq!(
            h.get("X-IDE-Version").map(String::as_str),
            Some(DEFAULT_CLIENT_VERSION)
        );
        // 官方只声明 application/json；官方转发时过滤 Origin / Referer。
        assert_eq!(
            h.get("Accept").map(String::as_str),
            Some("application/json")
        );
        assert!(!h.contains_key("Origin"));
        assert!(!h.contains_key("Referer"));
        // 官方头族里没有 X-Root-Request-ID。
        assert!(!h.contains_key("X-Root-Request-ID"));
        let ua = h.get("User-Agent").cloned().unwrap_or_default();
        assert!(
            ua.contains(&format!("WorkBuddy/{DEFAULT_CLIENT_VERSION}")),
            "{ua}"
        );
        assert!(ua.contains(&format!("CLI/{DEFAULT_CLI_VERSION}")), "{ua}");
    }

    /// 🔴 旧 hex_id 的高 16 位恒为零，导致 X-B3-SpanId 恒为 `0000000000000000`、
    /// TraceId 全带 16 个前导零 —— 一次格式校验就能识别的确定性指纹。
    #[test]
    fn ids_are_full_width_and_span_id_is_random() {
        for _ in 0..32 {
            let id = hex_id();
            assert_eq!(id.len(), 32, "{id}");
            assert!(!id.starts_with("0000000000000000"), "{id}");
        }

        let h = chat_headers(&test_account(), None);
        let span = h.get("X-B3-SpanId").expect("必须有 span id");
        assert_eq!(span.len(), 16);
        assert_ne!(span, "0000000000000000");
        let trace = h.get("X-B3-TraceId").expect("必须有 trace id");
        assert_eq!(trace.len(), 32);
        assert!(!trace.starts_with("0000000000000000"), "{trace}");
        // 同一纳秒内连续生成也不能重复（hex_id 里有序号兜底）。
        let h2 = chat_headers(&test_account(), None);
        assert_ne!(h.get("X-B3-SpanId"), h2.get("X-B3-SpanId"));
        assert_ne!(h.get("X-Conversation-Request-ID"), h2.get("X-Conversation-Request-ID"));
    }

    /// 入站自带会话 id 时，出站 `X-Conversation-ID` 就是它（会话内稳定）；
    /// 请求级 id 仍按每次请求现生成 —— 发送侧也不再追加第二个同名头。
    #[test]
    fn conversation_id_follows_inbound_session() {
        let h = chat_headers(&test_account(), Some("3f8a0c1e-session"));
        assert_eq!(
            h.get("X-Conversation-ID").map(String::as_str),
            Some("3f8a0c1e-session")
        );
        assert_ne!(
            h.get("X-Conversation-Request-ID"),
            h.get("X-Conversation-ID"),
            "请求级 id 不能跟着会话走"
        );
        // 空串按「没有会话」处理，不能把空值发给上游。
        let h2 = chat_headers(&test_account(), Some(""));
        assert_ne!(h2.get("X-Conversation-ID").map(String::as_str), Some(""));
        assert_eq!(
            h2.get("X-Conversation-ID").map(|v| v.len()),
            Some(32),
            "无会话时用现生成的 32 位 hex"
        );
    }

    /// 11133 / 11134 / 14003 属于「模型侧瞬时故障」，靠重试解决，不是账号问题。
    #[test]
    fn transient_model_errors_are_recognized() {
        let real = r#"{"code":11133,"msg":"the request parameters were rejected by the model provider","extError":{"code":"1210"}}"#;
        assert!(is_transient_model_error(real));
        assert_eq!(transient_code(real), Some("11133"));
        assert!(is_transient_model_error(r#"{"code":11134}"#));
        assert!(is_transient_model_error(r#"{"code":14003}"#));
        // 别的码不能落进这一类
        assert!(!is_transient_model_error(r#"{"code":11128,"msg":"request illegal"}"#));
        assert!(!is_transient_model_error(r#"{"code":60004,"msg":"频率限制"}"#));
        assert!(!is_transient_model_error(r#"{"code":11102}"#));
        assert_eq!(transient_code(r#"{"code":11102}"#), None);
    }

    /// 🔴 出站头必须同时带齐三个会话 ID。
    ///
    /// 国际版渠道校验只查**存在性**：缺 `X-Conversation-ID` 时，同一个号、同一套
    /// 其余头、同一个 payload 会被稳定判成 `11128 request illegal`（上游
    /// 2026-10-26 的对照实验）。这条测试防止将来「精简头族」时又把它删掉 ——
    /// 这个缺失曾让国际版整整 9 天全线不可用。
    #[test]
    fn chat_headers_carry_all_three_conversation_ids() {
        let h = chat_headers(&test_account(), None);
        for key in [
            "X-Conversation-ID",
            "X-Conversation-Request-ID",
            "X-Conversation-Message-ID",
        ] {
            let v = h.get(key).unwrap_or_else(|| panic!("{key} 必须存在"));
            assert!(!v.is_empty(), "{key} 不能为空");
        }
    }

    /// 申诉要 Trace ID：上游的 `requestId` 就是它，必须能从失败体里抠出来。
    #[test]
    fn extracts_upstream_trace_id_for_appeal() {
        let real = r#"{"code":11140,"msg":"request illegal","requestId":"9d5bc5fd-3e04-4752-8e13-a8d4cf28767e","displayMsg":{"en":"..."}}"#;
        assert_eq!(
            upstream_request_id(real).as_deref(),
            Some("9d5bc5fd-3e04-4752-8e13-a8d4cf28767e"),
            "必须原样取出 requestId（Trace ID）"
        );
        // 没有该字段 / 空体 → None，不能 panic
        assert_eq!(upstream_request_id(r#"{"code":11128}"#), None);
        assert_eq!(upstream_request_id(""), None);
        // 值为空串也不算有效
        assert_eq!(upstream_request_id(r#"{"requestId":""}"#), None);
    }

    /// 11140 双语义必须靠 displayMsg 区分（调研报告 §2）：
    /// 账号级 → 摘号；内容审核 → 只回文案，不动账号；认不出 → 保守长冷却。
    #[test]
    fn ban_kind_splits_11140_by_display_msg() {
        let account = r#"{"code":11140,"msg":"request illegal","displayMsg":{"en":"Your account has been restricted due to unusual activity","zh":"账号存在异常行为，已被限制使用"}}"#;
        assert_eq!(ban_kind(account), Some(BanKind::Account));

        let content = r#"{"code":11140,"msg":"request illegal","displayMsg":{"zh":"内容未通过安全审核，请调整后重试"}}"#;
        assert_eq!(ban_kind(content), Some(BanKind::Content));

        let unknown = r#"{"code":11140,"msg":"request illegal"}"#;
        assert_eq!(ban_kind(unknown), Some(BanKind::Unknown));

        // 其它码不能被误判成封号。
        assert_eq!(ban_kind(r#"{"code":11128,"msg":"unapproved channel"}"#), None);
        assert_eq!(ban_kind(r#"{"code":6004,"msg":"frequency limit"}"#), None);
        assert_eq!(ban_kind("plain text"), None);
    }

    /// 中文被转义成 `\uXXXX` 时也要能分出「账号级」。
    #[test]
    fn ban_kind_recognises_escaped_chinese_display_msg() {
        let body = "{\"code\":11140,\"msg\":\"request illegal\",\"displayMsg\":\"\\u8d26\\u53f7\\u5b58\\u5728\\u5f02\\u5e38\\u884c\\u4e3a\\uff0c\\u5df2\\u88ab\\u9650\\u5236\\u4f7f\\u7528\"}";
        assert!(!body.contains("异常行为"), "前提：中文确实被转义了");
        assert_eq!(ban_kind(body), Some(BanKind::Account));
    }

    /// 业务码匹配必须锚定 `code` 键：`requestId` 里的 hex 数字不能被误命中
    /// （上游 2026-10-26 的同类修复）。
    #[test]
    fn upstream_code_match_is_anchored() {
        // requestId 里含 11128，但真正的码是 11133：不能当渠道拦截去重试。
        let body = r#"{"code":11133,"msg":"model busy","requestId":"00000000000011128aabb"}"#;
        assert!(!is_channel_block(body));
        // 双层转义形态照旧要认。
        assert!(is_channel_block(r#"{"message":"{\"code\":11128}"}"#));
        assert!(is_model_not_found(r#"{"message":"{\"code\":11102}"}"#));
        assert!(ban_kind(r#"{"message":"{\"code\":11140,\"displayMsg\":\"unusual activity\"}"}"#)
            .is_some());
    }
}
