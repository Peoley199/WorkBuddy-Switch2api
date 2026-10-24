//! 本地 WorkBuddy / CodeBuddy CLI / CodeBuddy IDE Token 统计。
//!
//! 这个模块是统计数据的唯一归属：日志只在这里解码、去重和按时间聚合，
//! Tauri 与 HTTP 层只负责转发结果。响应只包含聚合数字和脱敏标识，不返回
//! 消息正文、arguments 或认证信息。
//!
//! CodeBuddy IDE 不写 JSONL，用量在 `CodeBuddyExtension/Data/**/history/**/index.json`
//! 的 `requests[].usage` 中；消息正文文件（`messages/`）不会被扫描。

use chrono::{Datelike, Local, Timelike};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Usage {
    input: u64,
    output: u64,
    read: u64,
    write: u64,
}

#[derive(Clone, Debug, Default)]
struct Totals {
    usage: Usage,
    records: u64,
}

/// 一个用量来源的聚合结果，**额外**按会话（[`session_hash`] 的掩码）拆分。
///
/// 用来回答与统计页同口径的问题：「最近这批会话各自命中多少」——
/// 这是评估「按会话粘滞选号」是否有效所需要的对照数据，本身不影响任何已展示的统计。
#[derive(Clone, Debug, Default)]
struct TotalsBundle {
    usage: Usage,
    records: u64,
    by_session: HashMap<u64, Totals>,
}

impl TotalsBundle {
    fn add(&mut self, session: u64, usage: Usage) {
        self.usage.input = self.usage.input.saturating_add(usage.input);
        self.usage.output = self.usage.output.saturating_add(usage.output);
        self.usage.read = self.usage.read.saturating_add(usage.read);
        self.usage.write = self.usage.write.saturating_add(usage.write);
        self.records = self.records.saturating_add(1);
        let bucket = self.by_session.entry(session).or_default();
        bucket.usage.input = bucket.usage.input.saturating_add(usage.input);
        bucket.usage.output = bucket.usage.output.saturating_add(usage.output);
        bucket.usage.read = bucket.usage.read.saturating_add(usage.read);
        bucket.usage.write = bucket.usage.write.saturating_add(usage.write);
        bucket.records = bucket.records.saturating_add(1);
    }

    fn totals(&self) -> Value {
        let mut sessions: Vec<Value> = self
            .by_session
            .iter()
            .map(|(session, totals)| {
                let mut value = totals.value();
                value["key"] = json!(format!("0x{session:04x}"));
                value
            })
            .collect();
        sessions.sort_by_key(|value| std::cmp::Reverse(total_value(value)));
        json!({
            "usage": Totals { usage: self.usage, records: self.records }.value(),
            "sessionCount": self.by_session.len(),
            "sessions": sessions,
        })
    }
}

#[derive(Clone, Debug)]
struct SessionTotals {
    key: String,
    title: Option<String>,
    project: String,
    session_id: String,
    totals: Totals,
}

impl Totals {
    fn add(&mut self, usage: Usage) {
        self.usage.input = self.usage.input.saturating_add(usage.input);
        self.usage.output = self.usage.output.saturating_add(usage.output);
        self.usage.read = self.usage.read.saturating_add(usage.read);
        self.usage.write = self.usage.write.saturating_add(usage.write);
        self.records = self.records.saturating_add(1);
    }

    /// 并入另一份已聚合的统计。反代的按日桶里 `records` 是请求数之和，
    /// 不能像 [`Totals::add`] 那样每个桶只记一条，所以单独走这个入口。
    fn merge(&mut self, other: &Totals) {
        self.usage.input = self.usage.input.saturating_add(other.usage.input);
        self.usage.output = self.usage.output.saturating_add(other.usage.output);
        self.usage.read = self.usage.read.saturating_add(other.usage.read);
        self.usage.write = self.usage.write.saturating_add(other.usage.write);
        self.records = self.records.saturating_add(other.records);
    }

    fn value(&self) -> Value {
        let cache_hit_rate = (self.usage.input > 0)
            .then(|| self.usage.read as f64 / self.usage.input as f64);
        // `input` already includes cache reads; expose the same headline total
        // used by the dashboard without double-counting the cached portion.
        let total = self
            .usage
            .input
            .saturating_add(self.usage.output)
            .saturating_add(self.usage.write);
        json!({
            "total": total,
            "input": self.usage.input,
            "output": self.usage.output,
            "cacheRead": self.usage.read,
            "cacheWrite": self.usage.write,
            "uncachedInput": self.usage.input.saturating_sub(self.usage.read),
            "records": self.records,
            "cacheHitRate": cache_hit_rate,
        })
    }

    /// 面板的分组项 = 统计字段 + `key`，与 [`groups`] 的输出保持一致。
    fn value_with_key(&self, key: &str) -> Value {
        let mut value = self.value();
        value["key"] = json!(key);
        value
    }
}

/// Read a non-negative integer from a JSON number or string.
fn number(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|n| u64::try_from(n).ok()))
        .or_else(|| value.as_f64().filter(|n| n.is_finite() && *n >= 0.0).map(|n| n as u64))
        .or_else(|| value.as_str()?.trim().parse::<u64>().ok())
}

fn field(object: &Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| object.get(*key).and_then(number))
}

fn positive_field(object: &Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(number)
            .filter(|value| *value > 0)
    })
}

fn cached_input_field(object: &Map<String, Value>) -> u64 {
    // Providers have emitted both flat aliases and OpenAI-compatible nested
    // details. Prefer a positive flat alias so a stale `cache_read...: 0`
    // field cannot hide a populated `prompt_cache_hit_tokens` value.
    positive_field(
        object,
        &[
            "cache_read_input_tokens",
            "cacheReadInputTokens",
            "prompt_cache_hit_tokens",
            "cached_tokens",
        ],
    )
    .or_else(|| {
        object
            .get("prompt_tokens_details")
            .and_then(Value::as_object)
            .and_then(|details| positive_field(details, &["cached_tokens"]))
    })
    .or_else(|| {
        object
            .get("inputTokensDetails")
            .and_then(Value::as_array)
            .and_then(|details| {
                details.iter().find_map(|detail| {
                    detail
                        .as_object()
                        .and_then(|detail| positive_field(detail, &["cached_tokens"]))
                })
            })
    })
    .unwrap_or(0)
}

const CACHE_WRITE_KEYS: &[&str] = &[
    "cache_write_input_tokens",
    "cacheWriteInputTokens",
    "cache_creation_input_tokens",
    "prompt_cache_write_tokens",
];

fn usage_fields(object: &Map<String, Value>) -> Usage {
    Usage {
        input: field(object, &["input_tokens", "inputTokens", "prompt_tokens"]).unwrap_or(0),
        output: field(
            object,
            &["output_tokens", "outputTokens", "completion_tokens"],
        )
        .unwrap_or(0),
        read: cached_input_field(object),
        write: positive_field(object, CACHE_WRITE_KEYS).unwrap_or(0),
    }
}

/// A usage object's cache counts may be incomplete even when it carries the
/// input anchor: WorkBuddyAI CLI writes `message.usage` without any cache
/// field and keeps the real hit count in `providerData.usage` /
/// `providerData.rawUsage`. Missing (or zero) counts are therefore filled from
/// the other candidate objects; populated counts are never overridden.
fn merge_missing_cache(mut selected: Usage, candidates: &[Option<&Value>]) -> Usage {
    let object_at = |index: usize| candidates[index].and_then(Value::as_object);
    if selected.read == 0 {
        selected.read = (0..candidates.len())
            .filter_map(object_at)
            .map(cached_input_field)
            .find(|read| *read > 0)
            .unwrap_or(0);
    }
    if selected.write == 0 {
        selected.write = (0..candidates.len())
            .filter_map(object_at)
            .find_map(|object| positive_field(object, CACHE_WRITE_KEYS))
            .unwrap_or(0);
    }
    selected
}

fn usage_object(value: Option<&Value>) -> Option<&Map<String, Value>> {
    value?.as_object().filter(|object| {
        // Input is the required anchor for a usage record. It may legitimately
        // be zero (for example a provider reports output-only retries), so do
        // not use `input > 0` as the validity check.
        field(object, &["input_tokens", "inputTokens", "prompt_tokens"]).is_some()
    })
}

/// 会话标识的 FNV-1a 64 位哈希。
///
/// 只用来「分桶」：统计接口里只露出低 16 位的掩码，不出现原始标识
/// （反代那边上游会话 id 不外泄）。
pub(crate) fn session_hash(seed: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash & 0xffff
}

/// Decode one record. Usage precedence is message.usage > providerData.usage >
/// top-level usage. Input/output come from the selected object only, so they
/// are never counted twice; missing cache counts (read and write) are filled
/// from every other candidate object and `providerData.rawUsage`.
fn usage(value: &Value) -> Option<Usage> {
    let provider = value.get("providerData");
    let candidates = [
        value.get("message").and_then(|message| message.get("usage")),
        provider.and_then(|data| data.get("usage")),
        value.get("usage"),
    ];
    let selected = candidates.iter().copied().find_map(usage_object)?;
    let result = usage_fields(selected);

    // prompt_cache_miss_tokens is deliberately not a write alias: current
    // WorkBuddy/CodeBuddy logs use it for newly computed (uncached) input,
    // while their explicit cache-write fields may legitimately remain zero.

    let raw_usage = provider.and_then(|data| data.get("rawUsage"));
    Some(merge_missing_cache(
        result,
        &[candidates[0], candidates[1], candidates[2], raw_usage],
    ))
}

fn timestamp(value: &Value) -> Option<i64> {
    value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .and_then(|timestamp| {
            timestamp
                .as_i64()
                .or_else(|| timestamp.as_u64().and_then(|n| i64::try_from(n).ok()))
                .or_else(|| timestamp.as_str()?.trim().parse::<i64>().ok())
        })
}

fn date(value: &Value) -> Option<String> {
    let timestamp = timestamp(value)?;
    chrono::DateTime::from_timestamp_millis(timestamp)
        .map(|date| date.with_timezone(&Local).format("%Y-%m-%d").to_string())
}

fn hour(value: &Value) -> Option<String> {
    let timestamp = timestamp(value)?;
    chrono::DateTime::from_timestamp_millis(timestamp).map(|date| {
        let local = date.with_timezone(&Local);
        format!("{}-{}", local.weekday().num_days_from_monday(), local.hour())
    })
}

fn model(value: &Value) -> String {
    value
        .get("providerData")
        .and_then(|data| data.get("model"))
        .or_else(|| value.get("model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or("未知模型")
        .to_string()
}

fn files(root: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // Subagent logs duplicate parent-session context and are not part
            // of either product's primary usage accounting.
            if path.file_name().and_then(|name| name.to_str()) != Some("subagents") {
                files(&path, output);
            }
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("jsonl") {
            output.push(path);
        }
    }
}

fn project_name(root: &Path, file: &Path) -> String {
    let name = file
        .strip_prefix(root)
        .ok()
        .and_then(|relative| relative.components().next())
        .and_then(|component| component.as_os_str().to_str())
        .filter(|name| !name.is_empty() && !name.ends_with(".jsonl"));
    match name {
        // Product directories commonly encode the complete absolute path.
        // Returning that would leak a user name and parent directories.
        Some(name) if !name.starts_with("Users-") && !name.starts_with("home-") => {
            name.to_string()
        }
        _ => "未知项目".to_string(),
    }
}

fn record_project(value: &Value, fallback: &str) -> String {
    value
        .get("cwd")
        .and_then(Value::as_str)
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty() && name.len() <= 120)
        .unwrap_or(fallback)
        .to_string()
}

fn non_empty_text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn groups(groups: HashMap<String, Totals>) -> Vec<Value> {
    let mut values: Vec<_> = groups
        .into_iter()
        .map(|(key, totals)| {
            let mut value = totals.value();
            value["key"] = json!(key);
            value
        })
        .collect();
    values.sort_by(|left, right| {
        total_value(right).cmp(&total_value(left))
    });
    values
}

fn session_groups(sessions: Vec<SessionTotals>) -> Vec<Value> {
    let mut values: Vec<_> = sessions
        .into_iter()
        .map(|session| {
            let mut value = session.totals.value();
            value["key"] = json!(session.key);
            value["title"] = json!(session.title);
            value["project"] = json!(session.project);
            value["sessionId"] = json!(session.session_id);
            value
        })
        .collect();
    values.sort_by(|left, right| {
        total_value(right).cmp(&total_value(left))
    });
    values
}

fn total_value(value: &Value) -> u64 {
    value
        .get("total")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            value
                .get("input")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .saturating_add(value.get("output").and_then(Value::as_u64).unwrap_or(0))
                .saturating_add(value.get("cacheWrite").and_then(Value::as_u64).unwrap_or(0))
        })
}

/// 把 `%Y-%m-%d` 按**本地时区**展开成当日 0 点 / 23:59:59.999 的毫秒时间戳。
///
/// 反代的 daily 桶只存日期，没有逐条时间戳；覆盖区间只能这样推。
/// 写成显式 `01:00:00` 而不是在 `midnight()` 上加 1 秒，是避免夏令时结束那天
/// 把区间多算 25 小时。
fn local_day_bounds(day: &str) -> Option<(i64, i64)> {
    use chrono::TimeZone;
    let date = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()?;
    let start = Local
        .from_local_datetime(&date.and_hms_opt(0, 0, 0)?)
        .earliest()?;
    let end = Local
        .from_local_datetime(&date.and_hms_milli_opt(23, 59, 59, 999)?)
        .earliest()?;
    Some((
        start.timestamp_millis(),
        end.timestamp_millis().max(start.timestamp_millis()),
    ))
}

fn proxy_bucket_totals(bucket: &Map<String, Value>) -> Totals {
    Totals {
        usage: Usage {
            input: field(bucket, &["promptTokens"]).unwrap_or(0),
            output: field(bucket, &["completionTokens"]).unwrap_or(0),
            read: field(bucket, &["cacheReadTokens"]).unwrap_or(0),
            write: field(bucket, &["cacheWriteTokens"]).unwrap_or(0),
        },
        // 反代写盘时已把多次请求合成一个桶，records 是桶里的请求数，
        // 不能像会话日志那样一个桶只记一条。
        records: field(bucket, &["requests"]).unwrap_or(0),
    }
}

/// 把反代用量文档的「模型 → 日期 → 桶」累加回「模型 → 日期 → Totals」。
///
/// summary / models / daily / dailyByModel 全部由它派生，而不是读各版的 `total`
/// 计数器：total 是逐次请求累加的独立计数器，旧版本迁移/回填后可能与 daily
/// 对不上，一旦两边不一致，「总览」和「趋势图合计」就会互相矛盾。
///
/// 取数顺序由 `totals` 决定：
/// - `false`（趋势视图）：各版的 `dailyByModel`，与界面「今天 / 7 天 / 30 天」按日期
///   筛选同口径；
/// - `true`（累计视图）：各版的 `totalDailyByModel` —— 每版**再单独记一份、永不裁剪**
///   的按天表，用来让「总计」等于「全部历史」，同时保留按天结构（旧版本在
///   `total` 里只留了合计、没有按天拆分的那些请求无法还原，只能按天保留）。
///
/// 老数据文件没有 `totalDailyByModel` 时退回 `dailyByModel`；迁移时会把当时的
/// `dailyByModel` 快照写进 `totalDailyByModel`（见 `proxy::load_usage`）。
fn proxy_daily_by_model(
    doc: &Value,
    totals: bool,
) -> HashMap<String, HashMap<String, Totals>> {
    let mut result: HashMap<String, HashMap<String, Totals>> = HashMap::new();
    for edition in crate::modules::edition::Edition::ALL {
        let Some(bucket) = doc
            .get("byEdition")
            .and_then(|value| value.get(edition.key()))
        else {
            continue;
        };
        let per_model = if totals {
            bucket.get("totalDailyByModel").or_else(|| {
                // 首次升级：还没有快照 ⇒ 用当前按天表作起点（之后的裁剪不影响它）。
                bucket.get("dailyByModel")
            })
        } else {
            bucket.get("dailyByModel")
        };
        let Some(models) = per_model.and_then(Value::as_object) else {
            continue;
        };
        for (model_name, days) in models {
            let Some(days) = days.as_object() else {
                continue;
            };
            for (day, bucket) in days {
                let Some(bucket) = bucket.as_object() else {
                    continue;
                };
                result
                    .entry(model_name.clone())
                    .or_default()
                    .entry(day.clone())
                    .or_default()
                    .merge(&proxy_bucket_totals(bucket));
            }
        }
    }
    result
}

/// 构造 `api-proxy` 来源。拆成纯函数（只读传入的 Value）是为了单测合并/整形逻辑，
/// 不必依赖真实 home 目录里的 `proxy-usage.json`。
///
/// `totals_only` 为真时只返回累计视图（`summary` / `models` / `projects` 以及
/// 命中率所需的按天结构），供「反代页 / 用量视图」使用；为假时返回统计页的完整结构。
fn proxy_source_from(doc: &Value, totals_only: bool) -> Value {
    // 两个版本合并成一个来源：用户看到的是一条完整的反代用量曲线，
    // 不关心请求是从国内版还是国际版账号发出去的。
    let by_model = proxy_daily_by_model(doc, true);

    let mut summary = Totals::default();
    let mut daily_totals: HashMap<String, Totals> = HashMap::new();
    for days in by_model.values() {
        for (day, point) in days {
            summary.merge(point);
            daily_totals.entry(day.clone()).or_default().merge(point);
        }
    }

    let mut daily: Vec<Value> = daily_totals
        .iter()
        .map(|(day, totals)| totals.value_with_key(day))
        .collect();
    // 与 groups() 一致按 total 降序；面板自己会再按日期排序。
    daily.sort_by(|left, right| total_value(right).cmp(&total_value(left)));

    let mut models: Vec<Value> = by_model
        .iter()
        .map(|(model_name, days)| {
            let mut totals = Totals::default();
            for point in days.values() {
                totals.merge(point);
            }
            totals.value_with_key(model_name)
        })
        .collect();
    models.sort_by(|left, right| total_value(right).cmp(&total_value(left)));

    let daily_by_model = by_model
        .iter()
        .map(|(model_name, days)| {
            let mut points: Vec<Value> = days
                .iter()
                .map(|(day, totals)| totals.value_with_key(day))
                .collect();
            points.sort_by(|left, right| total_value(right).cmp(&total_value(left)));
            (model_name.clone(), Value::Array(points))
        })
        .collect::<Map<String, Value>>();

    // 反代不区分项目，但「用量分布」至少需要一个分组，否则整块只显示空态。
    let projects = if by_model.is_empty() {
        Vec::new()
    } else {
        vec![summary.value_with_key("API 反代")]
    };

    // 覆盖区间只能由日期推：没有 daily 时保持 null，前端据此判断该来源是否可用。
    let coverage_start_at = daily_totals
        .keys()
        .filter_map(|day| local_day_bounds(day).map(|(start, _)| start))
        .min();
    let coverage_end_at = daily_totals
        .keys()
        .filter_map(|day| local_day_bounds(day).map(|(_, end)| end))
        .max();

    let cumulative = json!({
        "summary": summary.value(),
        "models": models,
        "projects": projects,
        "dailyByModel": daily_by_model,
    });
    if totals_only {
        return json!({
            "source": "api-proxy",
            "cumulative": cumulative,
            "coverageStartAt": coverage_start_at,
            "coverageEndAt": coverage_end_at,
        });
    }

    // 趋势视图：与界面「今天 / 7 天 / 30 天」筛选同口径，只取各版的 `dailyByModel`。
    let trend_by_model = proxy_daily_by_model(doc, false);
    let mut trend_summary = Totals::default();
    let mut trend_daily: HashMap<String, Totals> = HashMap::new();
    for days in trend_by_model.values() {
        for (day, point) in days {
            trend_summary.merge(point);
            trend_daily.entry(day.clone()).or_default().merge(point);
        }
    }
    let mut daily: Vec<Value> = trend_daily
        .iter()
        .map(|(day, totals)| totals.value_with_key(day))
        .collect();
    // 与 groups() 一致按 total 降序；面板自己会再按日期排序。
    daily.sort_by(|left, right| total_value(right).cmp(&total_value(left)));
    let trend_daily_by_model = trend_by_model
        .iter()
        .map(|(model_name, days)| {
            let mut points: Vec<Value> = days
                .iter()
                .map(|(day, totals)| totals.value_with_key(day))
                .collect();
            points.sort_by(|left, right| total_value(right).cmp(&total_value(left)));
            (model_name.clone(), Value::Array(points))
        })
        .collect::<Map<String, Value>>();
    let trend_models = trend_by_model
        .iter()
        .map(|(model_name, days)| {
            let mut totals = Totals::default();
            for point in days.values() {
                totals.merge(point);
            }
            totals.value_with_key(model_name)
        })
        .collect::<Vec<Value>>();
    let trend_projects = if trend_by_model.is_empty() {
        Vec::new()
    } else {
        vec![trend_summary.value_with_key("API 反代")]
    };

    json!({
        "source": "api-proxy",
        // 「总计」= 累计投影（全部按天历史）；趋势与热力图用 `daily` / `dailyByModel`。
        "summary": summary.value(),
        "models": models,
        "projects": projects,
        // 反代没有会话与小时桶，这两个数组保持为空。
        "sessions": [],
        "daily": daily,
        "dailyByModel": trend_daily_by_model,
        "trend": {
            "summary": trend_summary.value(),
            "models": trend_models,
            "projects": trend_projects,
        },
        "cumulative": cumulative,
        // 最近请求按会话（掩码）拆分：用来对照「同一会话换号 / 粘号」时的命中率。
        "bySession": if totals_only { Value::Null } else { proxy_recent_by_session(doc) },
        "hours": [],
        "filesScanned": 0,
        "parseErrors": 0,
        "coverageStartAt": coverage_start_at,
        "coverageEndAt": coverage_end_at,
    })
}

/// 读取 `~/.wb-switch/proxy-usage.json` 并合并两版，产出与 [`source`] 同构的 Value。
fn proxy_source() -> Value {
    proxy_source_from(&crate::modules::proxy::load_usage(), false)
}

/// 反代 `recent`（每版最多 100 条）按会话掩码拆分。
///
/// 只统计带会话标识请求的**同口径**用量：同一个会话在粘滞前后各自命中多少。
/// 没带会话标识的请求（纯轮转）不进这里，避免把两种调度混在一起比。
fn proxy_recent_by_session(doc: &Value) -> Value {
    let mut bundle = TotalsBundle::default();
    for edition in crate::modules::edition::Edition::ALL {
        let Some(recent) = doc
            .get("byEdition")
            .and_then(|value| value.get(edition.key()))
            .and_then(|value| value.get("recent"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for item in recent.iter().filter_map(Value::as_object) {
            let Some(session) = field(item, &["session"]) else {
                continue;
            };
            if item.get("ok").and_then(Value::as_bool) == Some(false) {
                continue;
            }
            bundle.add(
                session,
                Usage {
                    input: field(item, &["promptTokens"]).unwrap_or(0),
                    output: field(item, &["completionTokens"]).unwrap_or(0),
                    read: field(item, &["cacheReadTokens"]).unwrap_or(0),
                    write: field(item, &["cacheWriteTokens"]).unwrap_or(0),
                },
            );
        }
    }
    bundle.totals()
}

#[derive(Default)]
struct SourceCollector {
    total: Totals,
    bundles: TotalsBundle,
    models: HashMap<String, Totals>,
    projects: HashMap<String, Totals>,
    sessions: Vec<SessionTotals>,
    daily: HashMap<String, Totals>,
    daily_by_model: HashMap<String, HashMap<String, Totals>>,
    hours: HashMap<String, Totals>,
    parse_errors: u64,
    coverage_start_at: Option<i64>,
    coverage_end_at: Option<i64>,
    session_key_counts: HashMap<String, usize>,
}

impl SourceCollector {
    fn note_parse_error(&mut self) {
        self.parse_errors = self.parse_errors.saturating_add(1);
    }

    fn add(&mut self, usage: Usage, value: &Value, project: &str, session: u64) {
        self.total.add(usage);
        self.bundles.add(session, usage);
        let model_name = model(value);
        self.models
            .entry(model_name.clone())
            .or_default()
            .add(usage);
        self.projects
            .entry(project.to_string())
            .or_default()
            .add(usage);
        if let Some(day) = date(value) {
            self.daily.entry(day.clone()).or_default().add(usage);
            self.daily_by_model
                .entry(model_name)
                .or_default()
                .entry(day)
                .or_default()
                .add(usage);
        }
        if let Some(hour) = hour(value) {
            self.hours.entry(hour).or_default().add(usage);
        }
        if let Some(timestamp) = timestamp(value) {
            self.coverage_start_at = Some(
                self.coverage_start_at
                    .map_or(timestamp, |current| current.min(timestamp)),
            );
            self.coverage_end_at = Some(
                self.coverage_end_at
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
        }
    }

    fn push_session(
        &mut self,
        session_id: String,
        title: Option<String>,
        project: String,
        totals: Totals,
    ) {
        if totals.records == 0 {
            return;
        }
        let base_key = format!("{project} · {session_id}");
        let count = self.session_key_counts.entry(base_key.clone()).or_default();
        *count += 1;
        let key = if *count == 1 {
            base_key
        } else {
            format!("{base_key} · {}", *count)
        };
        self.sessions.push(SessionTotals {
            key,
            title,
            project,
            session_id,
            totals,
        });
    }

    fn into_value(self, name: &str, files_scanned: usize) -> Value {
        let daily_by_model = self
            .daily_by_model
            .into_iter()
            .map(|(model, points)| (model, Value::Array(groups(points))))
            .collect::<Map<String, Value>>();
        json!({
            "source": name,
            "summary": self.total.value(),
            "models": groups(self.models),
            "projects": groups(self.projects),
            "sessions": session_groups(self.sessions),
            "daily": groups(self.daily),
            "dailyByModel": daily_by_model,
            "hours": groups(self.hours),
            "filesScanned": files_scanned,
            "parseErrors": self.parse_errors,
            "coverageStartAt": self.coverage_start_at,
            "coverageEndAt": self.coverage_end_at,
            // 按会话（掩码）拆分的同口径统计，供「粘滞选号是否有效」的对照评估。
            "bySession": self.bundles.totals(),
        })
    }
}

fn source(root: PathBuf, name: &str, cutoff: Option<i64>) -> Value {
    let mut paths = Vec::new();
    files(&root, &mut paths);
    let mut collector = SourceCollector::default();

    paths.sort();
    for path in &paths {
        let session_id = path
            .file_stem()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("未知会话")
            .to_string();
        let fallback_project = project_name(&root, path);
        let Ok(file) = std::fs::File::open(path) else {
            collector.note_parse_error();
            continue;
        };

        let mut session_totals = Totals::default();
        let mut session_project: Option<String> = None;
        let mut ai_title: Option<String> = None;
        let mut summary: Option<String> = None;

        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                collector.note_parse_error();
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&line) else {
                collector.note_parse_error();
                continue;
            };
            // Title metadata belongs to the whole JSONL session file. Read it
            // before applying the usage cutoff so an older title can still
            // label usage that falls inside the selected range. aiTitle has
            // precedence over summary regardless of event order.
            if let Some(title) = non_empty_text(value.get("aiTitle")) {
                ai_title = Some(title);
            }
            if let Some(value) = non_empty_text(value.get("summary")) {
                summary = Some(value);
            }
            // Records with a missing timestamp are excluded from a bounded
            // range rather than guessed from file mtime or browser time.
            if cutoff.is_some_and(|minimum| timestamp(&value).is_none_or(|ts| ts < minimum)) {
                continue;
            }
            let Some(usage) = usage(&value) else {
                continue;
            };
            let project = record_project(&value, &fallback_project);
            if session_project.is_none() {
                session_project = Some(project.clone());
            }
            session_totals.add(usage);
            collector.add(usage, &value, &project, session_hash(&session_id));
        }

        collector.push_session(
            session_id,
            ai_title.or(summary),
            session_project.unwrap_or(fallback_project),
            session_totals,
        );
    }

    collector.into_value(name, paths.len())
}

fn codebuddy_extension_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("CodeBuddyExtension")
        .join("Data")
}

fn is_ide_conversation_index(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("index.json")
        && path
            .parent()
            .and_then(|parent| parent.parent())
            .and_then(|parent| parent.parent())
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            == Some("history")
}

fn ide_index_files(root: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|name| name.to_str());
            // Message bodies contain chat content and must not be scanned.
            // Checkpoints and the shared Public bucket are unrelated to usage.
            if !matches!(name, Some("messages" | "check-point" | "backups" | "Public")) {
                ide_index_files(&path, output);
            }
        } else if is_ide_conversation_index(&path) {
            output.push(path);
        }
    }
}

fn decode_genie_workspace(name: &str) -> Option<String> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;
    let try_decode = |value: &str| -> Option<String> {
        let padded = match value.len() % 4 {
            0 => value.to_string(),
            remainder => format!("{value}{}", "=".repeat(4 - remainder)),
        };
        let bytes = engine.decode(padded).ok()?;
        let text = String::from_utf8(bytes).ok()?;
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed.contains('\0') {
            return None;
        }
        Some(trimmed.to_string())
    };
    try_decode(name)
        .or_else(|| try_decode(&name.replace('_', "/")))
        .or_else(|| try_decode(&name.replace('_', "+")))
}

fn ide_project_by_session() -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Some(root) = crate::modules::vscode_cn_inject::codebuddy_cn_data_dir().map(|dir| {
        dir.join("User")
            .join("globalStorage")
            .join("tencent-cloud.coding-copilot")
            .join("genie-history")
    }) else {
        return map;
    };
    let Ok(entries) = std::fs::read_dir(root) else {
        return map;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let folder = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let project = decode_genie_workspace(folder)
            .as_deref()
            .map(Path::new)
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .map(str::trim)
            .filter(|name| !name.is_empty() && name.len() <= 120)
            .unwrap_or("未知项目")
            .to_string();
        let Ok(conversations) = std::fs::read_dir(path.join("conversations")) else {
            continue;
        };
        for conversation in conversations.flatten() {
            if let Some(id) = conversation.file_name().to_str() {
                if !id.is_empty() {
                    map.insert(id.to_string(), project.clone());
                }
            }
        }
    }
    map
}

fn ide_workspace_meta(conv_index: &Path, conv_id: &str) -> (Option<String>, String) {
    let Some(ws_index) = conv_index
        .parent()
        .and_then(|parent| parent.parent())
        .map(|parent| parent.join("index.json"))
    else {
        return (None, "未知模型".to_string());
    };
    let Ok(text) = std::fs::read_to_string(ws_index) else {
        return (None, "未知模型".to_string());
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return (None, "未知模型".to_string());
    };
    let Some(conversations) = value.get("conversations").and_then(Value::as_array) else {
        return (None, "未知模型".to_string());
    };
    for conversation in conversations {
        if conversation.get("id").and_then(Value::as_str) != Some(conv_id) {
            continue;
        }
        let title = non_empty_text(conversation.get("name"))
            .or_else(|| non_empty_text(conversation.get("title")));
        let model = non_empty_text(conversation.get("selectedModelId"))
            .or_else(|| non_empty_text(conversation.get("modelId")))
            .or_else(|| non_empty_text(conversation.get("model")))
            .unwrap_or_else(|| "未知模型".to_string());
        return (title, model);
    }
    (None, "未知模型".to_string())
}

fn ide_request_usage(request: &Value) -> Option<Usage> {
    let object = request.get("usage")?.as_object()?;
    if field(object, &["inputTokens", "input_tokens", "prompt_tokens"]).is_none() {
        return None;
    }
    Some(Usage {
        input: field(object, &["inputTokens", "input_tokens", "prompt_tokens"]).unwrap_or(0),
        output: field(
            object,
            &["outputTokens", "output_tokens", "completion_tokens"],
        )
        .unwrap_or(0),
        read: field(
            object,
            &[
                "cacheTokens",
                "cacheReadInputTokens",
                "cache_read_input_tokens",
            ],
        )
        .unwrap_or(0),
        write: positive_field(
            object,
            &[
                "cachedWriteTokens",
                "cacheWriteInputTokens",
                "cache_write_input_tokens",
                "cache_creation_input_tokens",
            ],
        )
        .unwrap_or(0),
    })
}

fn ide_request_timestamp(request: &Value) -> Option<i64> {
    timestamp(request).or_else(|| {
        request.get("startedAt").and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|n| i64::try_from(n).ok()))
                .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
        })
    })
}

/// 单条请求的指纹：优先「时间戳 + 用量」，缺时间戳时退回请求 id。
///
/// 副本之间是逐字重放——时间戳与用量都不变——所以这个指纹可以直接当并集去重的键；
/// 两者都没有就返回 `None`（不进并集，按原样计数，实测不存在这种记录）。
/// 一次用量的可比总量：用于挑出最完整的那份副本（cache read 不算，它只反映复用）。
fn usage_total(usage: Usage) -> u64 {
    usage
        .input
        .saturating_add(usage.output)
        .saturating_add(usage.write)
}

fn ide_request_fingerprint(
    request: &Value,
    usage: &Usage,
    timestamp: Option<i64>,
) -> Option<String> {
    Some(match timestamp {
        Some(timestamp) => format!(
            "{timestamp}:{}:{}:{}:{}",
            usage.input, usage.output, usage.read, usage.write
        ),
        None => non_empty_text(request.get("id"))?.to_string(),
    })
}

/// 会话 id：会话目录名。副本会被重随机 id，所以它**不是**会话身份（见 [`ide_file_weight`]）。
fn ide_session_id(path: &Path) -> String {
    path.parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("未知会话")
        .to_string()
}

/// 一份会话 index 的轻量指标：**会话指纹**（首个可计入请求的指纹，同一会话的所有副本
/// 共享它）、可计入的请求条数、token 总量。
///
/// 切号复制会让同一会话在多个账号数据目录下各留一份：副本整份重放父历史，之后各自继续
/// 追加——所以各份长短不一、内容并不完全相同，会话 id 也可能被重随机（插件侧
/// `CopyIdPolicy::AlwaysNew`）。会话身份因此不能按目录名判，只能按「同一个起点请求」判。
/// 指标里的条数/总量用于挑出最完整的那份，作为该会话的标题、模型与项目归属。
fn ide_file_weight(path: &Path) -> Option<(String, usize, u64)> {
    let text = std::fs::read_to_string(path).ok()?;
    let value = serde_json::from_str::<Value>(&text).ok()?;
    let requests = value.get("requests")?.as_array()?;
    let mut key: Option<String> = None;
    let mut count = 0usize;
    let mut total = 0u64;
    for request in requests {
        let Some(usage) = ide_request_usage(request) else {
            continue;
        };
        if key.is_none() {
            key = ide_request_fingerprint(request, &usage, ide_request_timestamp(request));
        }
        count += 1;
        total = total.saturating_add(usage_total(usage));
    }
    Some((key?, count, total))
}

/// 把一个会话（`requests` 是它的某一份副本）里可计入的请求并入统计。
///
/// `seen` 按请求指纹去重、跨副本共享：副本重放父历史时带的是原始时间戳，所以并集
/// 既不会把历史日期重复抬高，也不会丢掉各副本复制后各自追加的请求。指纹缺失的记录
/// 一律计入。
fn collect_ide_requests(
    #[allow(clippy::too_many_arguments)] collector: &mut SourceCollector,
    requests: &[Value],
    cutoff: Option<i64>,
    model_name: &str,
    project: &str,
    session: u64,
    seen: &mut HashSet<String>,
    session_totals: &mut Totals,
) {
    for request in requests {
        let ts = ide_request_timestamp(request);
        if cutoff.is_some_and(|minimum| ts.is_none_or(|value| value < minimum)) {
            continue;
        }
        let Some(usage) = ide_request_usage(request) else {
            continue;
        };
        if let Some(fingerprint) = ide_request_fingerprint(request, &usage, ts) {
            // 同一请求已在另一份副本里计过。
            if !seen.insert(fingerprint) {
                continue;
            }
        }
        session_totals.add(usage);
        collector.add(
            usage,
            &json!({
                "timestamp": ts,
                "providerData": { "model": model_name },
            }),
            project,
            session,
        );
    }
}

fn ide_source(
    root: PathBuf,
    name: &str,
    cutoff: Option<i64>,
    project_by_session: &HashMap<String, String>,
) -> Value {
    let mut paths = Vec::new();
    ide_index_files(&root, &mut paths);
    paths.sort();

    // 同一会话（按起点请求判身份）因切号复制可能散落在多个账号数据目录里。各份之间按
    // 请求指纹取**并集**：父历史只算一次（副本会把历史日期回溯性抬高），副本复制后各自
    // 追加的请求也都保留（只取「最长一份」会漏掉别的副本独有的请求）。会话标题、模型与
    // 项目归属取最完整的那份。
    let weights: Vec<Option<(String, usize, u64)>> =
        paths.iter().map(|path| ide_file_weight(path)).collect();
    let mut groups: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut lead: HashMap<&str, (usize, usize, u64)> = HashMap::new();
    for (index, weight) in weights.iter().enumerate() {
        let Some((fingerprint, count, total)) = weight else {
            continue;
        };
        groups.entry(fingerprint.as_str()).or_default().push(index);
        let replace = lead
            .get(fingerprint.as_str())
            .is_none_or(|current| (*count, *total) > (current.1, current.2));
        if replace {
            lead.insert(fingerprint.as_str(), (index, *count, *total));
        }
    }

    let mut collector = SourceCollector::default();

    for (index, _) in paths.iter().enumerate() {
        let fingerprint = weights[index]
            .as_ref()
            .map(|(fingerprint, _, _)| fingerprint.as_str());
        // 同一会话只在它的第一份副本处处理一次，并把其余副本一起并入。读不出会话指纹
        // （读取/解析失败）的文件照旧单独走一遍，让解析错误被记一笔。
        if let Some(fingerprint) = fingerprint {
            if groups.get(fingerprint).and_then(|members| members.first()) != Some(&index) {
                continue;
            }
        }
        // 标题/模型/项目取最完整的那份：副本 id 被重随机过，元信息不一定都齐全。
        let lead_index = fingerprint
            .and_then(|fingerprint| lead.get(fingerprint).map(|current| current.0))
            .unwrap_or(index);
        let lead_session_id = ide_session_id(&paths[lead_index]);
        let (title, model_name) = ide_workspace_meta(&paths[lead_index], &lead_session_id);
        let project = project_by_session
            .get(&lead_session_id)
            .cloned()
            .unwrap_or_else(|| "未知项目".to_string());
        let members: &[usize] = match fingerprint {
            Some(fingerprint) => groups
                .get(fingerprint)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            None => std::slice::from_ref(&index),
        };
        let mut session_totals = Totals::default();
        let mut seen: HashSet<String> = HashSet::new();
        for member in members {
            let Ok(text) = std::fs::read_to_string(&paths[*member]) else {
                collector.note_parse_error();
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&text) else {
                collector.note_parse_error();
                continue;
            };
            let Some(requests) = value.get("requests").and_then(Value::as_array) else {
                continue;
            };
            collect_ide_requests(
                &mut collector,
                requests,
                cutoff,
                &model_name,
                &project,
                session_hash(&lead_session_id),
                &mut seen,
                &mut session_totals,
            );
        }

        collector.push_session(lead_session_id, title, project, session_totals);
    }

    collector.into_value(name, paths.len())
}

/// Return independent WorkBuddy (domestic), WorkBuddyAI (international),
/// CodeBuddy CLI, CodeBuddy IDE, and API proxy aggregates.
/// `days` is interpreted in Rust using the same millisecond clock for every source.
/// 只取 `api-proxy` 来源的**累计视图**（全部按天历史，不套 7/30/90 天裁剪）。
///
/// 给「反代页 / 用量视图」用：那里的数字表示全部历史，与本文件顶部按天累积的口径一致。
pub fn proxy_usage_totals() -> Value {
    proxy_source_from(&crate::modules::proxy::load_usage(), true)
}

pub fn get_statistics(days: Option<i64>) -> Value {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let generated_at = crate::modules::config::now_ms();
    let range_days = match days {
        Some(7) => Some(7),
        Some(30) => Some(30),
        Some(90) => Some(90),
        _ => None,
    };
    let cutoff = range_days.map(|value| generated_at - value * 86_400_000);
    let ide_projects = ide_project_by_session();
    json!({
        "generatedAt": generated_at,
        "rangeDays": range_days,
        "sources": [
            source(home.join(".workbuddy/projects"), "workbuddy", cutoff),
            // 国际版（WorkBuddyAI）与国内版并存：两者数据目录独立，
            // 日志格式一致，复用同一个 source() 解析器。
            source(home.join(".workbuddy-ai/projects"), "workbuddy-ai", cutoff),
            source(home.join(".codebuddy/projects"), "codebuddy-cli", cutoff),
            ide_source(
                codebuddy_extension_data_dir(),
                "codebuddy-ide",
                cutoff,
                &ide_projects,
            ),
            // 反代用量**不套 cutoff**：proxy-usage.json 的 daily 桶本身就是全量历史，
            // 面板各视图（今天/7 天/30 天/本月）会按日期自行筛选；在这里裁剪反而会把
            // 历史曲线提前截断。这与上面 4 个按文件日志扫描的来源不同。
            proxy_source(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn usage_priority_aliases_and_raw_cache_write() {
        let value = json!({
            "providerData": {
                "usage": { "inputTokens": 99, "outputTokens": 22 },
                "rawUsage": { "prompt_cache_write_tokens": 2 }
            },
            "message": { "usage": {
                "input_tokens": 10,
                "output_tokens": 3,
                "cache_read_input_tokens": 4
            }}
        });
        assert_eq!(usage(&value), Some(Usage { input: 10, output: 3, read: 4, write: 2 }));
    }

    #[test]
    fn cache_write_uses_explicit_aliases_but_never_cache_miss() {
        let provider_usage_write = json!({
            "providerData": {
                "usage": {
                    "inputTokens": 99,
                    "outputTokens": 22,
                    "cache_write_input_tokens": 0,
                    "cache_creation_input_tokens": 7
                },
                "rawUsage": {
                    "prompt_cache_miss_tokens": 91,
                    "prompt_cache_write_tokens": 0
                }
            },
            "message": { "usage": {
                "input_tokens": 10,
                "output_tokens": 3,
                "cache_read_input_tokens": 4
            }}
        });
        assert_eq!(
            usage(&provider_usage_write),
            Some(Usage {
                input: 10,
                output: 3,
                read: 4,
                write: 7,
            })
        );

        let cache_miss_only = json!({
            "providerData": {
                "rawUsage": { "prompt_cache_miss_tokens": 91 }
            },
            "message": { "usage": {
                "input_tokens": 10,
                "output_tokens": 3
            }}
        });
        assert_eq!(
            usage(&cache_miss_only),
            Some(Usage {
                input: 10,
                output: 3,
                read: 0,
                write: 0,
            })
        );
    }

    #[test]
    fn cache_read_accepts_nested_provider_details() {
        let value = json!({
            "providerData": {
                "usage": {
                    "inputTokens": 99,
                    "outputTokens": 3,
                    "inputTokensDetails": [{ "cached_tokens": 7 }]
                }
            }
        });
        assert_eq!(
            usage(&value),
            Some(Usage {
                input: 99,
                output: 3,
                read: 7,
                write: 0,
            })
        );

        let raw = json!({
            "usage": {
                "prompt_tokens": 20,
                "completion_tokens": 2,
                "cache_read_input_tokens": 0,
                "prompt_cache_hit_tokens": 12
            }
        });
        assert_eq!(
            usage(&raw),
            Some(Usage {
                input: 20,
                output: 2,
                read: 12,
                write: 0,
            })
        );

    }

    #[test]
    fn source_excludes_subagents_and_counts_each_record_once() {
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-{}-{}",
            std::process::id(),
            crate::modules::config::now_ms()
        ));
        let project = root.join("fixture-project");
        let ignored = project.join("subagents");
        fs::create_dir_all(&ignored).expect("create fixture dirs");
        let record = json!({
            "timestamp": crate::modules::config::now_ms(),
            "providerData": {
                "model": "fixture-model",
                "usage": { "inputTokens": 20, "outputTokens": 5 }
            },
            "message": { "usage": {
                "input_tokens": 10,
                "output_tokens": 3,
                "cache_read_input_tokens": 4
            }}
        });
        fs::write(project.join("session.jsonl"), format!("{}\nnot-json\n", record))
            .expect("write fixture");
        fs::write(ignored.join("agent.jsonl"), format!("{}\n", record)).expect("write ignored fixture");

        let result = source(root.clone(), "fixture", None);
        assert_eq!(result["filesScanned"], 1);
        assert_eq!(result["parseErrors"], 1);
        assert_eq!(result["summary"]["input"], 10);
        assert_eq!(result["summary"]["output"], 3);
        assert_eq!(result["summary"]["cacheRead"], 4);
        assert_eq!(result["summary"]["total"], 13);
        assert_eq!(result["summary"]["records"], 1);
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        assert_eq!(result["dailyByModel"]["fixture-model"][0]["key"], today);
        assert_eq!(result["projects"][0]["key"], "fixture-project");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn bounded_source_excludes_records_before_cutoff() {
        let now = crate::modules::config::now_ms();
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-range-{}-{now}",
            std::process::id()
        ));
        let project = root.join("fixture-project");
        fs::create_dir_all(&project).expect("create fixture dirs");
        let record = |timestamp| {
            json!({
                "timestamp": timestamp,
                "cwd": "/fixture/example-project",
                "message": { "usage": { "input_tokens": 10, "output_tokens": 2 } }
            })
        };
        fs::write(
            project.join("session.jsonl"),
            format!("{}\n{}\n", record(now - 10_000), record(now - 100_000)),
        )
        .expect("write fixture");

        let result = source(root.clone(), "fixture", Some(now - 50_000));
        assert_eq!(result["summary"]["records"], 1);
        assert_eq!(result["summary"]["input"], 10);
        assert_eq!(result["projects"][0]["key"], "example-project");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn session_titles_are_file_scoped_and_independent_from_usage_cutoff() {
        let now = crate::modules::config::now_ms();
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-titles-{}-{now}",
            std::process::id()
        ));
        let project = root.join("fixture-project");
        fs::create_dir_all(&project).expect("create fixture dirs");
        let usage_record = |input| {
            json!({
                "timestamp": now,
                "cwd": "/private/example-project",
                "message": { "usage": { "input_tokens": input, "output_tokens": 2 } }
            })
        };

        fs::write(
            project.join("session-a.jsonl"),
            format!(
                "{}\n{}\n{}\n{}\n",
                json!({ "type": "summary", "summary": "摘要不应覆盖 AI 标题" }),
                json!({ "type": "ai-title", "aiTitle": "旧标题" }),
                usage_record(10),
                json!({ "type": "ai-title", "aiTitle": "最新 AI 标题" }),
            ),
        )
        .expect("write ai title fixture");
        fs::write(
            project.join("session-b.jsonl"),
            format!(
                "{}\n{}\n",
                json!({
                    "type": "ai-title",
                    "timestamp": now - 100_000,
                    "aiTitle": "范围外保留标题"
                }),
                usage_record(20),
            ),
        )
        .expect("write cutoff title fixture");
        fs::write(
            project.join("session-c.jsonl"),
            format!(
                "{}\n{}\n",
                usage_record(30),
                json!({ "type": "summary", "summary": "摘要回退标题" }),
            ),
        )
        .expect("write summary fixture");
        fs::write(
            project.join("session-d.jsonl"),
            format!("{}\n", usage_record(40)),
        )
        .expect("write untitled fixture");
        fs::write(
            project.join("session-e.jsonl"),
            format!(
                "{}\n{}\n",
                json!({ "type": "ai-title", "aiTitle": "最新 AI 标题" }),
                usage_record(50),
            ),
        )
        .expect("write duplicate title fixture");

        let result = source(root.clone(), "fixture", Some(now - 50_000));
        let sessions = result["sessions"].as_array().expect("session groups");
        let by_id = |session_id: &str| {
            sessions
                .iter()
                .find(|session| session["sessionId"] == session_id)
                .expect("session group by id")
        };

        assert_eq!(result["summary"]["input"], 150);
        assert_eq!(result["summary"]["records"], 5);
        assert_eq!(sessions.len(), 5);
        assert_eq!(by_id("session-a")["title"], "最新 AI 标题");
        assert_eq!(by_id("session-b")["title"], "范围外保留标题");
        assert_eq!(by_id("session-c")["title"], "摘要回退标题");
        assert!(by_id("session-d")["title"].is_null());
        assert_eq!(by_id("session-a")["project"], "example-project");
        assert_ne!(by_id("session-a")["key"], by_id("session-e")["key"]);

        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// 回归：切号复制产生的会话副本按请求指纹取并集，父历史只算一次。
    #[test]
    fn ide_source_merges_copied_session_history_as_a_union() {
        let now = crate::modules::config::now_ms();
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-ide-copy-{}-{now}",
            std::process::id()
        ));
        let conversation = |uid: &str, conv: &str| {
            root.join(uid)
                .join("VSCode")
                .join(uid)
                .join("history")
                .join("workspace-hash")
                .join(conv)
        };
        let original = conversation("uid-a", "conv-a");
        // 插件侧副本取新 id：目录名与源不同，但起点请求相同。
        let replay = conversation("uid-b", "conv-replay");
        // 另一个账号从同一份历史往下走：两份分叉后各自独有的请求都要保留。
        let fork = conversation("uid-c", "conv-fork");
        for dir in [&original, &replay, &fork] {
            fs::create_dir_all(dir).expect("create conversation dirs");
        }
        let request = |id: &str, input: u64| {
            json!({
                "id": id,
                "state": "complete",
                "startedAt": now,
                "usage": {
                    "inputTokens": input,
                    "outputTokens": 0,
                    "cacheTokens": 0,
                    "cachedWriteTokens": 0
                }
            })
        };
        fs::write(
            original.join("index.json"),
            json!({ "requests": [request("req-1", 100)] }).to_string(),
        )
        .expect("write original conversation");
        fs::write(
            replay.join("index.json"),
            json!({ "requests": [request("req-1", 100), request("req-2", 200)] }).to_string(),
        )
        .expect("write replayed conversation");
        fs::write(
            fork.join("index.json"),
            json!({ "requests": [request("req-1", 100), request("req-3", 400)] }).to_string(),
        )
        .expect("write forked conversation");

        let result = ide_source(root.clone(), "codebuddy-ide", None, &HashMap::new());

        assert_eq!(result["filesScanned"], 3);
        // 并集：100 + 200 + 400；重放的 req-1 只算一次，分叉两边独有的请求都在。
        assert_eq!(result["summary"]["records"], 3);
        assert_eq!(result["summary"]["input"], 700);
        assert_eq!(result["summary"]["total"], 700);
        // 同一个会话只出一行，不出现重复副本。
        assert_eq!(result["sessions"].as_array().map(Vec::len), Some(1));

        fs::remove_dir_all(root).expect("remove fixture");
    }


    #[test]
    fn ide_source_reads_request_usage_and_skips_message_bodies() {
        let now = crate::modules::config::now_ms();
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-ide-{}-{now}",
            std::process::id()
        ));
        let history = root
            .join("uid")
            .join("CodeBuddyIDE")
            .join("uid")
            .join("history")
            .join("workspace-hash");
        let conv_a = history.join("conv-a");
        let conv_b = history.join("conv-b");
        let messages = conv_a.join("messages");
        fs::create_dir_all(&messages).expect("create ide fixture dirs");
        fs::create_dir_all(&conv_b).expect("create second conversation");

        fs::write(
            history.join("index.json"),
            json!({
                "conversations": [
                    {
                        "id": "conv-a",
                        "name": "IDE 会话标题",
                        "selectedModelId": "deepseek-v4-flash"
                    },
                    {
                        "id": "conv-b",
                        "name": "范围外会话",
                        "selectedModelId": "hy4-preview"
                    }
                ]
            })
            .to_string(),
        )
        .expect("write workspace index");
        fs::write(
            conv_a.join("index.json"),
            json!({
                "messages": [{ "id": "m1", "role": "assistant", "isComplete": true }],
                "requests": [{
                    "id": "req-1",
                    "state": "complete",
                    "startedAt": now,
                    "usage": {
                        "inputTokens": 100,
                        "outputTokens": 20,
                        "cacheTokens": 40,
                        "cachedWriteTokens": 5
                    }
                }]
            })
            .to_string(),
        )
        .expect("write conversation index");
        fs::write(
            conv_b.join("index.json"),
            json!({
                "requests": [{
                    "id": "req-old",
                    "state": "complete",
                    "startedAt": now - 100_000,
                    "usage": {
                        "inputTokens": 999,
                        "outputTokens": 9,
                        "cacheTokens": 1,
                        "cachedWriteTokens": 0
                    }
                }]
            })
            .to_string(),
        )
        .expect("write out-of-range conversation");
        fs::write(
            messages.join("ignored.json"),
            json!({
                "role": "assistant",
                "usage": { "inputTokens": 10_000, "outputTokens": 10_000 }
            })
            .to_string(),
        )
        .expect("write ignored message body");

        let mut projects = HashMap::new();
        projects.insert("conv-a".to_string(), "example-project".to_string());
        let result = ide_source(root.clone(), "codebuddy-ide", Some(now - 50_000), &projects);

        assert_eq!(result["source"], "codebuddy-ide");
        assert_eq!(result["filesScanned"], 2);
        assert_eq!(result["summary"]["records"], 1);
        assert_eq!(result["summary"]["input"], 100);
        assert_eq!(result["summary"]["output"], 20);
        assert_eq!(result["summary"]["cacheRead"], 40);
        assert_eq!(result["summary"]["cacheWrite"], 5);
        assert_eq!(result["summary"]["total"], 125);
        assert_eq!(result["summary"]["uncachedInput"], 60);
        assert_eq!(result["models"][0]["key"], "deepseek-v4-flash");
        assert_eq!(result["projects"][0]["key"], "example-project");
        assert_eq!(result["sessions"][0]["sessionId"], "conv-a");
        assert_eq!(result["sessions"][0]["title"], "IDE 会话标题");
        assert_eq!(result["sessions"].as_array().map(Vec::len), Some(1));
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        assert_eq!(result["dailyByModel"]["deepseek-v4-flash"][0]["key"], today);

        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn decode_genie_workspace_recovers_unix_project_path() {
        assert_eq!(
            decode_genie_workspace("L1VzZXJzL2FwcGxlL0RvY3VtZW50cy9Qcm9qZWN0L215LWFnZW50")
                .as_deref(),
            Some("/Users/apple/Documents/Project/my-agent")
        );
    }

    #[test]
    fn get_statistics_returns_five_isolated_sources() {
        let value = get_statistics(None);
        let sources = value["sources"].as_array().expect("sources");
        let names: Vec<_> = sources
            .iter()
            .map(|source| source["source"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(
            names,
            [
                "workbuddy",
                "workbuddy-ai",
                "codebuddy-cli",
                "codebuddy-ide",
                "api-proxy"
            ]
        );
    }

    /// 反代桶的测试夹具：只有 dailyByModel 参与统计，故意把 total 写成错值，
    /// 用来证明合并/汇总不依赖旧的 total 计数器。
    fn proxy_usage_bucket(prompt: u64, completion: u64, read: u64, write: u64, requests: u64) -> Value {
        json!({
            "requests": requests,
            "errors": 0,
            "promptTokens": prompt,
            "completionTokens": completion,
            "cacheReadTokens": read,
            "cacheWriteTokens": write,
            "credit": 0,
        })
    }

    fn proxy_usage_doc(editions: &[(&str, Value)]) -> Value {
        let mut by_edition = Map::new();
        for (edition, daily_by_model) in editions {
            by_edition.insert(
                (*edition).to_string(),
                json!({
                    // 与 dailyByModel 不一致的 total：必须被忽略。
                    "total": proxy_usage_bucket(999_999, 999_999, 0, 0, 9_999),
                    "byAccount": {},
                    "byModel": {},
                    "daily": {},
                    "dailyByModel": daily_by_model,
                    "recent": [],
                }),
            );
        }
        json!({ "updatedAt": 0, "byEdition": by_edition })
    }

    #[test]
    fn proxy_source_merges_both_editions_and_shapes_daily_points() {
        let doc = proxy_usage_doc(&[
            (
                "domestic",
                json!({
                    "deepseek-v4-flash": {
                        "2026-10-01": proxy_usage_bucket(100, 20, 40, 5, 2),
                    },
                }),
            ),
            (
                "international",
                json!({
                    "deepseek-v4-flash": {
                        // 同一天两个版本都有量 ⇒ 相加。
                        "2026-10-01": proxy_usage_bucket(50, 10, 60, 0, 1),
                    },
                }),
            ),
        ]);

        let source = proxy_source_from(&doc, false);
        assert_eq!(source["source"], "api-proxy");
        assert_eq!(source["daily"].as_array().map(Vec::len), Some(1));

        let point = &source["daily"][0];
        assert_eq!(point["key"], "2026-10-01");
        assert_eq!(point["input"], 150);
        assert_eq!(point["output"], 30);
        assert_eq!(point["cacheRead"], 100);
        assert_eq!(point["cacheWrite"], 5);
        assert_eq!(point["records"], 3);
        // total 不含 cacheRead：150 + 30 + 5。
        assert_eq!(point["total"], 185);
        // cacheRead(100) 超过 input(150) 也不越界，且结果非负。
        assert_eq!(point["uncachedInput"], 50);
        assert_eq!(point["cacheHitRate"], 100.0 / 150.0);

        // summary 与趋势图合计自洽，不受 fixture 里错误的 total 影响。
        assert_eq!(source["summary"]["input"], 150);
        assert_eq!(source["summary"]["records"], 3);
        assert_eq!(source["summary"]["total"], 185);
        assert_eq!(source["summary"]["cacheRead"], 100);

        // 覆盖区间来自日期，按本地时区展开。
        let coverage_start = source["coverageStartAt"].as_i64().expect("coverage start");
        let coverage_end = source["coverageEndAt"].as_i64().expect("coverage end");
        let (day_start, day_end) = local_day_bounds("2026-10-01").expect("local day bounds");
        assert_eq!(coverage_start, day_start);
        assert_eq!(coverage_end, day_end);
    }

    #[test]
    fn proxy_source_models_are_summed_from_daily_by_model_and_sorted() {
        let doc = proxy_usage_doc(&[
            (
                "domestic",
                json!({
                    "model-small": {
                        "2026-10-01": proxy_usage_bucket(10, 1, 0, 0, 1),
                        "2026-10-02": proxy_usage_bucket(10, 1, 0, 0, 1),
                    },
                }),
            ),
            (
                "international",
                json!({
                    "model-large": {
                        "2026-10-02": proxy_usage_bucket(300, 20, 200, 7, 3),
                    },
                }),
            ),
        ]);

        let source = proxy_source_from(&doc, false);
        let models = source["models"].as_array().expect("models");
        assert_eq!(models.len(), 2);
        assert_eq!(models[0]["key"], "model-large");
        assert_eq!(models[0]["total"], 327);
        assert_eq!(models[0]["records"], 3);
        assert_eq!(models[1]["key"], "model-small");
        assert_eq!(models[1]["input"], 20);
        assert_eq!(models[1]["records"], 2);

        // dailyByModel 也按模型给出面板形状的按日序列。
        assert_eq!(
            source["dailyByModel"]["model-small"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(source["dailyByModel"]["model-large"][0]["key"], "2026-10-02");

        // 反代不区分项目，但「用量分布」要有一项，否则整块显示空态。
        assert_eq!(source["projects"][0]["key"], "API 反代");
        assert_eq!(source["projects"][0]["total"], 349);
    }

    #[test]
    fn cache_read_falls_back_to_provider_data_when_message_usage_omits_it() {
        // WorkBuddyAI 国际版 CLI 的真实形态：message.usage 只有 input/output，
        // 命中数在 providerData.usage.inputTokensDetails[].cached_tokens。
        let value = json!({
            "message": { "usage": { "input_tokens": 37091, "output_tokens": 250 } },
            "providerData": {
                "model": "deepseek-v4.1-flash",
                "usage": {
                    "inputTokens": 37091,
                    "outputTokens": 250,
                    "inputTokensDetails": [{ "cached_tokens": 35200 }]
                },
                "rawUsage": {
                    "prompt_tokens": 37091,
                    "prompt_cache_hit_tokens": 35200,
                    "cache_read_input_tokens": 0
                }
            }
        });
        assert_eq!(
            usage(&value),
            Some(Usage {
                input: 37091,
                output: 250,
                read: 35200,
                write: 0,
            })
        );

        // message.usage 自带命中数时以它为准，不被 providerData 覆盖。
        let populated = json!({
            "message": { "usage": {
                "input_tokens": 100,
                "output_tokens": 5,
                "cache_read_input_tokens": 60
            }},
            "providerData": { "usage": {
                "inputTokens": 100,
                "outputTokens": 5,
                "inputTokensDetails": [{ "cached_tokens": 99 }]
            }}
        });
        assert_eq!(usage(&populated).map(|u| u.read), Some(60));
    }

    /// 累计口径：`totalDailyByModel` 不被 `dailyByModel` 的裁剪影响。
    ///
    /// 老文件没有这个快照时退回 `dailyByModel`，否则「总计」永远从升级那天算起。
    #[test]
    fn proxy_cumulative_projection_survives_daily_pruning() {
        let doc = json!({
            "byEdition": {
                "domestic": {
                    "total": {"requests": 3, "errors": 0, "promptTokens": 300,
                              "completionTokens": 0, "credit": 0},
                    "daily": {},
                    // 「今天」只剩一个窗口，之前的日期已被裁掉。
                    "dailyByModel": {
                        "m": {"2026-10-24": proxy_usage_bucket(100, 0, 50, 0, 1)}
                    },
                    // 累计投影仍保留全部三天。
                    "totalDailyByModel": {
                        "m": {
                            "2026-10-22": proxy_usage_bucket(50, 0, 10, 0, 1),
                            "2026-10-23": proxy_usage_bucket(150, 0, 90, 0, 1),
                            "2026-10-24": proxy_usage_bucket(100, 0, 50, 0, 1)
                        }
                    }
                }
            }
        });
        let source = proxy_source_from(&doc, false);
        assert_eq!(source["summary"]["input"], 300);
        assert_eq!(source["summary"]["cacheRead"], 150);
        // 「近 30 天」的实际驱动数据只有今天这个窗口。
        assert_eq!(source["daily"].as_array().map(Vec::len), Some(1));
        assert_eq!(source["trend"]["summary"]["input"], 100);

        // 老文件：没有 totalDailyByModel ⇒ 累计视图退回 dailyByModel。
        let mut legacy = doc.clone();
        legacy["byEdition"]["domestic"]
            .as_object_mut()
            .unwrap()
            .remove("totalDailyByModel");
        let source = proxy_source_from(&legacy, false);
        assert_eq!(source["summary"]["input"], 100);
    }

    #[test]
    fn by_session_buckets_use_hashed_keys() {
        let now = crate::modules::config::now_ms();
        let root = std::env::temp_dir().join(format!(
            "wb-switch-token-stats-sessions-{}-{now}",
            std::process::id()
        ));
        let project = root.join("fixture-project");
        fs::create_dir_all(&project).expect("create fixture dirs");
        let record = |input| {
            json!({
                "timestamp": now,
                "message": { "usage": { "input_tokens": input, "output_tokens": 1 } }
            })
        };
        fs::write(
            project.join("session-a.jsonl"),
            format!("{}\n{}\n", record(10), record(20)),
        )
        .expect("write fixture a");
        fs::write(project.join("session-b.jsonl"), format!("{}\n", record(30)))
            .expect("write fixture b");

        let result = source(root.clone(), "fixture", None);
        let bundle = &result["bySession"];
        assert_eq!(bundle["usage"]["input"], 60);
        assert_eq!(bundle["sessionCount"], 2);
        let sessions = bundle["sessions"].as_array().expect("sessions");
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0]["input"], 30, "按总量降序");
        // key 是掩码（0x + 4 位 hex），不出现原始会话 id。
        for session in sessions {
            let key = session["key"].as_str().expect("key");
            assert!(key.starts_with("0x") && key.len() == 6, "{key}");
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn proxy_source_stays_available_when_document_is_empty() {
        let source = proxy_source_from(&json!({}), false);
        assert_eq!(source["source"], "api-proxy");
        assert_eq!(source["summary"]["total"], 0);
        assert_eq!(source["summary"]["records"], 0);
        assert!(source["coverageStartAt"].is_null());
        assert!(source["coverageEndAt"].is_null());
        for key in ["models", "projects", "sessions", "daily", "hours"] {
            assert_eq!(
                source[key].as_array().map(Vec::len),
                Some(0),
                "{key} should be empty"
            );
        }
        assert_eq!(source["dailyByModel"].as_object().map(Map::len), Some(0));
    }

    #[test]
    fn proxy_source_does_not_double_count_cache_read_in_totals() {
        let doc = proxy_usage_doc(&[(
            "domestic",
            json!({
                "model-a": {
                    // prompt 已含 cacheRead：total 只能算 input + output + cacheWrite。
                    "2026-10-03": proxy_usage_bucket(100, 10, 60, 5, 1),
                },
            }),
        )]);

        let source = proxy_source_from(&doc, false);
        let point = &source["daily"][0];
        assert_eq!(point["input"], 100);
        assert_eq!(point["cacheRead"], 60);
        assert_eq!(point["total"], 115);
        assert_eq!(point["uncachedInput"], 40);
        assert_eq!(source["summary"]["total"], 115);
        assert_eq!(source["models"][0]["total"], 115);

        // 缓存命中量比输入还大时，uncachedInput 饱和为 0，不会出现负数。
        let overshoot = proxy_source_from(
            &proxy_usage_doc(&[(
                "domestic",
                json!({
                    "model-b": { "2026-10-04": proxy_usage_bucket(10, 1, 99, 0, 1) },
                }),
            )]),
            false,
        );
        assert_eq!(overshoot["daily"][0]["uncachedInput"], 0);
        assert_eq!(overshoot["daily"][0]["total"], 11);
    }
}
