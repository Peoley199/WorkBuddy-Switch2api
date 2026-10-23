//! Token 刷新与保活。
//!
//! 对照 server.py `refresh_account_token` / `ensure_fresh_token` /
//! `run_keepalive_cycle`。

use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;

use crate::modules::account::{build_auth_headers, is_envelope, upsert_account};
use crate::modules::config::{
    http_request, load_checkin_config, norm_ts, now_ms, RunFlagGuard,
};
use crate::modules::edition::edition_of;

static KEEPALIVE_RUNNING: AtomicBool = AtomicBool::new(false);

/// 凭据是否为加密信封（`{$wbEncrypted, envelope}`）。
///
/// 新版客户端把 token 加密存储，switch 手上只有密文 ⇒ **无法刷新、无法用于反代**，
/// 可用范围仅限「切换账号」（密文原样写回 auth 文件，由客户端自行解密）。
fn has_encrypted_credentials(account: &Value) -> bool {
    is_envelope(account, "refresh_token") || is_envelope(account, "access_token")
}

/// 信封账号的自愈 + 标记（内存实现，便于单测）。返回是否清除了误标。
///
/// v1.6.11 时期被误标的 `needs_relogin` 在这里清掉并落盘 —— 不清的话，
/// 升级后红标永远挂在卡片上，用户会以为修复没生效。
fn heal_encrypted_account(acc: &mut Value) -> bool {
    let was_misflagged = acc.get("needs_relogin").and_then(Value::as_bool) == Some(true);
    acc["credentials_encrypted"] = json!(true);
    if was_misflagged {
        acc["needs_relogin"] = json!(false);
        acc["needs_relogin_reason"] = Value::Null;
    }
    was_misflagged
}

/// 刷新单账号 token（POST /v2/plugin/auth/token/refresh），成功则落盘并返回新账号。
///
/// 刷新失败（refresh token 失效等）时，结果通过 `needs_relogin` 字段返回给调用方。
/// 需要**不落盘**的版本（反代出站前的顺手刷新）见 [`refresh_token_only`]。
pub async fn refresh_account_token(account: Value) -> Value {
    let (account, rejected) = refresh_core(account, true).await;
    // 信封账号只打标记、不改状态；其余（成功 / 缺 RT / 被拒 / 缺 accessToken）都落盘。
    if rejected && account.get("credentials_encrypted").and_then(Value::as_bool) != Some(true) {
        // 「被上游拒绝」时 `refresh_core` 不落盘，由这里补上 —— 这是
        // `refresh_account_token` 与 `refresh_token_only` 唯一的区别。
        let _ = upsert_account(&account);
    }
    account
}

/// 同 [`refresh_account_token`]，但**从不落盘** —— 上游拒绝（refresh token 失效等）
/// 时不会把号标成 `needs_relogin`，只把状态写进返回值的 `needs_relogin` /
/// `_wb_refresh_rejected`。
///
/// 反代出站前的「顺手刷新」必须用它：那一步的失败不该把一个号永久打成 `needs_relogin`。
/// 🔴 曾经踩过：落盘版会把 `needs_relogin=true` 立刻写进账号库，于是 `take_account`
/// 里的「刷新失败先用旧 token 试一次」永远触发不了 —— 首次失败之后每个请求都只能 503，
/// 直到用户手动点「刷新」或重新登录。
pub async fn refresh_token_only(account: Value) -> Value {
    let (account, _rejected) = refresh_core(account, false).await;
    account
}

/// 内部字段：`refresh_core` 用来回报「上游拒绝了这个 refresh token」。
const REFRESH_REJECTED_KEY: &str = "_wb_refresh_rejected";

/// 刷新的公共部分。`persist_without_token` 决定「拿不到 refresh token」时是否落盘。
///
/// 返回 `(账号, 是否被上游拒绝)`。被拒绝时**只有** `refresh_account_token` 会落盘
/// （见它自己的说明），`refresh_token_only` 一律不落盘。
async fn refresh_core(mut account: Value, persist_without_token: bool) -> (Value, bool) {
    let previous_access_token = account
        .get("access_token")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let rt = account
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();
    // 🔴 加密信封凭据（`{$wbEncrypted, envelope}`，新版客户端把 token 加密存储）
    // **无法参与刷新**：switch 手上只有密文，打不了 `/auth/token/refresh`。
    //
    // 但它**不是**「缺少 refresh token」—— 若放任流程走到下面那条分支，从本机导入的
    // 加密账号会在第一次自动保活 / 签到后就被标上 `needs_relogin` 并落盘，
    // `eligible_accounts()` 随即跳过它，用户看到的是「刚导进来就坏了」。
    // 这里提前返回、只记录真实原因，不改动 `needs_relogin` 等其它状态。
    // 该类账号的可用范围仅限于「切换账号」（密文原样写回，客户端自行解密）。
    if has_encrypted_credentials(&account) {
        account["credentials_encrypted"] = json!(true);
        return (account, false);
    }

    if rt.is_empty() {
        account["needs_relogin"] = json!(true);
        account["needs_relogin_reason"] = json!("缺少 refresh token，无法刷新，需重新登录");
        if persist_without_token {
            let _ = upsert_account(&account);
        }
        return (account, false);
    }

    let mut headers = build_auth_headers(&account);
    headers.insert("X-Refresh-Token".to_string(), rt.clone());
    // 按账号所属档位选端点：国际版必须打 www.workbuddy.ai，
    // 拿国内域名去刷新国际版 refresh token 会被 401（既有的「国际版刷新失败」）。
    let variant = edition_of(&account);
    let url = format!(
        "{}{}/auth/token/refresh",
        variant.api_endpoint(),
        variant.api_prefix()
    );
    let mut resp = http_request(&url, "POST", Some(json!({})), Some(&headers)).await;

    // 🔴🔴 `code == -1` 是 **HTTP 层的网络失败**（DNS 解析不了 / 超时 / 连接被拒），
    // 不是「refresh token 失效」。`http_request` 对网络错误一律返回 `{"code": -1, ...}`
    // （见 `config.rs` 的 `Err(e) => json!({"code": -1, ...})`）。
    //
    // 之前的写法把两者一视同仁 —— 网络抖一下就给号盖上 `needs_relogin`、**永久踢出反代池**
    // （`eligible_accounts` 会跳过这种号），于是「网络一抖 → 整池罢工」。
    // 2026-10-11 一晚踩了两次：国内版 19:18 / 20:09、国际版 21:53 / 22:05，都是这个原因。
    //
    // 这里先重试一次（网络抖动多为瞬时），仍失败就**原样返回**：
    // 旧的 access token 可能还有效，后续请求会再试；瞬时失败交给反代的失败冷却去兜，
    // 而不是给账号判死刑。
    if resp.get("code").and_then(Value::as_i64) == Some(-1) {
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        resp = http_request(&url, "POST", Some(json!({})), Some(&headers)).await;
        if resp.get("code").and_then(Value::as_i64) == Some(-1) {
            return (account, false);
        }
    }

    let code = resp.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
    if code != 0 && code != 200 {
        // 走到这里说明**上游真的拒绝了**（401 / refresh token 失效 / 参数不对），
        // 这种才是需要重新登录的。
        account[REFRESH_REJECTED_KEY] = json!(true);
        account["needs_relogin"] = json!(true);
        account["needs_relogin_reason"] = json!(format!(
            "刷新失败(code={code}): {}",
            resp.get("message")
                .or_else(|| resp.get("msg"))
                .and_then(|v| v.as_str())
                .unwrap_or("未知错误")
        ));
        return (account, true);
    }

    let data = resp.get("data").cloned().unwrap_or_else(|| json!({}));
    let new_at = data
        .get("accessToken")
        .and_then(|v| v.as_str())
        .or_else(|| data.get("access_token").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let Some(new_at) = new_at else {
        account["needs_relogin"] = json!(true);
        account["needs_relogin_reason"] = json!("刷新响应缺少 accessToken");
        if persist_without_token {
            let _ = upsert_account(&account);
        }
        return (account, false);
    };

    account["access_token"] = json!(new_at);
    if let Some(new_rt) = data
        .get("refreshToken")
        .and_then(|v| v.as_str())
        .or_else(|| data.get("refresh_token").and_then(|v| v.as_str()))
    {
        account["refresh_token"] = json!(new_rt);
    }
    // 官方接口只返回相对 expiresIn（秒），需换算为绝对时间戳
    let new_exp = norm_ts(data.get("expiresAt").or_else(|| data.get("expires_at")));
    let new_exp = match new_exp {
        Some(v) => Some(v),
        None => data
            .get("expiresIn")
            .and_then(|v| v.as_i64())
            .map(|e| now_ms() + e * 1000),
    };
    if let Some(v) = new_exp {
        account["expiresAt"] = json!(v);
    }
    let fallback_rt_exp = norm_ts(
        account
            .get("auth_raw")
            .and_then(|a| a.get("refreshExpiresAt")),
    );
    let mut new_rt_exp = norm_ts(
        data.get("refreshExpiresAt")
            .or_else(|| data.get("refresh_expires_at")),
    );
    if new_rt_exp.is_none() {
        new_rt_exp = fallback_rt_exp;
    }
    let new_rt_exp = match new_rt_exp {
        Some(v) => Some(v),
        None => data
            .get("refreshExpiresIn")
            .and_then(|v| v.as_i64())
            .map(|e| now_ms() + e * 1000),
    };
    if let Some(v) = new_rt_exp {
        account["refreshExpiresAt"] = json!(v);
    }
    account["refreshedAt"] = json!(now_ms());
    let map = account.as_object_mut().unwrap();
    map.remove("needs_relogin");
    map.remove("needs_relogin_reason");
    // 刷新**成功**时总是落盘：反代路径也要把新 token 写回去，否则每次请求都会再刷新一遍。
    let _ = upsert_account(&account);
    // Windows 不执行 apiKeyHelper；当前 CLI 账号刷新后同步 settings env。
    // 同步失败不阻断 WorkBuddy 保活；状态接口会根据 settings 与账号库是否
    // 一致显示“待同步”，避免把认证配置错误混入账号数据。
    if cfg!(windows) {
        let _ = crate::modules::codebuddy_cli::sync_windows_env_for_account(
            &account,
            previous_access_token.as_deref(),
        );
    }
    (account, false)
}

/// 惰性刷新：expiresAt 缺失或剩余 < lazy_refresh_hours 则刷新。返回最新账号。
pub async fn ensure_fresh_token(mut account: Value, cfg: &Value) -> Value {
    let lazy_h = cfg
        .get("lazy_refresh_hours")
        .and_then(|v| v.as_i64())
        .unwrap_or(24);
    let exp = account.get("expiresAt").and_then(|v| v.as_i64());
    let stale = match exp {
        Some(e) => now_ms() >= e || e - now_ms() < lazy_h * 3600 * 1000,
        None => true,
    };
    // 信封凭据统一走 has_encrypted_credentials 判定：行为上等价（信封时
    // has_rt 为 false、不会触发刷新），但语义明确 —— 以后有人改这里时不会
    // 又把信封对象当成「有 refresh token」。
    let has_rt = !has_encrypted_credentials(&account)
        && !account
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty();
    if stale && has_rt {
        account = refresh_account_token(account).await;
    }
    account
}

/// 保活检查：每天由后台循环调用一次，默认（keepalive_days <= 0）无条件刷新
/// 全部带 refresh token 的账号；keepalive_days > 0 时仅刷新剩余不足该天数的账号。
///
/// 高频保活是为了避免官方服务端清理闲置的 refresh 会话——曾出现闲置数天后
/// 刷新返回 12153 invalid_grant（Session doesn't have required client）导致
/// 账号被迫重新登录。
pub async fn run_keepalive_cycle() -> Value {
    let Some(_guard) = RunFlagGuard::try_acquire(&KEEPALIVE_RUNNING) else {
        return json!({"skipped": "already_running"});
    };
    let cfg = load_checkin_config();
    let keep_days = cfg
        .get("keepalive_days")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let accounts = crate::modules::account::load_accounts();
    let total = accounts.len();
    let mut results: Vec<Value> = Vec::new();
    for mut acc in accounts {
        let exp = acc.get("expiresAt").and_then(|v| v.as_i64());
        let stale = keep_days <= 0
            || match exp {
                Some(e) => now_ms() >= e || e - now_ms() < keep_days * 24 * 3600 * 1000,
                None => true,
            };
        if !stale {
            continue;
        }
        // 🔴 加密信封凭据无法保活：switch 手上只有密文，打不了刷新接口。
        // 它**不是**「缺少 refresh token」—— 不在这里拦掉的话，从本机导入的
        // 信封账号会被打上 needs_relogin 并落盘，用户看到的是「刚导进来就坏了」。
        // （v1.6.11 只给 refresh_account_token 加了这道防护，保活这条独立路径漏了。）
        //
        // 自愈：v1.6.11 时期被误标的 needs_relogin 在这里清掉并落盘。
        // 不清的话，升级后红标永远挂在卡片上 —— 用户会以为修复没生效。
        // `credentials_encrypted` 同时落盘，前端据此显示中性徽标而不是红色警告。
        if has_encrypted_credentials(&acc) {
            let healed = heal_encrypted_account(&mut acc);
            let _ = upsert_account(&acc);
            results.push(json!({
                "email": crate::modules::account::account_display_name(&acc),
                "status": "encrypted",
                "healed": healed,
            }));
            continue;
        }
        if acc
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty()
        {
            acc["needs_relogin"] = json!(true);
            acc["needs_relogin_reason"] = json!("缺少 refresh token，无法保活，需重新登录");
            let _ = upsert_account(&acc);
            results.push(json!({
                "email": crate::modules::account::account_display_name(&acc),
                "status": "missing_rt",
            }));
            continue;
        }
        let fresh = refresh_account_token(acc).await;
        let failed = fresh.get("needs_relogin").and_then(|v| v.as_bool()) == Some(true);
        results.push(json!({
            "email": crate::modules::account::account_display_name(&fresh),
            "status": if failed { "failed" } else { "ok" },
            "error": if failed {
                fresh.get("needs_relogin_reason").and_then(|v| v.as_str()).map(|s| s.to_string())
            } else {
                None
            },
        }));
    }
    json!({"checked": total, "refreshed": results})
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 加密信封凭据要能被识别 —— 否则导入的加密账号会被当成「缺少 refresh token」
    /// 直接标 `needs_relogin` 并落盘，表现为「刚导进来就坏了」。
    #[test]
    fn detects_encrypted_credentials() {
        let envelope = json!({"$wbEncrypted": 1, "envelope": "x"});

        assert!(has_encrypted_credentials(&json!({
            "access_token": envelope, "refresh_token": envelope
        })));
        assert!(has_encrypted_credentials(&json!({
            "access_token": "plain", "refresh_token": envelope
        })));
        assert!(has_encrypted_credentials(&json!({
            "access_token": envelope, "refresh_token": "plain"
        })));
    }

    /// 明文、缺失、以及非信封的普通对象都不算加密凭据
    /// （判定口径与 `account::secret_value` 保持一致）。
    #[test]
    fn plain_and_missing_credentials_are_not_encrypted() {
        assert!(!has_encrypted_credentials(&json!({
            "access_token": "a", "refresh_token": "r"
        })));
        assert!(!has_encrypted_credentials(&json!({})));
        assert!(!has_encrypted_credentials(&json!({
            "access_token": {"foo": "bar"}
        })));
    }

    /// 🔴 信封账号被 v1.6.11 误标 needs_relogin 后，自愈必须清掉红标并落
    /// `credentials_encrypted` —— 否则升级后用户看到的还是「需重新登录」，
    /// 会以为修复没生效（群友 issue 里「怎么重新登录都治不好」正是这个）。
    #[test]
    fn heal_encrypted_account_clears_misflagged_relogin() {
        let mut acc = json!({
            "id": "a1",
            "uid": "u1",
            "access_token": {"$wbEncrypted": 1, "envelope": "x"},
            "needs_relogin": true,
            "needs_relogin_reason": "缺少 refresh token，无法保活，需重新登录",
        });

        assert!(heal_encrypted_account(&mut acc));

        assert_eq!(acc["credentials_encrypted"], json!(true));
        assert_eq!(acc["needs_relogin"], json!(false));
        assert!(acc["needs_relogin_reason"].is_null());
    }

    /// 没被误标过的信封账号只落密文标记，不得凭空生出 needs_relogin。
    #[test]
    fn heal_encrypted_account_marks_clean_account_without_side_effects() {
        let mut acc = json!({
            "id": "a2",
            "uid": "u2",
            "access_token": {"$wbEncrypted": 1, "envelope": "x"},
        });

        assert!(!heal_encrypted_account(&mut acc));

        assert_eq!(acc["credentials_encrypted"], json!(true));
        assert!(acc.get("needs_relogin").is_none());
        assert!(acc.get("needs_relogin_reason").is_none());
    }
}
