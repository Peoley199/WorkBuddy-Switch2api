//! workbuddy-switch CLI：npm 安装形态的入口。
//!
//! ```bash
//! workbuddy-switch              # 启动本地服务 + 打开浏览器 webui
//! workbuddy-switch serve        # 只起服务不开浏览器（--port / --no-open）
//! workbuddy-switch status       # 终端输出当前账号
//! workbuddy-switch version      # 版本号
//! ```
//!
//! Docker / 反代形态（监听地址改非回环时必须给口令）：
//!
//! ```bash
//! WB_SWITCH_HOST=0.0.0.0 WB_SWITCH_PORT=57890 WB_SWITCH_TOKEN=xxx \
//!   WB_SWITCH_HOME_OVERRIDE=/data workbuddy-switch serve --no-open
//! ```

mod api;

use serde_json::json;

use wb_switch_core::modules::{
    account, auth_file, checkin, config, process, proxy, rotate, travel, update,
};

fn default_port() -> u16 {
    57890
}

/// 判断监听地址是不是回环（回环 = 只本机能访问，可以不校验口令）。
fn is_loopback(host: &str) -> bool {
    matches!(host.trim(), "127.0.0.1" | "::1" | "localhost")
}

/// 从 `--host` / `WB_SWITCH_HOST` 取监听地址，默认 `127.0.0.1`（**默认不变**）。
fn resolve_host(args: &[String]) -> String {
    if let Some(i) = args.iter().position(|a| a == "--host") {
        if let Some(h) = args.get(i + 1).filter(|h| !h.trim().is_empty()) {
            return h.trim().to_string();
        }
    }
    std::env::var("WB_SWITCH_HOST")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

/// 从 `--port` / `WB_SWITCH_PORT` 取端口，默认 57890。
fn resolve_port(args: &[String]) -> u16 {
    if let Some(i) = args.iter().position(|a| a == "--port") {
        if let Some(p) = args.get(i + 1).and_then(|p| p.parse::<u16>().ok()) {
            return p;
        }
    }
    std::env::var("WB_SWITCH_PORT")
        .ok()
        .and_then(|p| p.trim().parse::<u16>().ok())
        .unwrap_or_else(default_port)
}

/// 从 `--token` / `WB_SWITCH_TOKEN` 取访问口令。空字符串 = 没设。
fn resolve_token(args: &[String]) -> Option<String> {
    let from_arg = args
        .iter()
        .position(|a| a == "--token")
        .and_then(|i| args.get(i + 1))
        .map(|t| t.trim().to_string());
    from_arg
        .or_else(|| {
            std::env::var("WB_SWITCH_TOKEN")
                .ok()
                .map(|t| t.trim().to_string())
        })
        .filter(|t| !t.is_empty())
}

/// 🔴 安全闸：**监听非回环地址就必须设口令**。
///
/// 这个服务能读写账号库、能切号、能签到 —— 裸奔在网上等于把账号交出去。
/// 与其等用户出事，不如启动时就拒绝，并把怎么解决写在报错里。
/// 确实想裸奔（例如前面已经有自己的鉴权反代）时，显式设 `WB_SWITCH_INSECURE=1`。
fn guard_exposure(host: &str, token: &Option<String>) {
    if is_loopback(host) || token.is_some() {
        return;
    }
    if std::env::var("WB_SWITCH_INSECURE").ok().as_deref() == Some("1") {
        eprintln!("⚠️  WB_SWITCH_INSECURE=1：监听 {host} 且不校验口令，任何能连上的人都能操作你的账号。");
        return;
    }
    eprintln!("拒绝启动：监听地址 {host} 不是回环，但没有设置访问口令。");
    eprintln!("这个服务能读写账号库、切号、签到，暴露在网上等于把账号交出去。");
    eprintln!();
    eprintln!("请二选一：");
    eprintln!("  1) 设口令：  WB_SWITCH_TOKEN=<自己定一个长随机串>  （或用 --token）");
    eprintln!("  2) 只本机用：WB_SWITCH_HOST=127.0.0.1");
    eprintln!();
    eprintln!("（确实不需要口令时，显式设 WB_SWITCH_INSECURE=1 跳过这道检查。）");
    std::process::exit(2);
}

/// 后台任务：自动签到启动即核验、每 30 分钟补签；自动轮换按配置间隔执行。
fn spawn_background_loops() {
    tokio::spawn(async move {
        if let Err(error) = config::compact_checkin_logs() {
            eprintln!("[签到] 历史日志整理失败: {error}");
        }
        let _ = checkin::run_checkin_cycle(checkin::CheckinCycleMode::StartupVerify).await;
        loop {
            tokio::time::sleep(checkin::CHECKIN_RECOVERY_INTERVAL).await;
            let _ = checkin::run_checkin_cycle(checkin::CheckinCycleMode::PeriodicRecovery).await;
        }
    });

    tokio::spawn(async move {
        let mut last_cycle_at: i64 = 0;
        loop {
            let cfg = config::load_auto_rotate_config();
            if cfg.get("enabled").and_then(|v| v.as_bool()) == Some(true) {
                let interval_minutes = cfg
                    .get("check_interval_minutes")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(5)
                    .max(1);
                let now = config::now_ms();
                if now - last_cycle_at >= interval_minutes * 60_000 {
                    last_cycle_at = now;
                    let _ = rotate::run_rotate_cycle().await;
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    // 派猫猫旅行：启动即派发，之后周期性补派（并重试 no-buddy / 瞬时错误）。
    tokio::spawn(async move {
        let _ = travel::run_travel_cycle().await;
        loop {
            tokio::time::sleep(travel::TRAVEL_RETRY_INTERVAL).await;
            let _ = travel::run_travel_cycle().await;
        }
    });

    // 旅行领取：启动立刻查一轮（避免重启后空等 15 分钟漏领），之后按周期检查。
    tokio::spawn(async move {
        let _ = travel::run_travel_claim_cycle().await;
        loop {
            tokio::time::sleep(travel::TRAVEL_CLAIM_INTERVAL).await;
            let _ = travel::run_travel_claim_cycle().await;
        }
    });
}

fn print_status() {
    let auth = auth_file::read_auth_file();
    let current = auth.as_ref().and_then(|a| {
        let acct = a.get("account").cloned().unwrap_or_else(|| json!({}));
        Some(json!({
            "uid": acct.get("uid"),
            "nickname": acct.get("nickname"),
            "email": acct.get("email"),
        }))
    });
    let running = process::is_workbuddy_running();
    println!("workbuddy-switch v{}", update::APP_VERSION);
    println!("WorkBuddy 运行中: {}", if running { "是" } else { "否" });
    match current {
        Some(c) => {
            let name = c
                .get("nickname")
                .and_then(|v| v.as_str())
                .or_else(|| c.get("email").and_then(|v| v.as_str()))
                .unwrap_or("未知");
            println!("当前账号: {name}");
        }
        None => println!("当前账号: 未登录"),
    }
    println!("账号数: {}", account::load_accounts().len());
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("serve");
    match cmd {
        "status" => print_status(),
        "version" | "--version" | "-V" => {
            println!("workbuddy-switch {}", env!("CARGO_PKG_VERSION"));
        }
        "serve" | _ => serve(&args).await,
    }
}

async fn serve(args: &[String]) {
    let host = resolve_host(args);
    let port = resolve_port(args);
    let token = resolve_token(args);
    guard_exposure(&host, &token);
    api::set_access_token(token.clone());

    let app = api::router();
    let addr = format!("{host}:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("启动失败: 无法监听 {addr}（{e}）。可用 --host / --port 指定其他地址。");
            std::process::exit(1);
        }
    };

    println!("workbuddy-switch v{}", update::APP_VERSION);
    // 带口令时把口令拼进提示里 —— 首次打开要的就是这个带 `?token=` 的地址。
    let shown_host = if host == "0.0.0.0" { "127.0.0.1" } else { &host };
    match &token {
        Some(t) => println!("webui: http://{shown_host}:{port}/?token={t}"),
        None => println!("webui: http://{shown_host}:{port}"),
    }
    println!("按 Ctrl+C 停止服务。");

    let no_open = args.iter().any(|a| a == "--no-open");
    if !no_open {
        open_browser(&format!("{shown_host}:{port}"));
    }

    spawn_background_loops();

    // 🔴 反代入口（7863 / 7864）也要在这里起 —— 桌面端是在 src-tauri/lib.rs 里起的，
    // CLI/容器形态不补这一句，Docker 版就只剩 webui、没有「常驻反代」这个核心用途。
    // `spawn_from_config` 自己会读 proxy.json，只起 enabled 的入口，起不来只打日志不 panic。
    proxy::spawn_from_config();

    axum::serve(listener, app).await.unwrap();
}

fn open_browser(addr: &str) {
    let url = format!("http://{addr}");
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(&url).spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let mut c = std::process::Command::new("cmd");
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::process::CommandExt;
            c.creation_flags(0x0800_0000); // CREATE_NO_WINDOW：开浏览器不闪 cmd 窗
        }
        let _ = c.args(["/C", "start", &url]).spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
    }
}
