import { invoke } from "@tauri-apps/api/core";
import type {
  AccountMeta,
  AccountRecord,
  AppStatus,
  AutoRotateConfig,
  CodeBuddyCliInstallResult,
  CodeBuddyCliStatus,
  CodeBuddyCliSwitchResult,
  CodeBuddyCnIdeStatus,
  CodeBuddyCnIdeSwitchResult,
  CheckinConfig,
  CheckinLog,
  CheckinResult,
  CreditExpiry,
  CreditStatistics,
  TokenStatistics,
  CopyResult,
  GithubConfig,
  ImportPreviewAccount,
  ImportResult,
  OAuthPollResult,
  OAuthStartResult,
  ProxyConfig,
  ProxyModels,
  ProxyStatus,
  ProxyUsage,
  RotateLog,
  RotateStatus,
  Session,
  SwitchResult,
  TravelConfig,
  TravelStatus,
  UpdateInfo,
  EditionStatus,
  TransferScan,
  TransferExportOptions,
  TransferExportResult,
  TransferPreview,
  TransferImportOptions,
  TransferImportResult,
  CacheMovePlan,
  CacheMoveResult,
  CacheMoveTarget,
  CacheMoveRollbackResult,
  CacheMoveCleanupResult,
  CacheBackupItem,
  CacheVerifyResult,
  UpdateGuardStatus,
  UpdateGuardActionResult,
} from "./types";
import { EDITIONS } from "./types";
import { DEMO_UNAVAILABLE_MESSAGE, demoModeEnabled } from "./demo-mode";
import { screenshotDemoResponse } from "./screenshot-demo";

/**
 * 双通道适配层：
 * - 桌面 App（Tauri）：`invoke` 调用 Rust commands
 * - webui（浏览器）：HTTP fetch 调用本地 workbuddy-switch 服务（127.0.0.1）
 */
/**
 * webui 下 API 与页面**同源**，所以端口要跟着页面走 ——
 * `workbuddy-switch serve --port 12345` 打开的是 12345，前端就得连 12345。
 * 原来这里写死 57890，换端口之后页面能开、数据全报「无法连接」。
 *
 * 只在「http + 本机回环」时才用页面来源：
 * - 桌面 App（Tauri）走 invoke，用不到这个常量
 * - GitHub Pages 上的演示站不是回环地址，保持原样（它有独立的 demo 拦截）
 */
const API_BASE = (() => {
  if (typeof window !== "undefined") {
    const { protocol, hostname, origin } = window.location;
    const host = hostname.toLowerCase();
    // Tauri 宿主的页面源是 http://tauri.localhost —— 那条通道走 invoke，用不到本常量，
    // 这里显式排除，免得万一被用到时指向 WebView 自己。
    const tauriHost =
      protocol === "tauri:" || host === "tauri.localhost" || host.endsWith(".tauri.localhost");
    if (!tauriHost && (protocol === "http:" || protocol === "https:")) {
      // 🔴 浏览器里跑（本机 webui / 容器部署）⇒ **一律同源**。
      //
      // 以前这里限定「必须回环」，于是容器部署时（从别的机器用 LAN IP 访问）
      // API_BASE 会退回写死的 http://127.0.0.1:57890 —— 指的是**访问者自己**的本机，
      // 于是页面能打开、所有接口都报「无法连接服务」。
      // GitHub Pages 的演示站不受影响：它有独立的 demoMode 拦截，压根走不到 httpCall。
      return origin.replace(/\/$/, "");
    }
  }
  return "http://127.0.0.1:57890";
})();

/**
 * 访问口令（容器 / 反向代理部署时用）。
 *
 * 服务端设了 `WB_SWITCH_TOKEN` 后，所有 `/api/*` 都要求带口令。
 * 第一次用带 `?token=…` 的地址打开页面，这里把它收进 localStorage，
 * 并**立刻从地址栏抹掉** —— 免得口令留在浏览器历史、截图、或被分享出去的链接里。
 * 之后每次请求自动带 `Authorization: Bearer`。
 */
const ACCESS_TOKEN = (() => {
  if (typeof window === "undefined") return "";
  let stored = "";
  try {
    stored = window.localStorage.getItem("wb-switch-token") ?? "";
  } catch {
    /* 隐私模式 / 禁用存储：退化成「只认地址栏里那一次」 */
  }
  try {
    const url = new URL(window.location.href);
    const fromUrl = url.searchParams.get("token");
    if (fromUrl) {
      stored = fromUrl;
      try {
        window.localStorage.setItem("wb-switch-token", fromUrl);
      } catch {
        /* ignore */
      }
      url.searchParams.delete("token");
      window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
    }
  } catch {
    /* ignore */
  }
  return stored;
})();

const DEMO_READ_COMMANDS = new Set([
  "get_status", "get_accounts", "get_codebuddy_cli_status", "get_codebuddy_cn_ide_status", "get_checkin_status",
  "get_credit_expiry", "get_credit_statistics", "get_auto_checkin_config",
  "get_token_statistics",
  "get_checkin_logs", "get_auto_rotate_config", "rotate_status", "get_rotate_logs",
  "get_github_config", "check_update", "get_launch_at_login_enabled", "switch_progress",
  "get_travel_status", "get_auto_travel_config",
  // 反代页：演示/Pages 里也要能渲染，否则整页只显示一条「演示模式下不可操作」
  "get_proxy_config", "get_proxy_status", "get_proxy_usage", "get_proxy_models",
  // 缓存迁移页：体检 / 验证 / 备份列表是只读的
  "cache_move_plan", "cache_move_verify", "cache_move_backups",
  // 更新防护页：状态是只读的（开关本身在演示模式下不可用）
  "update_guard_status",
]);

export function isDemoMode(): boolean {
  return demoModeEnabled;
}

export function isWebui(): boolean {
  return typeof window !== "undefined" && !("__TAURI_INTERNALS__" in window);
}

/** Tauri mobile 也注入内部 API；用现有平台 UA 约定把桌面宿主与移动宿主区分开。 */
function isMobilePlatform(): boolean {
  if (typeof navigator === "undefined") return false;
  const ua = navigator.userAgent;
  return (
    /Android|iPhone|iPad|iPod/i.test(ua) ||
    (ua.includes("Macintosh") && navigator.maxTouchPoints > 1)
  );
}

/** 是否为提供桌面专属能力的 Tauri 宿主。 */
export function isDesktop(): boolean {
  return !isWebui() && !isMobilePlatform();
}

type Route = { method: "GET" | "POST"; path: string };

/** Tauri command → HTTP 路由映射（webui 模式）。 */
const ROUTES: Record<string, Route> = {
  get_status: { method: "GET", path: "/api/status" },
  get_accounts: { method: "GET", path: "/api/accounts" },
  get_codebuddy_cli_status: { method: "GET", path: "/api/codebuddy-cli/status" },
  install_codebuddy_cli_helper: { method: "POST", path: "/api/codebuddy-cli/install-helper" },
  switch_codebuddy_cli_account: { method: "POST", path: "/api/codebuddy-cli/switch" },
  get_codebuddy_cn_ide_status: { method: "GET", path: "/api/codebuddy-cn-ide/status" },
  switch_codebuddy_cn_ide_account: { method: "POST", path: "/api/codebuddy-cn-ide/switch" },
  detect_codebuddy_cn_ide_account: { method: "POST", path: "/api/codebuddy-cn-ide/detect" },
  delete_account: { method: "POST", path: "/api/delete" },
  oauth_start: { method: "POST", path: "/api/oauth/start" },
  oauth_status: { method: "POST", path: "/api/oauth/status" },
  import_local: { method: "POST", path: "/api/import-local" },
  get_editions: { method: "GET", path: "/api/editions" },
  export_accounts: { method: "POST", path: "/api/export-accounts" },
  export_accounts_to_path: { method: "POST", path: "/api/export-accounts-to-path" },
  preview_import_accounts: { method: "POST", path: "/api/import/preview" },
  import_accounts: { method: "POST", path: "/api/import" },
  switch_account: { method: "POST", path: "/api/switch" },
  list_sessions: { method: "GET", path: "/api/sessions" },
  copy_sessions: { method: "POST", path: "/api/sessions/copy" },
  get_checkin_status: { method: "GET", path: "/api/checkin/status" },
  get_credit_expiry: { method: "POST", path: "/api/credits" },
  get_credit_statistics: { method: "GET", path: "/api/credits/stats" },
  get_token_statistics: { method: "GET", path: "/api/token-stats" },
  checkin: { method: "POST", path: "/api/checkin" },
  checkin_all: { method: "POST", path: "/api/checkin/all" },
  get_auto_checkin_config: { method: "GET", path: "/api/checkin/config" },
  save_auto_checkin_config: { method: "POST", path: "/api/checkin/config" },
  get_checkin_logs: { method: "GET", path: "/api/checkin/logs" },
  get_travel_status: { method: "GET", path: "/api/travel/status" },
  get_auto_travel_config: { method: "GET", path: "/api/travel/config" },
  save_auto_travel_config: { method: "POST", path: "/api/travel/config" },
  get_auto_rotate_config: { method: "GET", path: "/api/rotate/config" },
  save_auto_rotate_config: { method: "POST", path: "/api/rotate/config" },
  rotate_status: { method: "GET", path: "/api/rotate/status" },
  run_rotate: { method: "POST", path: "/api/rotate/run" },
  get_rotate_logs: { method: "GET", path: "/api/rotate/logs" },
  refresh_account_token: { method: "POST", path: "/api/refresh-token" },
  get_github_config: { method: "GET", path: "/api/update/config" },
  save_github_config: { method: "POST", path: "/api/update/config" },
  check_update: { method: "GET", path: "/api/update/check" },
  switch_progress: { method: "GET", path: "/api/switch/progress" },
  update_guard_status: { method: "GET", path: "/api/update-guard/status" },
  update_guard_set: { method: "POST", path: "/api/update-guard/set" },
  update_guard_clear_cache: { method: "POST", path: "/api/update-guard/clear-cache" },
  update_guard_set_frozen: { method: "POST", path: "/api/update-guard/set-frozen" },
  // API 反代。🔴 以前这几个**一条都没有** —— 浏览器里打开反代页只会看到
  // 「webui 模式暂不支持该操作」。本机用还能靠桌面端，但**容器部署没有桌面端**，
  // 缺了这条就等于常驻反代配不起来。
  get_proxy_config: { method: "GET", path: "/api/proxy/config" },
  save_proxy_config: { method: "POST", path: "/api/proxy/config" },
  get_proxy_status: { method: "GET", path: "/api/proxy/status" },
  get_proxy_usage: { method: "GET", path: "/api/proxy/usage" },
  reset_proxy_usage: { method: "POST", path: "/api/proxy/usage/reset" },
  get_proxy_models: { method: "GET", path: "/api/proxy/models" },
  restart_proxy: { method: "POST", path: "/api/proxy/restart" },
};

function queryString(args?: Record<string, unknown>): string {
  if (!args) return "";
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(args)) {
    if (value === undefined || value === null) continue;
    params.set(key, String(value));
  }
  const text = params.toString();
  return text ? `?${text}` : "";
}

async function httpCall<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const route = ROUTES[cmd];
  if (!route) throw new Error(`webui 模式暂不支持该操作: ${cmd}`);
  let res: Response;
  try {
    const url =
      route.method === "GET"
        ? `${API_BASE}${route.path}${queryString(args)}`
        : `${API_BASE}${route.path}`;
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    // 服务端设了口令就必须带 —— 不带会被 401（见 wb-switch-server 的 auth_guard）。
    if (ACCESS_TOKEN) headers.Authorization = `Bearer ${ACCESS_TOKEN}`;
    res = await fetch(url, {
      method: route.method,
      headers,
      body: route.method === "POST" ? JSON.stringify(args ?? {}) : undefined,
    });
  } catch {
    throw new Error(`无法连接 workbuddy-switch 服务（${API_BASE}），请先运行 \`workbuddy-switch\``);
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    if (res.status === 401) {
      throw new Error(
        data.message || "访问口令不对或缺失：请用带 ?token=… 的地址重新打开一次页面。",
      );
    }
    throw new Error(data.message || data.error || `请求失败 (${res.status})`);
  }
  return data as T;
}

// ---------------------------------------------------------------------------
// 读命令的内存缓存（stale-while-revalidate）
//
// 为什么要有这层：**每个页面在挂载时都会重新向后端要一遍数据**（切页 = 卸载 + 重新挂载），
// 而其中几个读命令本身很贵 —— `get_token_statistics` 要遍历 `~/.workbuddy/projects` 下的
// 会话文件、`get_credit_statistics` 要解析 700+ KB 的快照、`cache_move_plan` 要扫整个缓存
// 目录。于是**每次切页都要重新付一遍这个成本**，表现就是「切一下卡一会儿」。
//
// 策略：
//   - 命中且新鲜（≤ FRESH_MS）  → 直接返回缓存，**完全不打扰后端**；
//   - 命中但已旧（≤ STALE_MS）  → **先返回缓存（页面立刻渲染）**，同时后台重拉一次刷新缓存，
//                                 让下次更快、数据也不至于太旧；
//   - 更旧或没有                → 老实等后端（首次访问，避不开）；
//   - 🔴 任何**写命令**都会清空整个缓存 ⇒ 「改完再看」永远是新的。
const FRESH_MS = 60_000;
const STALE_MS = 5 * 60_000;

/**
 * 只缓存这些「贵、且不常变」的读命令。
 *
 * ⚠️ 白名单要保守：往里加之前先问「它变了会不会让人做错决定」。
 */
const CACHEABLE_READS = new Set([
  "get_accounts",
  "get_auto_checkin_config",
  "get_auto_travel_config",
  "get_auto_rotate_config",
  "get_github_config",
  "get_checkin_logs",
  "get_rotate_logs",
  "get_proxy_config",
  "get_proxy_status",
  "get_credit_statistics",
  "get_token_statistics",
  "list_sessions",
  "transfer_scan",
  "cache_move_plan",
  "cache_move_verify",
  "update_guard_status",
]);

/**
 * 便宜又实时的读命令：**既不缓存、也不清缓存**。
 *
 * 为什么需要这一档：账号页挂载时会逐个账号调 `get_checkin_status` / `get_travel_status`
 * 这类「短平快但每次都要新」的命令。如果把它们当成写命令去清缓存，就会把同一次挂载里
 * 刚缓存的 `get_accounts` 白刷掉 —— 缓存等于没有。
 */
const LIVE_READS = new Set([
  "get_status",
  "get_editions",
  "get_checkin_status",
  "get_travel_status",
  "get_credit_expiry",
  "get_codebuddy_cli_status",
  "get_codebuddy_cn_ide_status",
  "detect_codebuddy_cn_ide_account",
  "get_proxy_models",
  // 🔴 反代用量**必须实时**：它每个请求都在变。放进可缓存名单会让「进页面看到的
  // 还是 60 秒前的旧值」，用户得手点刷新 —— 这正是要修的症状。
  // 放这里 = 既不缓存、也不清缓存（不会把同一次挂载里其它的缓存白刷掉）。
  "get_proxy_usage",
  "oauth_status",
  "check_auth_permission",
  "get_launch_at_login_enabled",
]);

type ReadCacheEntry = { at: number; value: unknown };
const readCache = new Map<string, ReadCacheEntry>();

function readCacheKey(cmd: string, args?: Record<string, unknown>): string {
  return args && Object.keys(args).length > 0 ? `${cmd}::${JSON.stringify(args)}` : cmd;
}

/** 手动让读缓存失效（一般不需要 —— 写命令会自动清）。 */
export function invalidateApiCache(): void {
  readCache.clear();
}

function dispatch<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  return isWebui() ? httpCall<T>(cmd, args) : invoke<T>(cmd, args);
}

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  if (demoModeEnabled) {
    if (cmd === "get_credit_statistics" && args?.refresh === true) {
      throw new Error(DEMO_UNAVAILABLE_MESSAGE);
    }
    if (!DEMO_READ_COMMANDS.has(cmd)) throw new Error(DEMO_UNAVAILABLE_MESSAGE);
    return screenshotDemoResponse(cmd, args) as T;
  }

  // 不在两个名单里的命令一律当成**写操作** → 清空缓存。
  // 🔴 方向只敢选这一边：多清一次只是损失缓存收益，少清一次会让人看到旧数据。
  if (!CACHEABLE_READS.has(cmd)) {
    if (!LIVE_READS.has(cmd)) readCache.clear();
    return dispatch<T>(cmd, args);
  }

  const key = readCacheKey(cmd, args);
  const hit = readCache.get(key);
  const age = hit ? Date.now() - hit.at : Number.POSITIVE_INFINITY;
  // 带 refresh:true 的是用户**显式点刷新**，必须真的去后端问一次 —— 否则「点了没反应」。
  const forceFresh = args?.refresh === true;

  if (!forceFresh && hit && age <= FRESH_MS) return hit.value as T;

  if (!forceFresh && hit && age <= STALE_MS) {
    // 先给页面旧数据（立刻渲染），后台刷新留给下一次。
    void dispatch<T>(cmd, args)
      .then((value) => readCache.set(key, { at: Date.now(), value }))
      .catch(() => {});
    return hit.value as T;
  }

  const value = await dispatch<T>(cmd, args);
  readCache.set(key, { at: Date.now(), value });
  return value;
}

// ---------------------------------------------------------------------------
// 状态 / 账号
// ---------------------------------------------------------------------------

/**
 * 告诉桌面端「界面已经挂载完成」——白屏自愈的握手信号。
 *
 * Rust 侧的看门狗等不到它，就认为 WebView2 没把页面跑起来（状态脏），
 * 会自动重载、必要时用干净的用户数据目录重建窗口。webui 与演示模式没有这条通道，
 * 直接静默跳过；握手本身失败也不该影响界面。
 */
export function uiReady(): void {
  if (demoModeEnabled || isWebui()) return;
  void invoke("ui_ready").catch(() => {});
}

export function getStatus(): Promise<AppStatus> {
  return call("get_status");
}

export function getAccounts(): Promise<{ accounts: AccountMeta[] }> {
  return call("get_accounts");
}

export function getCodebuddyCliStatus(): Promise<CodeBuddyCliStatus> {
  return call("get_codebuddy_cli_status");
}

export function installCodebuddyCliHelper(): Promise<CodeBuddyCliInstallResult> {
  return call("install_codebuddy_cli_helper");
}

export function switchCodebuddyCliAccount(accountId: string): Promise<CodeBuddyCliSwitchResult> {
  if (demoModeEnabled) {
    return new Promise((resolve, reject) => {
      window.setTimeout(() => {
        try {
          resolve(screenshotDemoResponse("switch_codebuddy_cli_account", { accountId }) as CodeBuddyCliSwitchResult);
        } catch (error) {
          reject(error);
        }
      }, 1200);
    });
  }
  return call("switch_codebuddy_cli_account", { accountId });
}

export function getCodebuddyCnIdeStatus(): Promise<CodeBuddyCnIdeStatus> {
  return call("get_codebuddy_cn_ide_status");
}

export function switchCodebuddyCnIdeAccount(
  accountId: string,
  restart = true,
): Promise<CodeBuddyCnIdeSwitchResult> {
  return call("switch_codebuddy_cn_ide_account", { accountId, restart });
}

export function detectCodebuddyCnIdeAccount(): Promise<{
  ok: boolean;
  found: boolean;
  matched?: boolean;
  accountId?: string;
  message?: string;
}> {
  return call("detect_codebuddy_cn_ide_account");
}


export function deleteAccount(accountId: string): Promise<{ ok: boolean }> {
  return call("delete_account", { accountId });
}

/**
 * 发起 OAuth 扫码登录。
 *
 * `edition` 缺省 = 国内版；`"international"` = 国际版。
 * 两档位走不同域名与 `platform` 参数（`workbuddy` / `workbuddy-ai`），
 * 用错档位会拿到不属于该版本的登录态，所以必须显式传。
 */
export function oauthStart(edition?: string): Promise<OAuthStartResult> {
  return call("oauth_start", { edition });
}

export function oauthStatus(loginId: string): Promise<OAuthPollResult> {
  return call("oauth_status", { loginId });
}

/**
 * 导入本机当前账号。
 *
 * `edition` 缺省/`"domestic"` = 从国内版 `workbuddy-desktop.info` 导入；
 * `"international"` = 从国际版 `workbuddy-desktop-ai.info` 导入。
 */
export function importLocal(
  edition?: string,
): Promise<{ ok: boolean; account: AccountMeta; edition?: string }> {
  return call("import_local", { edition });
}

/** GET /api/editions —— 两个版本的客户端状态（是否安装 / 运行 / 已登录）。 */
export function getEditions(): Promise<{ editions: EditionStatus[] }> {
  return call("get_editions");
}

/** 同时尝试两个版本导入，返回成功的那些（某版本未登录就静默跳过）。 */
export async function importLocalAllEditions(): Promise<AccountMeta[]> {
  const results = await Promise.allSettled(
    EDITIONS.map((e) => importLocal(e.value)),
  );
  return results
    .filter(
      (r): r is PromiseFulfilledResult<{ ok: boolean; account: AccountMeta }> =>
        r.status === "fulfilled" && r.value?.account != null,
    )
    .map((r) => r.value.account);
}

export function exportAccounts(accountIds: string[]): Promise<{ ok: boolean; accounts: AccountRecord[] }> {
  return call("export_accounts", { accountIds });
}

/** 桌面端：把完整记录写入用户选择的路径（系统保存对话框产物）。 */
export function exportAccountsToPath(
  accountIds: string[],
  path: string,
): Promise<{ ok: boolean; path: string }> {
  return call("export_accounts_to_path", { accountIds, path });
}

export function previewImportAccounts(
  fileText: string,
): Promise<{ accounts: ImportPreviewAccount[]; total: number }> {
  return call("preview_import_accounts", { fileText });
}

export function importAccounts(fileText: string, indexes: number[]): Promise<ImportResult> {
  return call("import_accounts", { fileText, indexes });
}

export function switchAccount(args: {
  accountId: string;
  restart?: boolean;
  shareSessions?: boolean;
  copySessionIds?: string[];
  /** 目标客户端版本："domestic"（默认）/ "international"。 */
  edition?: string;
}): Promise<SwitchResult> {
  return call("switch_account", args as unknown as Record<string, unknown>);
}

/** 切换进度（webui 轮询用；桌面端走事件，此函数无副作用）。 */
export function switchProgress(): Promise<{ running: boolean; progress: string | null }> {
  return call("switch_progress");
}

/**
 * 列出会话。
 *
 * `edition` 决定读哪个档位的会话库（缺省国内版）。国际版有自己的
 * `~/.workbuddy-ai/workbuddy.db`，不传就会拿国内版的库去查，列表永远为空。
 */
export function listSessions(edition?: string): Promise<{
  sessions: Session[];
  current: string | null;
  edition?: string;
}> {
  return call("list_sessions", { edition });
}

/** 把勾选会话复制到目标账号；档位由后端按目标账号自身的 edition 决定。 */
export function copySessions(
  targetAccountId: string,
  sessionIds: string[],
): Promise<{ sourceUid: string; targetUid: string; copied: CopyResult[] }> {
  return call("copy_sessions", { targetAccountId, sessionIds });
}

/** 打开系统设置授权面板（桌面端专用；webui 模式由服务进程权限决定，无操作）。 */
export function openPermissionSettings(
  target?: "app_management" | "all_files",
): Promise<void> {
  if (demoModeEnabled) return Promise.reject(new Error(DEMO_UNAVAILABLE_MESSAGE));
  if (isWebui()) return Promise.resolve();
  return call("open_permission_settings", { target: target ?? "app_management" });
}

/** 权限自检：桌面端写探针；webui 模式由服务进程权限决定。 */
export function checkAuthPermission(): Promise<{
  ok: boolean;
  message?: string;
  error?: string;
  dir?: string;
  hint?: string;
}> {
  if (demoModeEnabled) return Promise.reject(new Error(DEMO_UNAVAILABLE_MESSAGE));
  if (isWebui()) {
    return Promise.resolve({
      ok: true,
      message: "webui 模式由服务进程（终端启动）的权限决定，无需额外授权",
      hint: "",
    });
  }
  return call("check_auth_permission");
}

/** 在 Finder 中显示当前 App（桌面端专用；webui 无操作）。 */
export function revealAppInFinder(): Promise<void> {
  if (demoModeEnabled) return Promise.reject(new Error(DEMO_UNAVAILABLE_MESSAGE));
  if (isWebui()) return Promise.resolve();
  return call("reveal_app_in_finder");
}

// ---------------------------------------------------------------------------
// 阶段 3：签到 + token 刷新
// ---------------------------------------------------------------------------

export async function getCheckinStatus(accountId: string): Promise<{
  ok: boolean;
  todayCheckedIn: boolean;
  error?: string;
  raw?: unknown;
}> {
  if (demoModeEnabled) {
    return screenshotDemoResponse("get_checkin_status", { accountId }) as {
      ok: boolean;
      todayCheckedIn: boolean;
      error?: string;
      raw?: unknown;
    };
  }
  if (isWebui()) {
    // webui 端为批量接口，按 accountId 过滤
    const all = await httpCall<{
      accounts: {
        accountId: string;
        email: string;
        ok: boolean;
        todayCheckedIn: boolean;
        error?: string;
        raw?: unknown;
      }[];
    }>("get_checkin_status");
    const one = all.accounts.find((a) => a.accountId === accountId);
    return one
      ? { ok: one.ok, todayCheckedIn: one.todayCheckedIn, error: one.error, raw: one.raw }
      : { ok: false, todayCheckedIn: false, error: "未找到账号" };
  }
  return call("get_checkin_status", { accountId });
}

export function getCreditExpiry(accountId: string): Promise<CreditExpiry> {
  return call("get_credit_expiry", { accountId });
}

export function getCreditStatistics(refresh = false): Promise<CreditStatistics> {
  return call("get_credit_statistics", refresh ? { refresh: true } : undefined);
}

export function getTokenStatistics(days?: number): Promise<TokenStatistics> { return call("get_token_statistics", days ? { days } : undefined); }

export function checkin(accountId: string): Promise<CheckinResult> {
  return call("checkin", { accountId });
}

export function checkinAll(): Promise<{
  accounts: { accountId: string; email: string; result: string; error?: string }[];
  status?: string;
  reason?: string;
}> {
  return call("checkin_all");
}

export function getAutoCheckinConfig(): Promise<CheckinConfig> {
  return call("get_auto_checkin_config");
}

export function saveAutoCheckinConfig(config: CheckinConfig): Promise<CheckinConfig> {
  return call("save_auto_checkin_config", {
    config: config as unknown as Record<string, unknown>,
  });
}

export function getCheckinLogs(): Promise<{ logs: CheckinLog[] }> {
  return call("get_checkin_logs");
}

// ------------------------------------------------------------------ API 反代

export function getProxyConfig(): Promise<ProxyConfig> {
  return call("get_proxy_config");
}

export function saveProxyConfig(config: ProxyConfig): Promise<{
  ok: boolean;
  running: boolean;
  /** 逐入口的启动结果：某个入口起不来不应让整体失败。 */
  endpoints: Array<{ edition: string; label: string; ok: boolean; error: string | null }>;
}> {
  return call("save_proxy_config", { config: config as unknown as Record<string, unknown> });
}

export function getProxyStatus(): Promise<ProxyStatus> {
  return call("get_proxy_status");
}

/** 反代用量统计（服务没跑也能读历史）。 */
/**
 * 反代用量统计。
 *
 * 🔴 **必须按版本分开**：国内版入口只统计经 7863 的请求，国际版只统计经 7864 的。
 * 两个页面各传自己的 `edition`（缓存键含参数，两页不会互相串数据）。
 */
export function getProxyUsage(edition?: "domestic" | "international"): Promise<ProxyUsage> {
  return call("get_proxy_usage", { edition });
}

/** 清空反代用量统计。传 `edition` 只清那一版；不传则两版都清。 */
export function resetProxyUsage(edition?: "domestic" | "international"): Promise<{ ok: boolean }> {
  return call("reset_proxy_usage", { edition });
}

/** 「获取模型ID」：拉一次可用模型列表，客户端 model 字段照着填。 */
/**
 * 拉可用模型列表。
 *
 * 不传 `edition` = 国内版和国际版各拉一次、分档返回（两版模型本来就不一样）；
 * 传 `"domestic"` / `"international"` 只拉那一档。
 */
export function getProxyModels(edition?: string): Promise<ProxyModels> {
  return call("get_proxy_models", { edition });
}

/** 设置账号分组："desktop"=桌面端 / "proxy"=反代API / ""=未分组。 */
export function setAccountGroup(
  accountId: string,
  group: string,
): Promise<{ ok: boolean; id: string; group: string }> {
  return call("set_account_group", { accountId, group });
}

export async function getTravelStatus(accountId: string): Promise<TravelStatus> {
  if (demoModeEnabled) {
    return screenshotDemoResponse("get_travel_status", { accountId }) as TravelStatus;
  }
  if (isWebui()) {
    // webui 端为批量接口，按 accountId 过滤
    const all = await httpCall<{
      accounts: { accountId: string; email: string; label: TravelStatus["label"]; rewardCredit: number | null; locationName?: string | null; arriveAt?: number | null }[];
    }>("get_travel_status");
    const one = all.accounts.find((a) => a.accountId === accountId);
    return one
      ? { label: one.label, rewardCredit: one.rewardCredit, locationName: one.locationName ?? null, arriveAt: one.arriveAt ?? null }
      : { label: "untraveled", rewardCredit: null, locationName: null, arriveAt: null };
  }
  return call("get_travel_status", { accountId });
}

export function getAutoTravelConfig(): Promise<TravelConfig> {
  return call("get_auto_travel_config");
}

export function saveAutoTravelConfig(config: TravelConfig): Promise<TravelConfig> {
  return call("save_auto_travel_config", {
    config: config as unknown as Record<string, unknown>,
  });
}

export function getAutoRotateConfig(): Promise<AutoRotateConfig> {
  return call("get_auto_rotate_config");
}

export function saveAutoRotateConfig(config: AutoRotateConfig): Promise<AutoRotateConfig> {
  return call("save_auto_rotate_config", {
    config: config as unknown as Record<string, unknown>,
  });
}

export function getRotateStatus(): Promise<RotateStatus> {
  return call("rotate_status");
}

export function runRotate(): Promise<{ status: string; reason?: string; error?: string; to?: string }> {
  return call("run_rotate");
}

export function getRotateLogs(): Promise<{ logs: RotateLog[] }> {
  return call("get_rotate_logs");
}

export function refreshAccountToken(accountId: string): Promise<AccountMeta> {
  return call("refresh_account_token", { accountId });
}

// ---------------------------------------------------------------------------
// 阶段 4：自动更新
// ---------------------------------------------------------------------------

export function getGithubConfig(): Promise<GithubConfig> {
  return call("get_github_config");
}

export function saveGithubConfig(config: GithubConfig): Promise<GithubConfig> {
  return call("save_github_config", {
    config: config as unknown as Record<string, unknown>,
  });
}

export function checkUpdate(proxy?: string, force?: boolean): Promise<UpdateInfo> {
  return call("check_update", { proxy: proxy?.trim() || null, force: force ?? false });
}

export function relaunchApp(): Promise<void> {
  return call("relaunch_app");
}

// ---------------------------------------------------------------------------
// 开机自启（仅桌面端；webui 不提供同名接口，卡片也不在 webui 渲染）
// ---------------------------------------------------------------------------

/** 查询系统当前的开机自启注册状态（桌面端）。 */
export function getLaunchAtLoginEnabled(): Promise<boolean> {
  if (demoModeEnabled) return call("get_launch_at_login_enabled");
  if (!isDesktop()) return Promise.resolve(false);
  return call("get_launch_at_login_enabled");
}

/** 注册 / 移除系统开机自启，返回回读后的权威状态（桌面端）。 */
export function setLaunchAtLoginEnabled(enabled: boolean): Promise<boolean> {
  if (demoModeEnabled) return Promise.reject(new Error(DEMO_UNAVAILABLE_MESSAGE));
  if (!isDesktop()) return Promise.resolve(false);
  return call("set_launch_at_login_enabled", { enabled });
}

/** 把 Tauri command / HTTP 抛出的错误统一为 Error。 */
export function asError(e: unknown): string {
  if (typeof e === "string") return e;
  if (e instanceof Error) return e.message;
  return JSON.stringify(e ?? "未知错误");
}

// ---------------------------------------------------------------------------
// 跨机器迁移包
// ---------------------------------------------------------------------------

/** 扫描本机可导出的内容（工作区 / 配置 / 各类体积）。 */
export function transferScan(edition?: string): Promise<TransferScan> {
  return call("transfer_scan", { edition });
}

/** 打包到指定路径（桌面端由系统保存对话框给出 path）。 */
export function transferExport(
  output: string,
  options: TransferExportOptions,
  edition?: string,
): Promise<TransferExportResult> {
  return call("transfer_export", { edition, output, options });
}

/** 只读预览一个迁移包：包里有什么、本地缺什么。 */
export function transferPreview(path: string, edition?: string): Promise<TransferPreview> {
  return call("transfer_preview", { edition, path });
}

/** 执行增量导入。 */
export function transferImport(
  path: string,
  options: TransferImportOptions,
  edition?: string,
): Promise<TransferImportResult> {
  return call("transfer_import", { edition, path, options });
}

// ---------------------------------------------------------------------------
// 缓存迁移
// ---------------------------------------------------------------------------

/** 只读体检：目录体积、目标盘空间、是否有进程占着。 */
export function cacheMovePlan(dest?: string): Promise<CacheMovePlan> {
  return call("cache_move_plan", { dest });
}

/** 只读验证：当前联接状态 + 遗留备份。 */
export function cacheMoveVerify(): Promise<CacheVerifyResult> {
  return call("cache_move_verify");
}

/**
 * 执行迁移（重 IO，进度走 `cache-move-progress` 事件）。
 *
 * `targets` 逐目录给目标根目录 —— 国内版与国际版可以分开放，
 * 也可以都填同一个根目录。
 */
export function cacheMoveRun(targets: CacheMoveTarget[]): Promise<CacheMoveResult> {
  return call("cache_move_run", { targets });
}

/** 回滚：删联接 + 把备份改名回来。 */
export function cacheMoveRollback(): Promise<CacheMoveRollbackResult> {
  return call("cache_move_rollback");
}

/** 列出可清理的 `.moved-*` 备份。 */
export function cacheMoveBackups(): Promise<{ backups: CacheBackupItem[] }> {
  return call("cache_move_backups");
}

/**
 * 删除选中的备份目录。
 *
 * `permanent = false`（默认）：整份移入回收站，删错了还能还原，空间要清空回收站才释放。
 * `permanent = true`：不经过回收站，直接永久删除。
 */
export function cacheMoveCleanup(
  paths: string[],
  permanent = false,
): Promise<CacheMoveCleanupResult> {
  return call("cache_move_cleanup", { paths, permanent });
}

/**
 * 在文件管理器里打开一个目录。
 *
 * 迁移之后应用看到的仍是原家目录路径（联接对程序透明），
 * 这个口子用来把人直接带到数据真正所在的目标盘。
 */
export function cacheMoveOpen(path: string): Promise<void> {
  return call("cache_move_open", { path });
}

// ---------------------------------------------------------------------------
// 更新防护
// ---------------------------------------------------------------------------

/**
 * 只读体检：开关状态 + 更新缓存内容 + 两个安装目录的身份。
 *
 * 要读注册表、扫安装目录，可能几百毫秒。
 */
export function updateGuardStatus(): Promise<UpdateGuardStatus> {
  return call("update_guard_status");
}

/**
 * 开/关「禁止 WorkBuddy 自动更新」。
 *
 * 开的时候会把更新源指到黑洞（用户级 `WORKBUDDY_UPDATE_URL`），并默认顺带
 * 隔离缓存里已下载的包 —— 只设环境变量拦不住已经下好的包。
 */
export function updateGuardSet(
  disabled: boolean,
  clearCache = true,
): Promise<UpdateGuardActionResult> {
  return call("update_guard_set", { disabled, clearCache });
}

/** 只隔离缓存里已下载的包，不动开关。 */
export function updateGuardClearCache(): Promise<UpdateGuardActionResult> {
  return call("update_guard_clear_cache");
}

/**
 * 单独冻结/解冻更新暂存目录（不碰环境变量和配置层）。
 *
 * 冻结 = 对暂存目录显式拒绝写入。WorkBuddy 判断「有没有下好的更新」
 * 就是看缓存目录里有没有 `WorkBuddy-Setup-<版本>.exe`，所以只要写不进去，
 * 这条路就死了 —— 而且是**与版本无关**的一层，重装/升级都抹不掉。
 */
export function updateGuardSetFrozen(freeze: boolean): Promise<UpdateGuardActionResult> {
  return call("update_guard_set_frozen", { freeze });
}
