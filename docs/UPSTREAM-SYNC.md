# 上游同步记录（UPSTREAM-SYNC）

本文件是「从上游同步了什么、改了什么、刻意不同步什么」的账本。
每次同步上游后**必须更新本文件**，并在提交说明里写明上游提交 sha。

- 上游仓库：<https://github.com/cv-superding/WorkBuddy-Switch2api>
- 本仓库：cv-superding 仓库的 fork，独立演进
- 分叉点：`f043352`（上游 v1.6.16，2026-10-17）
- 快速查看未同步提交：`git fetch upstream && git log upstream/main --not main`

> ⚠️ 版本号口径：本仓库与上游的版本号**各自独立**。两边都有"v1.6.20"，
> 但内容不同——本仓库的 v1.6.20 是自己的修复集，上游的 v1.6.20 是另一套。
> 判断同步状态请以上游提交 sha 为准，不要看版本号。

## 已同步

### 2026-10-24 · 上游 `c2cdabd`（上游 v1.6.20：参数透传 + tool_choice + 11128）

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `PASSTHROUGH_PARAMS` 13 个标准参数透传 | 原样移植（改名 `STANDARD_PASSTHROUGH`，与本仓库 `REASONING_PASSTHROUGH` 命名一致） | 补充：`stream_options.include_usage` 强制 true——本地用量统计依赖它，客户端传 false 会把统计弄瞎 |
| `normalize_tool_choice` 对象→字符串 | 移植 + 修正 | 上游在拿不到函数名时兜底成 `"required"`，会悄悄把「必须调 f」变成「必须调某个工具」；本地改为丢弃并记 `tool-choice-dropped` |
| `strip_identity_declarations` + `IDENTITY_DECLARATIONS` | 移植 | 位置不同：上游只在自愈重试时抹；本仓库**默认路径就抹**（字符串与多块 content 两种形态都覆盖）——这些短语是客户端自动注入的固定声明，不是用户内容 |
| `find_reset_datetime`（ASCII 时间串扫描） | 原样移植 | 与本仓库国际版 6004 识别合并：判定关键词保留 ASCII 优先（`60004`/`6004`/`frequency limit`），取时间一律走 ASCII 扫描，不再依赖中文关键词（防 `\uXXXX` 转义失效） |
| 429 + 可解析时间串 → 按频率超限处理 | 原样移植 | — |
| 11128 两种变体分开的可执行错误文案 | 原样移植（文案按本仓库情况微调：默认净化更激进，提示里说明 DEBUG_DUMP 用法） | — |
| 11128 自愈重试（`self_healed` + 重造请求体） | 移植 + 改动 | 自愈默认只做**无损层**（再抹一遍身份声明）；上游的完整归一化（角色收敛/去 name/拍平 content）会丢图片块，改为 `WB_SWITCH_AGGRESSIVE_HEAL=1` 才启用 |
| `sanitize_messages_aggressive`（最大强度归一化） | 移植 | 签名改为对整个 body 操作（本仓库不需要上游的 `build_upstream_body_ex(aggressive)` 双路径）；默认不调用 |
| 11128 重试耗尽不冷却账号 | 原样移植 | 这同时修了本仓库的旧 bug：v1.6.9 的注释写「绝不冷却」，实现却一直在 `mark_failed`，代码与设计相悖 |
| v1.6.19：信封凭据账号排除出反代池 | 无需移植 | 本仓库早已有（`is_envelope` 检查 + `is_leasable_uid`） |
| 版本号 1.6.16→1.6.20（Cargo.toml / package.json / tauri.conf.json / 各平台 npm 包） | 不同步 | 本仓库已独立升到 1.6.20，且 `package.json` 的 build 脚本（签名密码从文件读）与 `tauri.conf.json` 的 updater pubkey 是本仓库自己的，不能覆盖 |

### 2026-10-24 · 上游 `a20ccdc`（docs/notes 排障手记 6 篇）

原样同步（`git checkout upstream/main -- docs/notes/`）。已复核脱敏：无 token / 真实昵称 / 本机路径残留。

### 2026-10-24 · 上游 `cc55c66`（README 补文档入口）

内容采纳（API-PROXY.md 与 docs/notes/ 的入口链接），落在 README 的 Docker 段之后；
同时顺手把 README 的 Releases 徽章 / 下载链接指向本仓库，并新增 Fork 说明。

### 2026-10-24 · 上游 `e9bacf0`（API-PROXY.md 补 11128 / 参数透传文档）

内容采纳，按本仓库实现改写后合入 API-PROXY.md（本仓库默认净化更激进、自愈分两层）。

## 刻意不同步（同步时必须保护）

| 项 | 原因 |
|---|---|
| `package.json` 的 `build:app*` 脚本 | 上游硬编码签名密码 `wb-switch-dev`；本仓库从 `~/.wb-switch/wb-switch-updater.key(.password)` 文件读 |
| `src-tauri/tauri.conf.json` 的 updater `pubkey` | 两仓库签名密钥不同，覆盖后旧客户端全部更新失败 |
| `Cargo.lock` / `package-lock.json` | 依赖树不同 |
| `update.rs` / `update.ts` / `screenshot-demo.ts` 里的仓库地址 | 本仓库 `70afb1c` 已指向 Peoley199 仓库 |

## 本仓库独有（上游没有，merge 时不可被覆盖）

- **Anthropic Messages 兼容层**（`/v1/messages`、`/v1/messages/count_tokens`、`anthropic_to_openai`、`anthropic_sse_pump` 等）—— v1.6.17 引入
- **http_guard 访问校验**（Host/Origin 校验、`WB_SWITCH_ALLOWED_HOSTS`、`/__auth/boot` 口令引导）—— v1.6.20 引入；本次同步又把 `/v1/*` 路由也纳入保护；2026-10-24 起新增 `is_docker_host_name`（`host.docker.internal` / `gateway.docker.internal` 内建放行，容器网关免 421）
- **流式超时保护**（首包 90s / 帧间隔 240s / 建连 10s，环境变量可覆盖）
- **国际版 6004 频率超限识别**（英文文案三重判定）与**模型级长冷却**（`mark_model_failed_until`）
- **API 反代用量接入 Token 统计页**（v1.6.18）
- **`refresh_token_only`**（不落盘的顺手刷新，避免瞬时失败把账号永久标 needs_relogin）
- **`take_account` 排除本轮已失败 uid**（挑号与取明文失败不互相顶掉）
- **11140 双语义分流 + 账号级风控摘号**（2026-10-28）：`ban_kind()` 按 displayMsg 区分
  「账号级限制 / 内容审核 / 未知」；账号级写 `needs_relogin` 持久摘号 + 5 分钟池内冷却，
  内容审核不碰账号，未知走 1 小时保守冷却。上游 `dc4133f` 把 `request illegal` 归入
  「渠道拦截不冷却」——**同步该提交时必须让 11140 判定排在它前面**，否则被封的号会被
  「不冷却」地持续调用。
- **`has_upstream_code` 业务码锚定匹配**（2026-10-28）：`"code":NNN` / `code\":NNN` /
  `bizCode` 八种形态，避免 `requestId` 里的 hex 子串误命中。上游 `dc4133f` 有同思路实现，
  同步时可直接采用上游版本，但要注意本仓库还多了 `bizCode` 键与 11140 的调用点。
- **出站头族对齐官方 5.8.4**（2026-10-28）：`X-Product=SaaS` / `X-IDE-Type=WorkBuddy` /
  `X-IDE-Version` / `Accept: application/json` / 不发 `Origin`·`Referer`·`X-Root-Request-ID` /
  `X-B3-SpanId` 不再恒零 / `X-Conversation-ID` 不再双发。同步时若上游也动 `chat_headers()`，
  **两侧逐项对齐后再合并**，不要整块取一侧。


## 同步流程（下次照此办理）

1. `git fetch upstream && git log upstream/main --not main` 看差异
2. 逐个提交判断：原样移植 / 移植+修正 / 刻意不同步，理由写进本文件
3. 代码改动放 `sync/upstream-<版本>` 分支，提交说明带上游 sha（`Upstream: …@<sha>`）
4. `cargo test -p wb-switch-core` 全绿、`cargo clippy -D warnings` 零警告后合并
5. 冲突时**永远人工判断**，不要整文件取某一侧——上游不知道本仓库的独有功能
