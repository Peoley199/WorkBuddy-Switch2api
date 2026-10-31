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

### 2026-10-30 · 上游 `dc4133f`（v1.6.21：模型侧瞬时故障 11133/11134/14003）

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `is_transient_model_error` / `transient_code`（11133/11134/14003 识别） | 移植 | 判定走本仓库的 8 形态 `has_upstream_code`（上游是 2 形态） |
| 主循环退避重试 3 次（1s→3s→8s），耗尽**不冷却账号** | 原样移植 | — |
| 耗尽后的可读错误文案 | 原样移植 | — |
| `has_upstream_code`（2 形态） | **不引入** | 本仓库已有 8 形态超集（含转义与 `bizCode`），且 11140 的 `ban_kind` 依赖它 |
| `is_channel_block` 加 `request illegal` 裸兜底 | **不引入** | 11140 的 msg 与 11128 一字不差；上游靠先查 11140 挡住，但它的 2 形态匹配遇转义体会漏判，被封号会进渠道重试循环。本仓库 11140 在进 `is_channel_block` 前已被 `ban_kind` 分流。两仓库均无「body 无锚定 code 只有 msg」的 11128 实录（观察项） |
| 单测：requestId 撞码 | 本地已有 | `upstream_code_match_is_anchored` 覆盖同场景 |
| 单测：瞬时故障识别 | 原样移植 | — |

### 2026-10-30 · 上游 `22082b0`（v1.6.22：X-Conversation-ID）

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `chat_headers()` 补发 `X-Conversation-ID` | **代码跳过** | 本仓库更早就有，且更完整：值跟随入站会话（会话内稳定）+ 修掉了发送循环 append 双发的 bug（8a7d8e9） |
| 单测：三个会话头必须同时存在 | 移植 | 渠道校验存在性检查的护栏 |
| API-PROXY.md 的 9 天误判复盘 | 内容采纳，按本仓库实际改写 | 本仓库未踩坑，复盘作为「精简头族」护栏记录 |

### 2026-10-30 · 上游 `4ea798a`（v1.6.23：连接层重试 1→3 + 502 可读文案）

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `MAX_TRANSPORT_RETRY` 1→3 + 退避 400ms/1s/2s | 原样移植 | — |
| 502 改可读 JSON（网络层失败 ≠ 账号被封） | 原样移植 | 文案里去掉了上游的「换国内版入口」建议（那是上游双版本语境） |
| ⚠️ 该提交**误删了 3 个既有测试的 `#[test]` 属性**（requestId 撞码、按版本取号、tool_choice 归一） | **不移植** | 上游 main 里这三个测试已失效（属性丢失，编译通过但不运行）；本仓库属性完好，同步时严禁照搬 |

### 2026-10-30 · 上游 `42b573b`（密文凭据文案修正）

`checkin.rs` / `credits.rs` 两处「需重新登录」改为「重新登录也无法解决——客户端会再次写入密文」。cherry-pick 干净落地，无本地修正。

### 2026-10-30 · 上游 `f18286a`（v1.6.26：11140 冷却 3h + Trace ID）

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `upstream_request_id()`（抠 `requestId` 作 Trace ID） | 原样移植 | — |
| 11140 报错附 Trace ID + 申诉邮箱/工单指引 | 移植并入本地文案 | 本地账号级/未知两种 11140 文案都带上 Trace ID |
| 11140 固定冷却 6h→3h | **不取** | 本仓库走 `needs_relogin` 持久摘号（8a7d8e9），无固定冷却时长可改 |

### 2026-10-30 · 上游 `504bdfa` / `2f65a05`（v1.6.25/27：11140 识别与递增冷却）—— 被本地方案取代

上游把**一切** 11140 按账号风控处理（先 6h、后 3h、再改递增 3h→6h→12h→24h，成功出字清零）。
本仓库 8a7d8e9 已实现更强的 `ban_kind()` 三分流（账号级 / 内容审核 / 未知）：

- **不移植递增冷却**：它是为「不持久摘号」设计的兜底；本仓库账号级直接持久摘号（`needs_relogin`），递增序列没有作用对象。
- **证据链对比**：上游 2026-10-29 才发现 11140（5/5 号 unusual activity），且其「等几小时自愈」假设已被自身实测证伪（等 2 天不恢复）；本仓库调研报告引 2026-09-17 起的社区记录确认 11140 是**双语义码**——上游方案会把内容审核误判成封号。
- 上游对 11140/11128 同 msg 冲突的处理（`is_channel_block` 先排除 11140）与本仓库 `ban_kind` 前置分流同思路，本仓库已覆盖。

### 2026-10-30 · 上游 `b928bdd`（版本号 1.6.23→1.6.24）—— 不同步

版本号口径既定：两仓库各自独立，见文首说明。


### 2026-11-01 · 上游 `9aac4e1`（v1.6.28：国内版主动清洗渠道指纹）

背景：国内版自 2026-10-28 起也按**指令文本内容**判 11128（不只是出站头）——
`system` / `developer` / **`assistant` 历史**里出现两词品牌名就整条拒；
同一条消息里出现被空格包围的裸 `@` 且与「@ 是路径前缀」说明同现也拒。
`user` / `tool` 消息不触发。

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| `neutralize_domestic_fingerprints`（system/developer/assistant，字符串 + multipart） | 移植 | 放在 `build_upstream_body` 默认路径（本仓库本来就默认抹身份声明，见 10-24 记录） |
| `neutralize_domestic_tool_descriptions`（`tools[].function.description`） | 移植 | — |
| `neutralize_bare_at`（只把两侧带空格的 ` @ ` 换成全角 `＠`） | 移植 | `@file` / `@scope/pkg` / `a@b.com` / `@"..."` 均不动，有单测 |
| 替换词 `Assistant` → `DeepSeek` | 移植 | 语义零损失；原词会造出「powered by Assistant」病句 |
| 国内版才做全套 | 采纳 | 国际版维持既有行为：**system 身份声明照抹**（本仓库 10-24 起就比上游激进），其余新增项不做 |

### 2026-11-01 · 上游 `ee281bf`（v1.6.29：池策略）—— 部分移植

| 上游内容 | 我们的处理 | 本地修正与理由 |
|---|---|---|
| 并发上限（单号 3 / 国际版 2 / 整池 8 + `LeaseGuard` RAII） | 移植 | 流式响应的守卫搬进泵的任务体；503 文案新增「并发已占满」分支，与「冷却中」区分开 |
| 429/5xx 换号重试（单请求 ≤2 次） | 移植 + 修正 | 只在本请求内排除该号；429 不改动模型级冷却、5xx **不打任何冷却标记**（见下） |
| `session_key()`（首条 system + 首条 user 的哈希） | 移植 | 与入站 `X-Conversation-ID` 互为兜底 —— 不带该头的客户端（CCH 等）也能享受粘滞 |
| `Pool.sticky` 替换现有粘滞 | **不取** | 本仓库的 `SessionStick` 已接入界面冷却快照与解绑逻辑（160881d），功能等价且更完整 |
| 429 指数退避 600s→2h（**账号级**） | **不取** | 本仓库对 429 只冻**模型**（6004 口径，见 10-24 记录）：上游自己都说「可切换其他模型继续使用」，冻整账号会停掉还能用的模型 |
| 5xx 三连熔断 30m→6h | **不取** | 国内版只有一个号，3 次 5xx 就让整池停摆 30 分钟 —— 2026-10-11 的教训正是「5xx 冻号把网络抖动放大成整池故障」（见 `MAX_TRANSPORT_RETRY`）。上游这条与换号重试配套，但我们的池子规模撑不住它的代价 |
| 4 条单测 | 部分移植 | 粘性/并发用本仓库的结构重写；429 退避与 5xx 熔断随实现一并跳过 |

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
- **11140 双语义分流 + 账号级风控摘号**（2026-10-28，提交 8a7d8e9）：`ban_kind()` 按
  displayMsg 区分「账号级限制 / 内容审核 / 未知」；账号级写 `needs_relogin` 持久摘号 +
  5 分钟池内冷却，内容审核不碰账号，未知走 1 小时保守冷却。~~上游 `dc4133f` 把
  `request illegal` 归入「渠道拦截不冷却」——同步该提交时必须让 11140 判定排在它前面~~
  （2026-10-30 同步完成：上游 11140 语义在 v1.6.25/27 里独立演进了另一套方案，
  已按上文 2026-10-30 节的判定处理，无遗留冲突）。
- **`has_upstream_code` 业务码锚定匹配**（2026-10-28，提交 8a7d8e9）：`"code":NNN` /
  `code\":NNN` / `bizCode` 八种形态，避免 `requestId` 里的 hex 子串误命中。
  上游 `dc4133f` 有同思路实现（2 形态）——2026-10-30 同步时保留本仓库超集，未引入上游版本。
- **出站头族对齐官方 5.8.4**（2026-10-28，提交 8a7d8e9）：`X-Product=SaaS` /
  `X-IDE-Type=WorkBuddy` / `X-IDE-Version` / `Accept: application/json` /
  不发 `Origin`·`Referer`·`X-Root-Request-ID` / `X-B3-SpanId` 不再恒零 /
  `X-Conversation-ID` 不再双发。~~上游若也动 `chat_headers()`，两侧逐项对齐后再合并~~
  （2026-10-30 核实：上游唯一的相关改动是 22082b0 补发 `X-Conversation-ID`，
  本仓库实现严格更优，已跳过）。
- **会话粘滞 `SessionStick`**（160881d）：粘滞表按版本挂在进程级，已接入界面冷却
  快照与「号被判失败就解绑」逻辑。上游 v1.6.29 用 `Pool.sticky` 另起一套 ——
  本仓库保留自己的（功能等价且更完整），只采纳上游的 `session_key()` 作为
  「客户端不带 `X-Conversation-ID` 时」的兜底键。
- **429 只冻模型、5xx 不冻号**（v1.6.18 起）：上游 v1.6.29 引入账号级 429 递增退避
  与 5xx 三连熔断，本仓库**不取**（理由见 2026-11-01 节）。同步该提交或后续演化时，
  不要顺手把这两个机制带进来。


## 同步流程（下次照此办理）

> 📘 详细的做法、判断标准与踩坑实录见 [经验-上游同步与本地部署.md](经验-上游同步与本地部署.md)。
> 下面是可以直接照抄的步骤和脚本用法。

0. 本地有未提交改动 → **先落库**（否则同步 diff 会互相污染）
1. `sh scripts/upstream-sync.sh` —— 自动完成：拉取（断网会降级并警告）、逐提交导出差异到
   `/tmp/wb-upstream/`、风险扫描（上游误删 `#[test]`、保护清单文件被改、独有功能锚点被触碰）、
   生成决策表、备份分支 + bundle、开 `sync/upstream-<sha>` 分支并跑基线测试
2. 看 `/tmp/wb-upstream/SUMMARY.md` 与各 `<sha>.diff`，逐个提交判断：
   原样移植 / 移植+修正 / 文档采纳 / 刻意不同步，理由写进本文件
3. 代码改动提交说明带上游 sha（`Upstream: …@<sha>`）
4. `sh scripts/upstream-sync.sh --verify` —— 测试全绿、clippy **增量**零新增、账本已更新
5. 合并：`git merge --no-ff sync/upstream-<sha>`，再 `git merge -s ours upstream/main` 记账
   （只动历史，让 `git log upstream/main --not main` 与 GitHub 的「落后 N 个提交」一起归零）
6. 版本号用 `node scripts/version.mjs set <ver>`，**不要**手写 sed（会漏 npm optionalDependencies）
7. `git push origin main`，用 GitHub API 确认 `behind_by: 0`
8. 需要部署：`sh scripts/deploy-local.sh`（四道防线，探活失败自动回滚）

**冲突时永远人工判断**，不要整文件取某一侧——上游不知道本仓库的独有功能。
