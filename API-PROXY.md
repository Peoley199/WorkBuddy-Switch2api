# API 反代（把 Switch 变成 OpenAI / Anthropic 兼容网关）

在 WorkBuddy Switch 里内置了一个本机 HTTP 服务，把你在 Switch 里管理的账号
包装成标准的 OpenAI 接口，同时提供 Anthropic Messages 接口（Claude Code 可直接连）。

## 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | OpenAI 对话补全，支持 `stream: true/false` |
| POST | `/v1/messages` | Anthropic Messages API，支持流式、`tool_use` / `tool_result` |
| POST | `/v1/messages/count_tokens` | Claude Code 的 token 估算（本地估算，不消耗额度） |
| GET | `/v1/models` | 可用模型列表 |
| GET | `/healthz` | 健康检查（只回计数，不含账号信息） |
| GET | `/status` | 参与账号与运行状态（只接受本机 / 白名单来源，见下） |
| GET | `/usage-stats` | 本入口用量统计（同上，只接受本机 / 白名单来源） |

默认监听 `127.0.0.1:7863`，所以在客户端里填：

```
# OpenAI 客户端
base_url = http://127.0.0.1:7863/v1
api_key  = （没设置就随便填，例如 sk-anything）

# Claude Code / Anthropic 客户端
ANTHROPIC_BASE_URL = http://127.0.0.1:7863
ANTHROPIC_AUTH_TOKEN = <api_key>          # 也接受 x-api-key
```

> ⚠️ **如果你开了梯子（系统代理 127.0.0.1:7897）**：部分 HTTP 客户端会把
> `127.0.0.1` 也送去走代理，导致连不上。请求失败时先设：
> ```
> no_proxy=127.0.0.1,localhost
> ```
> 或者在客户端里显式关掉代理。用 curl 验证时同理：`curl --noproxy '*' http://127.0.0.1:7863/healthz`

## 怎么用

侧边栏有独立的 **「API 反代」** 页（在「设置」上面）：

1. 打开 Switch → **API 反代**
2. 打开「启用 OpenAI 兼容接口」
3. 点**保存**（保存即生效，不用重启软件）
4. 状态行会显示 `运行中 · OpenAI 基址 http://127.0.0.1:7863/v1` 与
   `Anthropic 基址 http://127.0.0.1:7863`

之后每次启动 Switch 都会自动拉起（配置存在 `~/.wb-switch/proxy.json`）。

## 取模型 ID（「获取模型ID」按钮）

「API 反代」页底部状态行右边有个 **「获取模型ID」** 按钮（就在「保存」左边）。
点一下会拉一次模型列表并弹窗展示，每行一个 ID、可单独复制，也能「复制全部」——
客户端里的 `model` 字段照着填就行，不用去翻文档猜 ID。

实现上分两步（`crates/wb-switch-core/src/modules/proxy.rs` 的 `fetch_models`）：

1. **反代开着** → 直接 GET 本机 `http://127.0.0.1:7863/v1/models`（有 api_key 会带上）。
   同一份数据、不额外占号、不消耗积分。
2. **反代没开** → 从账号库里挑一个可用账号（必要时先刷新 token）直连上游
   `/v2/enterprises/personal/models`，把 `data.agents[].models[]` 拆出来。

弹窗底部会写清数据来源（`本机反代` / `上游`）。拉不到时退回一组内置 ID，不会给空列表。
列表外的 ID 也能直接发（上游支持即可）；想让它出现在 `/v1/models` 里、并跳过版本防火墙，
就填进本页的「额外放行模型」。

> ⚠️ 这个列表只来自 `GET /v2/enterprises/personal/models`，**不是可用模型全集**：
> 上游的 `GET /v3/config`（CLI User-Agent）里还有 `deepseek-v4.1-flash` 等一批能调通、却没出现在这里的模型。
> 详见文末「已知限制 → 模型目录只取了一个来源」。

## 国内版 / 国际版是两套上游（v1.0.6）

**两版的模型列表不一样**，所以反代侧做了三件事：

1. **账号按版本分开**：反代页把「反代API」这一组拆成「反代API · 国内版」和「反代API · 国际版」。其它组不拆，只在每行标一个版本徽标。
2. **模型列表分档拉**：「获取模型ID」两档各问一次上游，弹窗分两栏列，每档带自己的来源 URL 与「复制」。某一档没拉到时直接显示原因（比如没有该档账号）。
3. **请求按 model 自动挑版本**：拉过列表之后，反代记住了「这个 model 属于哪一档」，`/v1/chat/completions` 会**只在该版本的号里选**，不再拿国际版专属的模型去打国内版网关。

本机 `/v1/models` 返回**两档并集**，`owned_by` 标出来源：

| owned_by | 含义 |
|---|---|
| `workbuddy` | 两档都有 |
| `workbuddy-cn` | 只有国内版有 |
| `workbuddy-intl` | 只有国际版有 |

> 还没拉过模型列表时（冷启动），路由表是空的，行为退回原来的轮转 ——点一次「获取模型ID」即可填上。所以在客户端里报过「模型不存在」的话，先点一次那个按钮。

数据来源：<img src="docs/images/api-proxy-editions.png" alt="反代页：账号按版本分组" />

## 账号分组

账号管理页里，每张账号卡片右上角的 **⋯ 菜单 → 分组**，可以把它设为：

- **桌面端** —— 日常在 WorkBuddy 里聊天的号
- **反代API** —— 专门给 API 出站的号
- **未分组** —— 没归类

分组存在 `accounts.json` 每条账号记录的 `group` 字段里，随导出/导入一起走。

在「API 反代」页关掉「使用全部账号」后，账号列表会**按分组分区展示**，
并且有一个 **「只选反代API 分组」** 的快捷按钮 —— 一键把标了反代组的号全部选进轮转池。

推荐用法：给桌面端常用的 1~2 个号标「桌面端」，其余标「反代API」，
然后反代页里选「只选反代API 分组」。两边互不抢号。

## 用量统计

「API 反代」页下半部分有**反代用量**，统计经过本接口的每一次请求：

- 总请求 / 输入 tokens / 输出 tokens / 累计消耗（上游返回的 `credit`）
- 按账号拆分、按模型拆分
- 最近 8 条记录（时间、账号、模型、是否流式、token 数）
- 右上角可刷新 / 清空

数据存在 `~/.wb-switch/proxy-usage.json`，**服务没在运行时也能读历史**。
也可以直接查接口：`GET http://127.0.0.1:7863/usage-stats`。

实现上：非流式从聚合后的 `usage` 字段取；流式用一个 `UsageTap` 边转发边累积 SSE 原文，
流结束时解析最后一个 `usage` 块再落盘 —— 所以流式请求的消耗同样会被记到。

`proxy-usage.json` 每版有两张按天表：`dailyByModel`（页面「今天 / 7 / 30 天」用）与
`totalDailyByModel`（累计投影，「总计」用，与 `total` 逐笔同写、不裁剪）。

### 在「Token 统计」页查看

侧边栏「Token 统计」页现在多了一个 **「反代 API」** 来源 tab，与 WorkBuddy / WorkBuddyAI /
CodeBuddy CLI / CodeBuddy IDE 并列，可查看总览、每日趋势、活跃热力图和模型 Top 10。
反代还没有任何用量时 tab 处于禁用状态。数据同样来自 `~/.wb-switch/proxy-usage.json`，与「API 反代」页共用已有的 `get_statistics`，不需要额外接口。

口径说明：

- **国内版与国际版合并**成一个来源。这一点与「API 反代」页不同 —— 那边按版本分开统计，
  统计页里两版的数字加在一起，不再区分入口。
- 「总计」来自**累计投影**（各版的 `totalDailyByModel`，与 `total` 逐笔同写、永不裁剪）：
  它就是全部按天历史之和，升级前的历史在首次读取时用当时的 `dailyByModel` 快照补上。
  趋势 / 热力图 / 「今天 / 7 天 / 30 天」筛选用 `dailyByModel`，按日期自行筛选。
  其它来源会按所选范围（7 / 30 / 90 天）在服务端裁剪；反代来源不做裁剪。
- 缓存读写（`cacheReadTokens` / `cacheWriteTokens`）只记录**上游真实返回**的数值，
  不做任何换算或回填推断；命中率 = `cacheReadTokens / promptTokens`（上游的 `prompt_tokens` 已含命中部分）。
- `bySession`：最近 100 条请求按会话（只存 16 位掩码，原始会话 id 不落盘）拆分，
  用来对照「同一会话换号 / 粘号」时的命中率；没带会话标识的请求（纯轮转）不计入。
- **会话排行为空是预期行为** —— 反代没有会话概念，项目维度只有固定的「API 反代」一项。

### 缓存命中率与「会话粘滞」

上游的 prompt 缓存**按账号隔离**：同一个对话如果轮转分散到多个号上，每个号都要各自重建一遍
上下文（实测：5 个号轮转时，每次上下文变化会连续出现 5 次低命中请求）。

反代因此在 `Pool` 上加了**会话粘滞**：请求带 `X-Conversation-ID` 时（官方桌面端必发，
值 = sessionId），同一会话的连续请求固定到第一次选中的号；该号冷却 / 需重新登录 / 该模型被限流时
自动解绑、重新轮转。粘滞有效期 10 分钟（上游缓存闲置几分钟就失效，再绑定没意义）。

- 没有 `X-Conversation-ID` 的请求**行为不变**（纯轮转）——不拿请求内容猜会话。
- 该头**原样转发**给上游（不吞、不改、不新增）。
- 这只改变「请求打到哪个号」，不改变计费口径；命中率高低如实记录在 `proxy-usage.json`。

## 配置

`~/.wb-switch/proxy.json` —— 国内版与国际版**各一个入口**，开关 / 监听地址 / key / 账号名单各自独立：

```json
{
  "domestic": {
    "enabled": true,
    "listen": "127.0.0.1:7863",
    "api_key": "",
    "accounts": [],
    "extra_models": []
  },
  "international": {
    "enabled": false,
    "listen": "127.0.0.1:7864",
    "api_key": "",
    "accounts": [],
    "extra_models": []
  }
}
```

- `listen` —— 想让局域网其它机器访问才改成 `0.0.0.0:7863`（**务必同时设 api_key**）
- `api_key` —— 留空 = 不鉴权；设了之后客户端必须带 `Authorization: Bearer <key>`。**两个入口各一把**
- `accounts` —— 参与轮转的 uid；**留空 = 该版本的全部账号**。两个入口各存一份
- `extra_models` —— 🔴 **额外放行**的模型（**上游列表里没有、但本入口实测可用的**）。
  会并入本入口的 `GET /v1/models`，并**跳过版本校验**。典型例子：`deepseek-v4.1-flash`
  在国际版上游能正常返回，却没出现在国际版列表里（原因见「已知限制」）。
  界面上每张供应商卡片都有这一栏，逗号分隔。

> v1 的扁平格式（顶层 `enabled` / `listen` / `api_key` / `accounts`）会被自动迁移到
> **国内版**入口，国际版保持关闭；迁移结果就地落盘，不会每次重迁。

## 设计要点（和别的实现的关键差别）

**它没有「两个进程各自刷新同一个 refresh token」的问题。**

常见的做法是单独跑一个网关进程，自己维护一份账号文件、自己刷新 token。
如果 Switch 同时也开着，两边调的是同一个刷新端点、且都会轮换 refresh token，
一旦撞车就会有一边拿到失效的 token。

这里的实现直接复用 Switch 自己的账号库（`~/.wb-switch/accounts.json`）和
它自己的刷新函数（`refresh::refresh_account_token`），**同一个进程、同一份状态**，
所以不存在跨进程竞争。

其它行为：

- 出站强制 `stream: true`（与上游一致），客户端要非流式时在本地聚合成一条
- 账号选择：健康账号轮转；失败的号冷却 120 秒后自动回到池子
- `needs_relogin` 的账号不参与
- 请求头对齐桌面端（含 `X-Conversation-*`、`X-B3-*` 等追踪头）

### 冷却状态

失败分两种，界面上**分开显示**（「API 反代」页的「冷却中」面板与账号行徽标）：

| 类型 | 触发 | 时长 | 影响范围 |
|---|---|---|---|
| 账号短冷却 | 上游 4xx（鉴权 / 额度）、Token 刷新失败或超时 | 固定 120 秒 | 整个账号 |
| 模型长冷却 | 上游「使用频率超限」，且响应里带重置时刻（国内版 `60004` / 国际版 `6004` / 任意 `429` + 时间串） | 到上游声明的时刻 | **仅该账号的该模型**，其他模型照常可用 |

> `11128` 渠道校验**不再冷却账号**：它拦的是这次请求，不是账号（同号的只读接口照样 200）。
> 冻号只会把本来能用的号从池子里摘掉 —— 池子小的时候一次 11128 就能让整池空转。

界面上的倒计时由本地计算（后端给的是绝对恢复时刻），不需要轮询续命；页面每 10 秒
向本进程问一次「有没有新的号进入/退出冷却」，冷却结束会自动从列表里消失。

> ⚠️ 冷却状态只存在**内存**里：重启本程序、或在反代页点一次「保存」（会重建账号池），
> 都会让所有冷却立即清零。这不是 bug —— 冷却是对「上游刚刚拒绝了」的短期记忆。

## 建议：给桌面端留 1~2 个账号

虽然是同一进程，但同一个账号**同时**被桌面端聊天和 API 调用时，上游可能限流、
积分消耗也会加快。所以在设置里关掉「使用全部账号」，把常用的 1~2 个号从反代里摘出去，
体验最好。

## 内容块降级（客户端兼容）

不是所有客户端都只发 `{"type":"text"}`。zcode / Cursor 这类 Agent 会发 OpenAI 较新的
`{"type":"file", ...}`、Responses 的 `input_file`、Anthropic 的 `document`，
而上游网关只认 text / image_url —— 原样透传就是一条 400：

```
{"code":11101,"msg":"Parse message failed: unsupported content type at index 0: file", ...}
```

反代现在会先把 `messages` 里的内容块过一遍：

| 内容块 | 处理 |
|---|---|
| `text` / `image_url` | 原样保留 |
| `file` / `input_file` / `document` / `image_file` / `input_audio` | 文本类附件（data URL 且 mime 是 text/*、json、xml…）解码成文本内联，上限 64 KB、超长截断；二进制（PDF/图片/音频）换成一行占位说明 `[file 附件 xxx 已省略：…]` |
| 其它未知类型 | 换成 `[xxx 内容块已省略：上游不支持该类型]` |

注意降级 ≠ 内容真的进了上下文：二进制附件的内容会被丢掉，模型只看到"这里本来有个附件"。
被降级时响应会带一个头便于排查：

```
x-wb-switch-degraded: file=1
```

好处是客户端不再因为一个附件块被整条打回 —— 长会话的自动 compact 最容易踩到，
因为它会把历史里所有附件块原样重发一遍。

## 上游渠道校验 11128（system prompt 净化）

2026-10-09 起上游多了一道**渠道校验**：它会识别 system prompt 里模仿 Claude Code 的
**环境上下文注入块**（`gitStatus:` / `Current branch:` / `Main branch:` / `Git user:` /
`Recent commits:` …），命中就把整条请求判成「非官方客户端」：

```
{"code":11128,"msg":"Illegal API invocation from an unapproved channel",
 "displayMsg":{"zh":"请求被拦截，请重新发送"},
 "displayTips":{"zh":"这次调用没通过渠道校验，可能不是官方客户端发出的。"}}
```

这个报错很容易被误判成「账号被封」或「出站请求头伪装不够」，其实两者都不是：

- **同一个号、同一套出站头**，只把那段环境块去掉就能过；
- 也**不是**单个关键词 —— 把那段单独拎出来测是**通过**的，**「长前缀 + 那段」的组合**
  才必然命中，说明上游认的是累积出来的整体模式；
- 只影响**长会话**（system prompt 上到 ~10k 字、环境块完整时）；新会话几乎踩不到，
  所以表现成「时好时坏、重发有时能过」。

反代现在的处理：`sanitize_messages` 对 `role == "system"` 的消息调
`strip_claude_code_markers`，摘掉这些 Claude Code 注入标记：

- 最前面的 `x-anthropic-billing-header: cc_version=...; cc_entrypoint=...;`
- `You are Claude Code, Anthropic's official CLI for Claude.` 身份声明
  （CCH 的供应商测试探针会发这句，实测单独这一句就能触发 11128）
- `# Environment` / `You have been invoked in the following environment:` 环境块
  （非 git 目录没有 `gitStatus:`，旧逻辑会漏掉）
- `gitStatus:` 块（从最后一个 `gitStatus:` 起截断）

被摘时响应头带：

```
x-wb-switch-degraded: system-env
```

代价是模型这一轮看不到 git 状态（ZCode 每轮都会重新注入），远小于整条请求被 400 打回。

> **复现手法（零额度消耗）**：把 `model` 换成一个不存在的名字。上游先做渠道校验、
> 再做模型校验，于是返回码就能区分这两件事 —— `11102`（model 不存在）＝**过了**渠道校验，
> `11128` ＝**被**渠道校验拦下。用真 model 去试会实打实消耗额度，没必要。

### 11128 的第二种触发：首条不是 system（仅国际版）

`11128` 是**渠道校验的通用错误码**，不同触发有不同文案。2026-10-11 在国际版入口上
实测到第二种：

```
{"code":11128,"msg":"first message is not system prompt"}
```

对照实验（7864 / 国际版，同一个号、同一套出站头）：

| 入站 `messages` 首条 role | 结果 |
|---|---|
| `user` | ❌ `HTTP 400 {"code":11128,"msg":"first message is not system prompt"}` |
| `system`（同一请求前补一条） | ✅ **200 正常返回** |

**影响面**：Codex++ 保存连接时的**探针**只发一条 `user`，于是它把整个连接判成失败，
界面上就是一个红叉 —— 但真实对话（带 system）其实能通。国内版没有这条约束
（7863 上只发一条 `user` 是通的），所以**只在国际版兜**。

**反代处理**：`build_upstream_body` 增加 `edition` 入参，对国际版调
`ensure_leading_system` —— 若 `messages` 为空、或首条不是 `system`，在最前插一条
`{"role":"system","content":"You are a helpful assistant."}`，并把 `leading-system`
记进 `degraded`（响应头 `x-wb-switch-degraded: leading-system`）。

> 取舍：客户端不保证带 system（探针、简单脚本、部分 OpenAI 兼容客户端都不带）。
> 多一句无害的 system，远好过整条请求被 400 打回。

### 11128 的第三、四种触发：身份声明与客户端身份（2026-10-21 实测补充）

上游的渠道校验规则**会漂**：2026-10-09 判环境注入块、2026-10-17 判 `developer` 角色、
2026-10-21 复测前者已放行。已确认的触发方式与应对（含前两种）汇总：

| `msg` 文案 | 触发原因 | 该改什么 |
|---|---|---|
| `first message is not system prompt` | 首条不是 system（仅国际版） | 反代自动补 system |
| `Illegal API invocation from an unapproved channel` | **负载**被判非官方客户端：`role:"developer"`（精确字符串比对）、system 里的第三方 agent 身份声明（`DeepSeek Harness` 等） | 反代默认路径已改写角色、抹声明 |
| `request illegal` | **调用方**被判非官方客户端 | 改负载无用，只能换入口 / 等上游放行 |

第三方身份声明这一种（群友反馈、上游确认）：腾讯风控识别 system prompt 里固定的
`DeepSeek Harness` 声明，命中即 `11128 unapproved channel`。DSH 用户可关掉原生开关
`includeHarnessIdentity`。**本仓库与上游处理不同**：上游只在自愈重试时抹，本仓库
**默认路径就抹**（字符串与多块 content 都覆盖，`degraded` 记 `system-identity`），
因为这些短语是客户端自动注入的固定声明，不是用户自己写的内容。

### 11128 自愈重试（本仓库的分层实现）

收到 `unapproved channel`（负载变体）时，先做一次自愈重试再退避：

1. **默认层（无损）**：再抹一遍 system 里的身份声明（多块 content 形态也覆盖）。
   不改变请求形状，图片块原样保留，`degraded` 记 `self-heal-identity`。
2. **激进层（有损，需 `WB_SWITCH_AGGRESSIVE_HEAL=1`）**：最大强度归一化 ——
   角色收敛到 system/user/assistant/tool、摘掉 `name`、content 数组拍平成字符串。
   拍平会丢图片块，且改变了用户请求的形状，所以默认关闭，只作「实在不行就全收敛」的退路。

`request illegal`（客户端身份变体）**不走自愈** —— 改负载对它无效，只退避重试；
重试耗尽后如实告知「本地无法绕过」。

**冷却口径**：`11128` 不冷却账号（它拦的是请求不是号，同号只读接口照常 200）。
重试耗尽后的报错文案按两种变体分开给出可执行建议。

## 标准参数透传与 `tool_choice` 归一化

`build_upstream_body` 以前只转 `model / messages / stream / temperature / top_p /
max_tokens / tools`，**其余全被静默丢弃** —— 客户端设了 JSON 模式 / stop 序列 /
seed 复现 / 禁止并行工具调用，全部等于没设，且没有任何日志。

2026-10-21 上游用假模型 oracle 逐个验证，下面这些上游**全部接受**，已改为原样透传：

```
temperature  top_p  max_tokens  stop  frequency_penalty  presence_penalty
response_format  parallel_tool_calls  seed  stream_options  user
logprobs  top_logprobs
```

> `stream_options.include_usage` 会被强制为 `true`：本地用量统计依赖上游回的 usage，
> 客户端传 `false` 会把统计弄瞎。

### `tool_choice` 必须归一化

上游 `tool_choice` **只吃字符串**。OpenAI 规范允许对象形式，直接发过去会：

```
{"code":11101,"msg":"Unmarshal chat params failed with error: json: cannot unmarshal object …"}
```

注意这是 **11101**（参数解析失败），发生在渠道校验**之前** —— 用假模型 oracle 看到
11101 时，别误以为是模型问题。

函数名字符串 `"f"` 上游是接受的，所以对象形式可以**无损降级**：

| 入站 `tool_choice` | 发上游 |
|---|---|
| `{"type":"function","function":{"name":"f"}}` | `"f"` |
| `{"type":"auto"}` / `{"type":"none"}` / `{"type":"required"}` | 同名字符串 |
| `{"type":"any"}`（Anthropic） | `"required"` |
| `{"type":"tool","name":"g"}`（Anthropic） | `"g"` |
| `{"type":"allowed",…}` | `"auto"` |
| 认不出的形态 / 函数名为空 | **丢弃**（发过去只会 400），记 `tool-choice-dropped` |

> 与上游的差异：上游在拿不到函数名时兜底成 `"required"`，那会悄悄把「必须调 f」
> 变成「必须调某个工具」。本仓库选择丢弃并记录，让问题显式暴露。

Anthropic 入口（`/v1/messages`）同理：`{"type":"tool","name":"g"}` 直接翻译成
字符串 `"g"` —— 以前翻译成对象形式，Claude Code 每逢「指定工具」必 11101。

## 思考强度（`reasoning_effort`）

**反代以前把这些参数整个丢掉** —— `build_upstream_body` 只转
`model / messages / stream / temperature / top_p / max_tokens / tools`，于是表现为
「客户端调了推理等级、上游毫无反应、`reasoning_content` 一直是空」。
v1.6.7 起**原样透传** `reasoning_effort` / `thinking` / `reasoning` / `enable_thinking` /
`chat_template_kwargs`（只转客户端真的发了的字段，对不发这些的客户端零影响）。
之所以不翻译、直接透传：不同客户端拼写差得远，上游只认其中一种，转发比猜测安全。

### 上游只认 `reasoning_effort`

2026-10-11 直连上游实测（绕过本地反代，`deepseek-v4.1-flash`，同一账号同一套头）：

| 出站体里带的字段 | `reasoning_tokens` | 有 `reasoning_content` |
|---|---|---|
| （不发） | 0 | ❌ |
| `reasoning_effort: "high"` | **32** | ✅ |
| `thinking: {type:"enabled"}` + `reasoning_effort: "high"` | 38 | ✅ |
| `thinking: {type:"enabled"}` 单独 | 0 | ❌ |
| `thinking: {type:"enabled", budget_tokens: 8192}` | 0 | ❌ |
| `enable_thinking: true` | 0 | ❌ |

⇒ **`thinking`（含 `budget_tokens`）和 `enable_thinking` 上游一律忽略**，只有
`reasoning_effort` 真的起作用。虽然带上 `thinking` 不会报错，但也没用。

**取值**：`minimal / low / medium / high / max / xhigh / none` 全部 200；
🔴 **`off` 会 400** —— `{"code":11150,"msg":"the reasoning effort value is not supported by the current model"}`。
要「不思考」就**别发这个字段**，永远不要发 `off`。

**哪些模型有效**：国内版 `deepseek-v4-pro`（0 → 34）、`deepseek-v4.1-flash`（0 → 32）、
`space-bunny`（0 → 13）效果最明显；`glm-5.3` 小幅。国际版 `deepseek-v4.1-flash` 同样生效（0 → 17）。
`hy4-preview`、`glm-5.3-flash`、`gemini-3.5-flash` 本来就会思考，加了只是微调。

### 客户端（DSH / pi-ai）侧还得配一次

只改反代不够。DSH 的 `dsh-llm-pi-ai` 对**手工声明的路由**不去查 pi-ai 内置目录，
`resolveModelReasoning` 直接回落到 `base?.reasoning ?? false` → 模型 `reasoning: false`
→ UI **根本不显示**「推理等级」选择器。必须显式声明：

```yaml
- id: llm-pi-ai
  name: "@deepseek-ai/dsh-llm-pi-ai"
  config:
    providers:
      wb-in:
        apiKeyEnv: WB_IN_API_KEY
        api: openai-completions
        baseURL: http://127.0.0.1:7863/v1
        compat:
          supportsReasoningEffort: true    # ← 缺这行，pi-ai 只会发 thinking，而上游不认 thinking
        models:
          - id: deepseek-v4-pro
            name: deepseek-v4-pro
            reasoningEfforts:              # ← 缺这行，UI 里没有「推理等级」
              off:                         # 空值 = 关思考时不发该字段（⚠️ 不是空字符串）
              low: low
              medium: medium
              high: high
              max: max
```

三条都是源码里的硬校验，写错会让 profile 加载失败：

1. 键只能是 `off / minimal / low / medium / high / xhigh / max`；
2. 除 `off` 外每档都必须给值（空字符串会报 `must not be an empty string`）；
3. 至少要有一档不是 `off`；纯非推理模型写 `reasoningEfforts: false`。

> ⚠️ 只写 `thinkingFormat` 是不够的：`deepseek` 格式会发
> `thinking: {type:"enabled"}`，而 `reasoning_effort` **只有 `compat.supportsReasoningEffort`
> 为真时才会发**。上游恰好只认后者 —— 少了这一行，等于白配。
>
> ⚠️ 用 PyYAML 校验这类文件会误报：YAML 1.1 把裸键 `off` 读成布尔 `false`。
> DSH 用的 js-yaml（4.x）保留字符串 `off`，以它为准。

## 客户端接入（Claude Code / CCH）

Claude Code 走 Anthropic Messages，直接指到反代入口即可：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:7864     # 国际版；国内版换 7863
export ANTHROPIC_AUTH_TOKEN=<反代 API Key>          # 也接受 x-api-key
export ANTHROPIC_MODEL=deepseek-v4.1-flash          # 必须是上游实际可用的模型 id
claude
```

请求转换会处理：

- `system`（字符串 / 文本块数组）
- `text` / `image` 内容块
- `tools` / `tool_choice`
- assistant 的 `tool_use` → OpenAI `tool_calls`
- user 的 `tool_result` → OpenAI `role:"tool"`（`is_error` 会加 `[tool_error]` 前缀）
- `stop_sequences` → `stop`
- `max_tokens` / `temperature` / `top_p` / `top_k`

响应按 Anthropic SSE 事件序列返回（`message_start` → `content_block_*` →
`message_delta` → `message_stop`）。只要有工具调用，`stop_reason` 一律是 `tool_use` ——
部分上游在返回 `tool_calls` 时仍给 `finish_reason=stop`，照搬会让 Claude Code 认为
本轮已结束、工具还没执行就收工。

### 经 CCH（claude-code-hub）转发

CCH 本身**不做跨格式转换**（同格式路由），所以要让它把 Claude Code 接到本入口，
必须在 CCH 里把本入口注册成 `claude` 类型供应商：

| 字段 | 值 |
|---|---|
| URL | `http://host.docker.internal:7864`（CCH 在容器里；国内版换 7863） |
| Key | 本入口的 API Key |
| 类型 | `claude` |
| 分组 | 与 Claude Code 使用的 CCH key 的 `provider_group` 一致 |
| 模型白名单 | `deepseek-v4.1-flash`（或留空） |

⚠️ CCH 的模型白名单按**客户端原始请求的模型名**匹配，模型重定向发生在选供应商之后。
所以要么让 Claude Code 的 `ANTHROPIC_MODEL` 与白名单一致，要么把白名单留空。

### 已知限制

### 安全姿态（2026-10-23 起）

- `/status`、`/usage-stats` 不要 `api_key`，但会**校验 `Host` 与 `Origin`**：伪装域名的
  DNS rebinding、其他网站页面的跨源请求一律拒绝（以前实测可被当同源读走账号列表）。
- 反代路由本身（`/v1/*`，2026-10-24 起同样校验 `Host` 与 `Origin`）：`api_key` 只证明
  「有口令」，证明不了「请求从哪发起」—— 本机浏览器里任意页面都能
  `fetch('http://127.0.0.1:7863/v1/...')`。**脚本 / SDK 不带 `Origin`，完全不受影响**。
- 容器里监听 `0.0.0.0`、用域名访问时，把域名加进 `WB_SWITCH_ALLOWED_HOSTS`
  （支持逗号分隔与前导 `*.`），否则 `Host` 校验会回 421。
- **Docker 宿主机别名内建放行**（2026-10-24 起）：`host.docker.internal` /
  `gateway.docker.internal` 不需要进 `WB_SWITCH_ALLOWED_HOSTS`——它们只在容器内的
  嵌入式 DNS 解析（宿主机与公网都解析不了，`.internal` 是保留 TLD），浏览器不可能
  带着这个 `Host` 打进来；CCH 等容器网关因此直接可用，不会再 421。

### 超时与断线（2026-10-23 起）

| 环节 | 上限 | 说明 |
|---|---|---|
| 建连 | 10 秒 | 超时走原有的连接层重试（`MAX_TRANSPORT_RETRY`） |
| 首包 | 90 秒 | 出站到上游第一个字节 |
| 帧间隔 | 240 秒 | 两次收到上游数据之间；防网关静默丢包导致请求永久悬挂 |

客户端中途断开（Claude Code 里按 Esc）时，泵**立刻停止读上游**，这次请求记 `ok=false`，
已经拿到的部分 token 照实计入用量。以前会把剩余回复读完丢弃、还记成功。


- 上游 `reasoning_content` 不转成 Anthropic `thinking`（没有可用签名），思考内容不展示
- 不支持 prompt caching：`cache_control` 丢弃，`cache_*` usage 字段按上游实际值给
- `stop_sequence` 无法区分，统一映射为 `end_turn`
- `count_tokens` 是本地估算值，不参与计费
- 非流式 Anthropic 请求内部走流式再聚合，用量统计里的 stream 标记会是 true
- 模型必须是 WorkBuddy 实际可用的 id（当前实测 `deepseek-v4.1-flash` 可用）

## 客户端接入（Codex / Codex++）

外部客户端把本反代当上游时，**OpenAI 侧协议必须选 Chat Completions** —— 我们实现了
`/v1/chat/completions` 与 Anthropic `/v1/messages`，**没有 Responses API**；选 Responses 会直接 404。

### Codex / Codex++

Codex 的供应商写在 `~/.codex/config.toml`：

```toml
model = "glm-5.3-flash"
model_provider = "wbrelay"

[model_providers.wbrelay]
name = "wb-switch 国内版反代"
base_url = "http://127.0.0.1:7863/v1"
wire_api = "chat"            # 🔴 不是 "responses"
requires_openai_auth = true  # 取 ~/.codex/auth.json 的 OPENAI_API_KEY 当 Bearer
```

`auth.json` 里的 `OPENAI_API_KEY` 必须等于本反代该入口的 `api_key`
（见 `~/.wb-switch/proxy.json`）。只列本入口那一版的模型（国内版 7863 / 国际版 7864，别连错端口）。

⚠️ **Codex++（第三方管理器）的坑**：它的「切换供应商」会**先回填当前供应商**，
而回填要求 `config.toml` 里**已经存在** `model_provider` + `[model_providers.*]`（得有 base_url）。
实时配置为空时它会直接中止并报「实时配置缺少有效的供应商 endpoint」——
于是**怎么写都过不去**（鸡生蛋）。**先手写一段上面的 bootstrap 配置即可解锁。**

另外两点别误判：它的 API Key 是用 Windows **DPAPI 加密**存在自己库里的，
**在磁盘上 grep 不到明文属正常**，不代表没保存；改完必须**从 Codex++ 入口启动**并重启它才生效
（直接开官方应用，Codex++ 不在链路上）。

## 已知限制

- OpenAI 侧只做了 `/v1/chat/completions`，没有 embeddings / images / audio；也没有 Responses API
- Anthropic 侧只做了 `/v1/messages` 与 `/v1/messages/count_tokens`
- 工具调用（function calling）会透传给上游，但没有做额外的 schema 校验
- `/v1/models` 是从上游 `data.agents[].models[]` 里提取的，拉不到时回退到一个内置列表
- 模型目录只取了 `/v2/enterprises/personal/models` 一个来源，不含 `/v3/config`（CLI UA）里的
  `deepseek-v4.1-flash` 等可用模型 —— 不影响调用，只影响列表展示，见下方
- 上游只认 text / image_url，其它内容块会被降级成文本（见上）

- 🔴 **上游的模型列表不完整 —— 版本防火墙会因此误拦**（2026-10-11 实测）。
  `www.workbuddy.ai/v2/enterprises/personal/models` 返回的 29 KB JSON 里 **"deepseek" 出现 0 次**，
  列表里含 `deep` 的只有 `deep-model`（一个 UI 用的模式名）；但直发 `/v2/chat/completions`
  用 **`deepseek-v4.1-flash` 是 200 成功的**，而列表里的 `deep-model` 反而返回 11102。
  ⇒ 也就是说这些列表是**给客户端 UI 的展示列表（很多是模式名）**，**不等于可用模型全集**。
  而 `model_edition()` / 版本防火墙正是按这个列表学习归属的 → 会把「国际版实际可用但未列出」的
  模型误判成国内版专属，直接 400 掉（报「请改用国内版入口」）。
  ⚠️ 防火墙本身的前提是对的（跨版本确实会被上游拒：`codebuddy.cn ← gpt-5.6-sol` → `11102 only available
  for authorized users`），**错的是判据来源**。

### 模型目录只取了一个来源，`/v3/config`（CLI UA）里的模型看不到（2026-10-19 实测，暂不修）

反代的 `/v1/models` 与「获取模型ID」只请求 `GET /v2/enterprises/personal/models`，
取 `data.agents[].models[]`。但上游还有**第二个目录接口**，按 `User-Agent` 下发不同集合：

| 请求 | 国际版返回 | 是否含 deepseek |
|---|---|---|
| `GET /v2/enterprises/personal/models`（**当前唯一来源**） | 18 个（cli agent 17 个） | ❌ 0 个 |
| `GET /v3/config` + IDE UA `CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0` | 13 个 | ❌ 0 个 |
| `GET /v3/config` + CLI UA `CLI/2.63.2 CodeBuddy/2.63.2` | 20 个 | ✅ `deepseek-v4.1-flash`、`deepseek-v4.1-flash-sg`、`deep-model` |

`/v3/config` + CLI UA 响应的形态（`data.models[]`，每项带 `id` / `name` / `credits` / `maxOutputTokens` / `tags`）：

```
default-model(Auto)  fast-model(Fast)  balanced-model(Balanced)  primary-model(Primary)
deep-model(Deep)     deepseek-v4.1-flash(Deepseek-V4.1-Flash, x0.00)  deepseek-v4.1-flash-sg(, x0.03)
hy4-preview  hy3  kimi-k2.8-preview  gpt-5.6-terra  gpt-5.6-luna  gpt-5.5  gpt-5.4
gemini-3.5-flash  glm-5.3-flash  glm-5.3  glm-5.2  kimi-k3  kimi-k2.6
```

另有 `data.productFeaturesConfig.ModelTrialBanner` 会放试用模型（如 `hy4-preview-f` → `hy4-preview`）。

同一次探测里，国际版这 5 个「只在 CLI UA 目录出现」的模型**实测都能调通**（流式首帧 200）：
`deepseek-v4.1-flash`、`deepseek-v4.1-flash-sg`、`glm-5.3-flash`、`kimi-k2.8-preview`、`hy4-preview-f`。
`glm-5.3-flashx` 同样不在目录里，`max_tokens` 较小时报 `11133`，调到 64 后 200。

**这不影响调用**：防火墙只按「拉到的列表」学习归属，没见过的模型一律放行交给上游判定
（`model_edition()` 返回 `None`），所以 `deepseek-v4.1-flash` 不在列表里也能正常请求。

**影响**：客户端看「获取模型ID」选模型时会漏掉这些可用模型；用户只能手动填 `extra_models`。
国内版目录没有账号可实测，但从 panel-plus 默认编排把 `cn:deepseek-v4.1-flash` 列为国内版主模型看，
它大概率也在国内版的 `/v3/config` 里。

**待办（暂缓）**：把 `/v3/config`（CLI UA）并入模型来源，两版都拉、并集去重；
需过滤非对话条目（`nes-*`、`completion-*`、`codewise-*`、`tags` 含 `text-to-image` 的图片/视频模型），
否则客户端可能选中它们换来 400。参考实现：`workbuddy2api-panel-plus` 的
`internal/upstream/client.go` `FetchModels` / `fetchV3Models`（MIT）。

## 源码位置

| 文件 | 作用 |
|---|---|
| `crates/wb-switch-core/src/modules/proxy.rs` | 服务本体：账号池、请求头、流式聚合、生命周期 |
| `src-tauri/src/commands.rs` | `get_proxy_config` / `save_proxy_config` / `get_proxy_status` / `set_account_group` |
| `src/pages/ApiProxyPage.tsx` | 独立的「API 反代」页（侧边栏「设置」上面） |
| `src/components/account-card.tsx` | 账号卡片：分组徽章 + 菜单里的分组设置 |
| `src/lib/api.ts`、`src/lib/types.ts` | 前端调用与类型 |

## 编译

```bash
export PATH="/f/Code/ENV/RUST/bin:$PATH"
export CARGO_HOME="F:/Code/ENV/RUST/cargo-home"
export RUSTUP_HOME="F:/Code/ENV/RUST/rustup-home"

npm install
npm run build                       # 先出 dist，Tauri 构建需要它
cargo build --release -p wb-switch-rust
# 产物：target/release/wb-switch-rust.exe
```

> 仓库归属已改为 `github.com/cv-superding/WorkBuddy-Switch2api`。
> `src-tauri/tauri.conf.json` 里 `updater.endpoints` 当前为空、
> `createUpdaterArtifacts` 为 false —— 这样自编译版不会被任何远端更新覆盖。
> 想开启自动更新时，把 endpoints 指向自己仓库的 releases 即可。
> 原配置备份在 `src-tauri/tauri.conf.json.orig`。
