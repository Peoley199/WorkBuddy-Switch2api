# API 反代（把 Switch 变成 OpenAI 兼容网关）

在 WorkBuddy Switch 里内置了一个本机 HTTP 服务，把你在 Switch 里管理的账号
包装成标准的 OpenAI 接口，任何支持 OpenAI SDK 的客户端都能直接用。

## 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | 对话补全，支持 `stream: true/false` |
| GET | `/v1/models` | 可用模型列表 |
| GET | `/healthz` | 健康检查 |
| GET | `/status` | 参与账号与运行状态 |

默认监听 `127.0.0.1:7863`，所以在客户端里填：

```
base_url = http://127.0.0.1:7863/v1
api_key  = （没设置就随便填，例如 sk-anything）
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
4. 状态行会显示 `运行中 · OpenAI 基址 http://127.0.0.1:7863/v1`

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
列表外的 ID 也能直接发（上游支持即可）。

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
`strip_env_injection`，从**最后一个** `gitStatus:` 起截断。被摘时响应头带：

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

## 客户端接入（Codex / Codex++）

外部客户端把本反代当上游时，**协议必须选 Chat Completions** —— 我们只实现了
`/v1/chat/completions`，**没有 Responses API**；选 Responses 会直接 404。

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

- 只做了 `/v1/chat/completions`，没有 embeddings / images / audio
- 工具调用（function calling）会透传给上游，但没有做额外的 schema 校验
- `/v1/models` 是从上游 `data.agents[].models[]` 里提取的，拉不到时回退到一个内置列表
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
