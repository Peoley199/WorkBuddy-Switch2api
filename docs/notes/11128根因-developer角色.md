# 11128 根因：`role: "developer"`（已修复）

## 一句话

**DSH 把 system prompt 用 `role: "developer"` 发出去，上游两道网关都把它判成「非官方渠道」，
整条请求 400 `11128 Illegal API invocation from an unapproved channel`。**

## 铁证：单变量对照

来源：反代调试落盘的真实报文 `~/.wb-switch/dump/last-request.json`。

```
edition=国际版  uid=uid-xxxxxx…  model=deepseek-v4.1-flash  degraded=['leading-system']
messages（8 条）:
  [0] role='system'     28 字     ← 反代 ensure_leading_system 兜的那条
  [1] role='developer'  5939 字   ← 🔴 DSH 的 system prompt，用了 developer 角色
  [2..7] role='user'    …
```

用**同一段正文**、只改第 2 条的 role：

| messages | 国际版 | 国内版 |
|---|---|---|
| `[system, system(真实正文), user]` | **200 ✅** | **200 ✅** |
| `[system, developer(同一正文), user]` | **11128 ❌** | **11128 ❌（3/3 稳定）** |
| `[system, user(同一正文), user]` | **200 ✅** | — |

⇒ `developer` 是唯一变量，**两版网关都拦**。

## 为什么之前怎么查都复现不出来

我从 DSH 会话存储（`~/.dsh/sessions/.../session.v4.jsonl.zstd`）里读 `system/message` 记录来复现，
那条记录写的是 **`role: "system"` + 数组型 content**；
但 DSH **真正发到线上**的是 **`role: "developer"` + 字符串 content**。

> **会话/日志里的副本 ≠ 实发报文。**
> 是我临时给反代加的「抓线路报文」开关才拿到真相：
> 标记文件 `~/.wb-switch/DEBUG_DUMP` 存在时，把发往上游的真实 body 写到 `~/.wb-switch/dump/`。
> （用标记文件而非环境变量，是为了**不用改启动方式** —— 从终端启动会继承 `HTTP_PROXY` 反而弄坏。）

## 修复

`crates/wb-switch-core/src/modules/proxy.rs` 的 `sanitize_messages()` 开头：

```rust
// developer 是 OpenAI 自 o1 起引入的 system 别名，语义等价；改写不丢内容。
if m.get("role").and_then(Value::as_str) == Some("developer") {
    m["role"] = Value::String("system".to_string());
    degraded.push("developer-role".to_string());
}
```

- 必须放在「system 环境注入净化」**之前**，这样开发者消息也能吃到净化。
- 改写后首条即 `system`，`ensure_leading_system` 不再多兜一条
  ⇒ 报文变成 `[system(5939), user…]`，正好是实测 200 的那一档。

## 验证

| 项 | 结果 |
|---|---|
| 单测 `developer_role_is_rewritten_to_system`（改 role / 内容不动 / 净化顺序 / 无 developer 不误标） | debug ✅  **release ✅** |
| `cargo test -p wb-switch-core --lib proxy` | 26 passed |
| `RUSTFLAGS="-D warnings"` | 干净 |
| exe 与编译产物 md5 | 一致 `0f5132915dd761f55fd78f7c40af40e9` |

## 顺带一起进的 1.6.8

1. **11128 自动重试**：`is_channel_block()` 识别后重试 2 次（300/600ms 退避、每次重算会话头族），
   **重试仍失败才冷却账号** —— 旧逻辑「任何非 2xx 即冷却 120 秒」会把好号几轮冻光（你只有 6 个国内号）。
2. **调试落盘开关**（标记文件控制，默认关）。

## 你要做的

1. **双击启动 wb-switch**（别从终端启动）
2. DSH 里再发一次 —— 国际版、国内版都试试

如果**还报错**：抓包开关我留着没关，`~/.wb-switch/dump/last-request.json` 会自动更新，
我再看一眼就能对齐。确认好了我就把开关关掉、然后把 1.6.8 推 GitHub。

---

*2026-10-11 22:05 ｜ 1.6.8 已部署，未推 GitHub*
