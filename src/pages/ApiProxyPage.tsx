import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Check, Clock3, Copy, ListPlus, Loader2, RefreshCw, Save, Trash2, Users } from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import * as api from "@/lib/api";
import {
  accountGroupLabel,
  EDITIONS,
  type AccountMeta,
  type ProxyAccountCooldown,
  type ProxyConfig,
  type ProxyEndpoint,
  type ProxyEndpointStatus,
  type ProxyModels,
  type ProxyStatus,
  type ProxyUsage,
  type UsageBucket,
} from "@/lib/types";
import { cn } from "@/lib/utils";

type AccountGroup = {
  key: string;
  label: string;
  items: AccountMeta[];
  /** 这一组本身就是按版本拆出来的，行内不用再标版本。 */
  byEdition: boolean;
};

/**
 * 账号分组。
 *
 * 「反代API」这一组**必须按版本再拆开**：国内版和国际版是两套上游，
 * 模型列表不一样，混在一起既看不出谁是谁，也说不清某个模型该由谁来接。
 * 其它组不拆，只在每行标一个版本徽标。
 *
 * 反代这两节只要有一档有人就都显示（另一档显示 0 个）——
 * 这样「我没国际版账号」这件事是看得见的，而不是悄悄少一截。
 */
function groupAccounts(accounts: AccountMeta[]): AccountGroup[] {
  const pick = (group: string, edition?: string) =>
    accounts.filter(
      (a) =>
        (a.group ?? "") === group &&
        (edition === undefined || (a.edition ?? "domestic") === edition),
    );

  const out: AccountGroup[] = [];
  const proxyTotal = pick("proxy").length;
  for (const e of EDITIONS) {
    out.push({
      key: `proxy|${e.value}`,
      label: `反代API · ${e.label}`,
      items: pick("proxy", e.value),
      byEdition: true,
    });
  }
  if (proxyTotal === 0) {
    out.splice(0, out.length);
  }
  out.push({
    key: "desktop",
    label: accountGroupLabel("desktop"),
    items: pick("desktop"),
    byEdition: false,
  });
  out.push({
    key: "ungrouped",
    label: accountGroupLabel(""),
    items: pick(""),
    byEdition: false,
  });
  return out.filter((g) => g.byEdition || g.items.length > 0);
}

/** 一行里显示的版本徽标（分组本身已经按版本拆了就不用再标）。 */
/** 本页那个入口的配置区：版本标识 + 开关、监听地址、API Key。
 *
 * 刻意**不带自己的外框** —— 它渲染在页面那张 `Card` 里面，再套一层边框只会
 * 显得嵌套。分隔线由各行自己给，和页面里其它设置行保持一致。
 */
function EndpointSection({
  edition,
  endpoint,
  status,
  onChange,
}: {
  edition: "domestic" | "international";
  endpoint: ProxyEndpoint;
  status?: ProxyEndpointStatus;
  onChange: (next: ProxyEndpoint) => void;
}) {
  const label = edition === "domestic" ? "国内版" : "国际版";
  const hint =
    edition === "domestic"
      ? "使用 .workbuddy 的账号 · www.codebuddy.cn"
      : "使用 .workbuddy-ai 的账号 · www.workbuddy.ai";
  const listenPlaceholder = edition === "domestic" ? "127.0.0.1:7863" : "127.0.0.1:7864";

  // 「额外放行模型」是受控输入：直接拿 parseModels 的结果 join 回显，
  // 会把用户正在输入的分隔符当场吞掉（多个名字粘成一个）。这里维护一份原始草稿，
  // 只有外部真的改了配置才同步进来。
  const [extraDraft, setExtraDraft] = useState(() => endpoint.extra_models.join(", "));
  const lastPushedRef = useRef(endpoint.extra_models.join(", "));
  const joined = endpoint.extra_models.join(", ");
  useEffect(() => {
    if (joined !== lastPushedRef.current && joined !== extraDraft) {
      lastPushedRef.current = joined;
      setExtraDraft(joined);
    }
    // 只在 joined 变化时同步；自己敲的内容不会被外部值覆盖。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [joined]);

  return (
    <>
      <div className="mx-4 flex items-center justify-between gap-3 border-b border-border/50 py-3 sm:mx-5">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <Label htmlFor={`proxy-enabled-${edition}`} className="text-[13px] leading-4">
              启用{label}接口
            </Label>
            {status?.running ? (
              <Badge
                variant="secondary"
                className="h-5 shrink-0 border-0 px-1.5 text-[10px] leading-5 text-emerald-600"
              >
                监听中
              </Badge>
            ) : null}
          </div>
          <p className="mt-0.5 text-xs leading-4 text-muted-foreground/75">{hint}</p>
        </div>
        <Switch
          id={`proxy-enabled-${edition}`}
          checked={endpoint.enabled}
          onCheckedChange={(v) => onChange({ ...endpoint, enabled: v })}
          aria-label={`启用${label}反代`}
        />
      </div>

      <div className="mx-4 flex flex-col items-stretch justify-between gap-2 border-b border-border/50 py-3 sm:mx-5 sm:flex-row sm:items-center">
        <div className="min-w-0 flex-1">
          <Label htmlFor={`proxy-listen-${edition}`} className="text-[13px] leading-4">
            监听地址
          </Label>
          <p className="mt-0.5 text-xs leading-4 text-muted-foreground/75">
            建议保持 127.0.0.1；只有受信任的内网才改成 0.0.0.0
          </p>
        </div>
        <Input
          id={`proxy-listen-${edition}`}
          value={endpoint.listen}
          onChange={(e) => onChange({ ...endpoint, listen: e.target.value })}
          className="h-8 w-full sm:w-56"
          placeholder={listenPlaceholder}
        />
      </div>

      <div className="mx-4 flex flex-col items-stretch justify-between gap-2 border-b border-border/50 py-3 sm:mx-5 sm:flex-row sm:items-center">
        <div className="min-w-0 flex-1">
          <Label htmlFor={`proxy-key-${edition}`} className="text-[13px] leading-4">
            API Key
          </Label>
          <p className="mt-0.5 text-xs leading-4 text-muted-foreground/75">
            留空 = 不鉴权；OpenAI 客户端用 Authorization: Bearer &lt;key&gt;，Anthropic / Claude Code 用 x-api-key（或 Bearer）
          </p>
        </div>
        <Input
          id={`proxy-key-${edition}`}
          type="password"
          value={endpoint.api_key}
          onChange={(e) => onChange({ ...endpoint, api_key: e.target.value })}
          className="h-8 w-full sm:w-56"
          placeholder="留空 = 不鉴权"
        />
      </div>

      <div className="mx-4 flex flex-col items-stretch justify-between gap-2 border-b border-border/50 py-3 sm:mx-5 sm:flex-row sm:items-center">
        <div className="min-w-0 flex-1">
          <Label htmlFor={`proxy-extra-${edition}`} className="text-[13px] leading-4">
            额外放行模型
          </Label>
          <p className="mt-0.5 text-xs leading-4 text-muted-foreground/75">
            上游列表里没有、但本入口实际可用的模型（逗号分隔）。填进来的会出现在
            /v1/models 里，并跳过「模型属于另一版」的拦截
          </p>
        </div>
        <Input
          id={`proxy-extra-${edition}`}
          value={extraDraft}
          onChange={(e) => {
            const next = e.target.value;
            setExtraDraft(next);
            lastPushedRef.current = parseModels(next).join(", ");
            onChange({ ...endpoint, extra_models: parseModels(next) });
          }}
          className="h-8 w-full sm:w-56"
          placeholder="如 deepseek-v4.1-flash"
        />
      </div>
    </>
  );
}

/** 把输入框里的一串模型名拆成数组：逗号 / 顿号 / 空白 / 换行都算分隔符。 */
function parseModels(s: string): string[] {
  return s
    .split(/[,，、\s]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function editionLabelOf(a: AccountMeta): string {
  return EDITIONS.find((e) => e.value === (a.edition ?? "domestic"))?.label ?? "国内版";
}

function num(v?: number): string {
  return (v ?? 0).toLocaleString("zh-CN");
}

/** credit 是浮点累加值，直接 String() 会漏出 603.9699999999999 这种尾数，固定三位小数。 */
function num3(v?: number): string {
  return new Intl.NumberFormat("zh-CN", {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  }).format(v ?? 0);
}

/** 剩余时间：`1:23` / `12 秒`；已过期返回 null（调用方据此收掉徽标）。 */
function formatRemaining(until: number, now: number): string | null {
  const ms = until - now;
  if (ms <= 0) return null;
  const total = Math.ceil(ms / 1000);
  if (total < 60) return `${total} 秒`;
  const m = Math.floor(total / 60);
  const s = total % 60;
  if (m < 60) return `${m}:${String(s).padStart(2, "0")}`;
  const h = Math.floor(m / 60);
  return `${h} 小时 ${m % 60} 分`;
}

/** 恢复时刻（本地时间），给 Tooltip 用。 */
function formatUntil(until: number): string {
  return new Date(until).toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/**
 * 每秒走一次的「当前时刻」。
 *
 * 后端给的是**绝对恢复时刻**，所以倒计时纯本地算 —— 不轮询、不请求后端。
 * 只在真的有冷却时才起定时器，冷却全部结束后自动停。
 */
function useNow(active: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [active]);
  return now;
}

/** 一条冷却徽标：账号级（琥珀）或模型级（次要色）。 */
function CooldownChip({
  label,
  title,
  tone,
}: {
  label: string;
  title: string;
  tone: "account" | "model";
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge
          variant="secondary"
          className={cn(
            "h-5 shrink-0 gap-1 border-0 px-1.5 text-[10px] leading-5",
            tone === "account"
              ? "bg-amber-100 text-amber-800"
              : "text-muted-foreground",
          )}
        >
          <Clock3 className="size-3" />
          {label}
        </Badge>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-72">
        {title}
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 某个账号的冷却徽标。
 *
 * 账号级冷却优先显示（整个号都不可用）；没有账号级冷却时，逐个列出被冻的模型
 * —— 那种情况下换个模型就能继续用，文案必须说清楚，别让人以为号废了。
 */
function AccountCooldownChips({
  cooldown,
  now,
}: {
  cooldown: ProxyAccountCooldown | undefined;
  now: number;
}) {
  if (!cooldown) return null;

  const accountLeft =
    cooldown.until != null ? formatRemaining(cooldown.until, now) : null;
  if (accountLeft) {
    return (
      <CooldownChip
        tone="account"
        label={`冷却 ${accountLeft}`}
        title={`${cooldown.reason || "上游失败"} · ${formatUntil(cooldown.until!)} 恢复。该账号暂时不参与轮转。`}
      />
    );
  }

  // 账号级冷却已结束（或本来就没有）：只展示仍在冷却的模型。
  const models = (cooldown.models ?? [])
    .map((m) => ({ ...m, left: formatRemaining(m.until, now) }))
    .filter((m): m is typeof m & { left: string } => m.left != null);
  if (models.length === 0) return null;

  return (
    <>
      {models.slice(0, 2).map((m) => (
        <CooldownChip
          key={m.model}
          tone="model"
          label={`${m.model} ${m.left}`}
          title={`模型 ${m.model}：${m.reason} · ${formatUntil(m.until)} 恢复。该账号的其他模型仍可用。`}
        />
      ))}
      {models.length > 2 ? (
        <CooldownChip
          tone="model"
          label={`+${models.length - 2} 个模型`}
          title={models
            .slice(2)
            .map((m) => `${m.model}（${m.left}）`)
            .join("、")}
        />
      ) : null}
    </>
  );
}

/**
 * 一张统计卡片。
 *
 * 🔴 **字号写死，不做任何「按长度分档」** —— 分档就会一排里出现「一张大一张小」，
 * 这个坑踩过两次（先按各卡片自己长度分，改成按整行最长分，都还是会被看成不齐）。
 * 四张统一 18px（`text-lg`）；个别超长的数字靠 `truncate` + `title` 兜底。
 */
function StatCard({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="min-w-0 rounded-xl border border-border bg-muted/25 px-3 py-3 sm:px-4">
      <div className="truncate text-xs text-muted-foreground">{label}</div>
      <div
        className="mt-1 truncate text-lg font-semibold tabular-nums tracking-tight"
        title={value}
      >
        {value}
      </div>
      {hint ? (
        <div className="mt-0.5 truncate text-[11px] text-muted-foreground/70" title={hint}>
          {hint}
        </div>
      ) : null}
    </div>
  );
}

function bucketRow(name: string, b: UsageBucket) {
  return (
    <div key={name} className="flex items-center justify-between gap-3 px-3 py-2 text-[13px]">
      <span className="min-w-0 flex-1 truncate">{name}</span>
      <span className="shrink-0 tabular-nums text-muted-foreground">
        {num(b.requests)} 次
      </span>
      <span className="w-28 shrink-0 text-right tabular-nums leading-tight text-muted-foreground">
        {num(b.promptTokens)} / {num(b.completionTokens)}
      </span>
    </div>
  );
}

export default function ApiProxyPage({
  edition,
}: {
  /** 本页服务哪一版。国内版与国际版是**两个独立页面**，各有自己的监听地址与 API Key。 */
  edition: "domestic" | "international";
}) {
  const [params] = useSearchParams();
  const [cfg, setCfg] = useState<ProxyConfig | null>(null);
  const [st, setSt] = useState<ProxyStatus | null>(null);
  const [accounts, setAccounts] = useState<AccountMeta[]>([]);
  const [usage, setUsage] = useState<ProxyUsage | null>(null);
  const [saving, setSaving] = useState(false);
  const [resettingUsage, setResettingUsage] = useState(false);
  const [msg, setMsg] = useState<{ type: "ok" | "err"; text: string } | null>(null);
  // 「获取模型ID」：拉一次模型列表，弹窗展示 + 一键复制
  const [models, setModels] = useState<ProxyModels | null>(null);
  const [modelsOpen, setModelsOpen] = useState(false);
  const [loadingModels, setLoadingModels] = useState(false);
  const [copied, setCopied] = useState<string | null>(null);

  useEffect(() => {
    void load();
    // 截图/演示用：?models=1 直接展开模型列表（和迁移页的 ?tab=import 一个路子）
    if (params.get("models") === "1") void fetchModels();
    // 🔴 依赖 `edition`，不能是空数组。
    //
    // 国内版与国际版是**同一个组件**（只换 `edition` prop）挂在两个路由上：
    // `/api-proxy/domestic` 与 `/api-proxy/international`。React 在路由切换时
    // 会**复用同一个实例**（同类型同位置）⇒ 空依赖的 effect 不会再跑，
    // 于是切过去看到的还是上一版的数据，必须手点「刷新」才对 —— 用户实测就是这个症状。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [edition]);

  /**
   * 冷却状态轮询。
   *
   * 冷却只会因为「请求失败」而产生，后端不会主动推送，所以这里定时问一次。
   * 只拉 `get_proxy_status`（轻量），不重跑 `load()` —— 那会把账号列表和用量
   * 也重拉一遍，既慢又会让页面闪。倒计时本身是本地算的（见 `useNow`），
   * 这个轮询只负责「有没有新的号进入/退出冷却」。
   */
  useEffect(() => {
    if (!st?.running) return;
    let cancelled = false;
    const poll = () => {
      // 窗口隐藏时不打扰后端。
      if (document.visibilityState === "hidden") return;
      void api.getProxyStatus().then((s) => !cancelled && setSt(s)).catch(() => {});
    };
    const timer = window.setInterval(poll, 10_000);
    // 隐藏期间浏览器会把定时器压到很慢，回前台时补拉一次，避免带着过期的冷却数据。
    const onVisible = () => {
      if (document.visibilityState !== "hidden") poll();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [st?.running]);

  async function load() {
    try {
      const [c, s, list, u] = await Promise.all([
        api.getProxyConfig(),
        api.getProxyStatus(),
        api.getAccounts(),
        api.getProxyUsage(edition),
      ]);
      setCfg(c);
      setSt(s);
      setAccounts(list.accounts);
      setUsage(u);
    } catch (e) {
      setMsg({ type: "err", text: api.asError(e) });
    }
  }

  async function save() {
    if (!cfg) return;
    setSaving(true);
    setMsg(null);
    try {
      const res = await api.saveProxyConfig(cfg);
      // 本页只管自己这一版 —— 另一版的结果不该在这里报（那边有它自己的页面）。
      const mine = res.endpoints.find((e) => e.edition === edition);
      if (mine && !mine.ok) {
        setMsg({ type: "err", text: `已保存，但${mine.label}入口未启动：${mine.error}` });
      } else if (mine?.ok) {
        setMsg({ type: "ok", text: `已保存，${mine.label}入口已启动` });
      } else {
        setMsg({ type: "ok", text: "已保存（本入口未启用）" });
      }
      setSt(await api.getProxyStatus());
    } catch (e) {
      setMsg({ type: "err", text: api.asError(e) });
    } finally {
      setSaving(false);
    }
  }

  function toggleAccount(uid: string, on: boolean) {
    const next = new Set(ep?.accounts ?? []);
    if (on) next.add(uid);
    else next.delete(uid);
    patchEndpoint({ accounts: Array.from(next) });
  }

  /** 拉一次可用模型列表：优先问本机反代，拿不到再直连上游。 */
  async function fetchModels() {
    setLoadingModels(true);
    setMsg(null);
    try {
      // 只拉本页这一版的模型 —— 另一版的模型在这个入口上用不了，列出来只会误导。
      const res = await api.getProxyModels(edition);
      setModels(res);
      setModelsOpen(true);
    } catch (e) {
      setMsg({ type: "err", text: api.asError(e) });
    } finally {
      setLoadingModels(false);
    }
  }

  async function copyText(text: string, key: string) {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(key);
      setTimeout(() => setCopied((cur) => (cur === key ? null : cur)), 1600);
    } catch {
      toast.error("复制失败", { description: "可以手动选中后 Ctrl+C" });
    }
  }

  const label = edition === "domestic" ? "国内版" : "国际版";
  /** 本页服务的那一个入口。 */
  const ep = cfg?.[edition];
  const epStatus = st?.endpoints.find((e) => e.edition === edition);
  /** 本页只关心**这一版**的账号 —— 另一版的号在这个入口上根本用不了。 */
  const editionAccounts = accounts.filter((a) => (a.edition ?? "domestic") === edition);
  const allAccounts = (ep?.accounts.length ?? 0) === 0;
  const groups = groupAccounts(editionAccounts);

  /** 本版正在冷却的号：`uid -> 冷却状态`。 */
  const cooldowns = new Map(
    (st?.accounts ?? [])
      .filter((c) => c.edition === edition)
      .map((c) => [c.uid, c] as const),
  );
  // 只在真有冷却时走秒；否则不起定时器（页面闲着不该每秒重渲染）。
  const now = useNow(cooldowns.size > 0);

  /**
   * 当前仍然有效的冷却条数（账号级算 1 条，每个被冻模型各算 1 条）。
   *
   * 用本地时刻算，所以冷却一结束汇总就自动消失 —— 不必等后端下一次返回。
   */
  const coolingCount = Array.from(cooldowns.values()).reduce((sum, c) => {
    const accountActive = c.until != null && c.until > now;
    const models = (c.models ?? []).filter((m) => m.until > now).length;
    return sum + (accountActive ? 1 : models);
  }, 0);

  /** 改本页那个入口的字段。
   *
   * 两个入口是独立的，写回时必须指明改的是哪一边。用 `{ ...cfg, [edition]: ... }`
   * 这种计算属性在 TS 里会退化成索引签名、丢掉类型，所以显式分两支写。
   */
  function patchEndpoint(patch: Partial<ProxyEndpoint>) {
    setCfg((prev) =>
      prev
        ? {
            ...prev,
            domestic: edition === "domestic" ? { ...prev.domestic, ...patch } : prev.domestic,
            international:
              edition === "international"
                ? { ...prev.international, ...patch }
                : prev.international,
          }
        : prev,
    );
  }

  async function onResetUsage() {
    setResettingUsage(true);
    try {
      // 只清**本页这一版**的统计，别把另一版的也清了。
      await api.resetProxyUsage(edition);
      setUsage(await api.getProxyUsage(edition));
      setMsg({ type: "ok", text: "本页的用量统计已清空" });
    } catch (e) {
      setMsg({ type: "err", text: api.asError(e) });
    } finally {
      setResettingUsage(false);
    }
  }

  return (
    <div className="mx-auto min-w-0 w-full max-w-3xl px-4 py-6 sm:px-6 sm:py-8">
      <header className="mb-8 sm:mb-10">
        <h1 className="text-[28px] font-semibold tracking-tight">{label} API 反代</h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          把 Switch 里的{label}账号包装成本机网关：OpenAI 兼容（<code className="rounded bg-muted px-1 py-0.5">/v1/chat/completions</code>）与 Anthropic 兼容（<code className="rounded bg-muted px-1 py-0.5">/v1/messages</code>），OpenAI SDK 与 Claude Code 都能直接用。
        </p>
      </header>

      <Card className="min-w-0 gap-0 overflow-hidden rounded-xl py-0 shadow-none">
        <CardContent className="space-y-0 p-0">
          {cfg ? (
            <>
              <EndpointSection
                edition={edition}
                endpoint={cfg[edition]}
                status={epStatus}
                onChange={patchEndpoint}
              />

              <div className="mx-4 flex items-center justify-between gap-3 border-b border-border/50 py-3 sm:mx-5">
                <div className="min-w-0 flex-1">
                  <div className="text-[13px] font-medium leading-4">使用全部账号</div>
                  <p className="mt-0.5 text-xs leading-4 text-muted-foreground/75">
                    关闭后按分组挑选；建议给桌面端留 1~2 个号不参与反代
                  </p>
                </div>
                <Switch
                  id="proxy-all"
                  checked={allAccounts}
                  onCheckedChange={(v) =>
                    patchEndpoint({
                      accounts: v ? [] : editionAccounts.map((a) => a.uid ?? "").filter(Boolean),
                    })
                  }
                  aria-label="使用全部账号"
                />
              </div>

              {!allAccounts ? (
                <div className="mx-4 my-3 space-y-3 sm:mx-5">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-xs text-muted-foreground">
                      参与轮转 <span className="font-medium text-foreground">{ep?.accounts.length ?? 0}</span> / {editionAccounts.length} 个账号
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8"
                      onClick={() =>
                        patchEndpoint({
                          accounts: editionAccounts
                            .filter((a) => a.group === "proxy")
                            .map((a) => a.uid ?? "")
                            .filter(Boolean),
                        })
                      }
                      disabled={!editionAccounts.some((a) => a.group === "proxy")}
                    >
                      <Users className="size-3.5" />
                      只选「反代API」分组
                    </Button>
                  </div>

                  {groups.map((g) => (
                    <div key={g.key} className="overflow-hidden rounded-lg border border-border/60">
                      <div className="flex items-center justify-between gap-2 border-b border-border/40 bg-muted/40 px-3 py-1.5">
                        <span className="text-xs font-medium text-muted-foreground">{g.label}</span>
                        <span className="text-xs text-muted-foreground/60">{g.items.length} 个</span>
                      </div>
                      <div className="divide-y divide-border/40">
                        {g.items.map((a) => {
                          const uid = a.uid ?? "";
                          return (
                            <div key={a.id} className="flex items-center justify-between gap-3 px-3 py-2">
                              <div className="min-w-0 flex-1">
                                <div className="flex items-center gap-1.5">
                                  <span className="truncate text-[13px] leading-4">
                                    {a.nickname || a.email || uid}
                                  </span>
                                  {!g.byEdition ? (
                                    <Badge
                                      variant="secondary"
                                      className="h-4 shrink-0 border-0 px-1 text-[10px] leading-4"
                                    >
                                      {editionLabelOf(a)}
                                    </Badge>
                                  ) : null}
                                  <AccountCooldownChips cooldown={cooldowns.get(uid)} now={now} />
                                </div>
                                {a.needsRelogin ? (
                                  <div className="text-xs text-amber-600">需要重新登录，不会参与</div>
                                ) : null}
                              </div>
                              <Switch
                                checked={(ep?.accounts ?? []).includes(uid)}
                                onCheckedChange={(v) => toggleAccount(uid, v)}
                                disabled={!uid}
                                aria-label="参与反代"
                              />
                            </div>
                          );
                        })}
                        {g.items.length === 0 ? (
                          <div className="px-3 py-2 text-xs text-muted-foreground/70">
                            {g.byEdition
                              ? `账号库里没有这一档的号 —— 它那套模型也拉不到`
                              : "没有账号"}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}

              {/*
                冷却面板：**不依赖「使用全部账号」那个开关**。
                关掉时账号列表里虽然有徽标，但打开「使用全部账号」后整个列表都不渲染，
                冷却状态就会彻底看不见 —— 而那恰恰是最常见的配置。所以单独给一块。
              */}
              {coolingCount > 0 ? (
                <div className="mx-4 my-3 overflow-hidden rounded-lg border border-amber-200/70 bg-amber-50/50 sm:mx-5 dark:border-amber-900/40 dark:bg-amber-950/20">
                  <div className="flex items-center gap-1.5 border-b border-amber-200/60 px-3 py-1.5 text-xs font-medium text-amber-800 dark:border-amber-900/40 dark:text-amber-300">
                    <Clock3 className="size-3.5" />
                    冷却中
                    <span className="font-normal text-amber-700/70 dark:text-amber-400/70">
                      · 到期后自动回到轮转池，无需操作
                    </span>
                  </div>
                  <div className="divide-y divide-amber-200/50 dark:divide-amber-900/30">
                    {Array.from(cooldowns.values()).map((c) => {
                      const accountLeft =
                        c.until != null ? formatRemaining(c.until, now) : null;
                      const models = (c.models ?? []).filter((m) => m.until > now);
                      if (!accountLeft && models.length === 0) return null;
                      const acc = accounts.find((a) => (a.uid ?? "") === c.uid);
                      const name = acc?.nickname || acc?.email || c.uid.slice(0, 8);
                      return (
                        <div key={c.uid} className="px-3 py-2 text-xs">
                          <div className="flex items-baseline justify-between gap-3">
                            <span className="min-w-0 flex-1 truncate font-medium">{name}</span>
                            {accountLeft ? (
                              <span className="shrink-0 tabular-nums text-amber-800 dark:text-amber-300">
                                {accountLeft} 后恢复
                              </span>
                            ) : (
                              <span className="shrink-0 text-muted-foreground">账号可用</span>
                            )}
                          </div>
                          {accountLeft ? (
                            <div className="mt-0.5 text-muted-foreground">
                              {c.reason || "上游失败"} · 整个账号暂停轮转
                            </div>
                          ) : null}
                          {models.length > 0 ? (
                            <div className="mt-1 space-y-0.5">
                              {models.map((m) => (
                                <div
                                  key={m.model}
                                  className="flex items-baseline justify-between gap-3 text-muted-foreground"
                                >
                                  <code className="min-w-0 truncate">{m.model}</code>
                                  <span className="shrink-0 tabular-nums">
                                    {formatRemaining(m.until, now) ?? "即将恢复"}
                                  </span>
                                </div>
                              ))}
                              <div className="text-[11px] text-muted-foreground/70">
                                只有上面这些模型被暂停，换其他模型可继续使用
                              </div>
                            </div>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : null}

              <div className={cn("mx-4 flex items-center justify-between gap-3 py-3 sm:mx-5", allAccounts && "border-t border-border/50")}>
                <div className="min-w-0 flex-1 text-xs leading-5 text-muted-foreground/80">
                  {epStatus?.running ? (
                    <>
                      运行中
                      {coolingCount > 0 ? (
                        <span className="text-amber-700 dark:text-amber-400">
                          {" "}· {coolingCount} 项冷却中
                        </span>
                      ) : null}
                      {" "}· OpenAI 基址{" "}
                      <code className="rounded bg-muted px-1 py-0.5">http://{epStatus.listen}/v1</code>
                      <br />
                      Anthropic 基址{" "}
                      <code className="rounded bg-muted px-1 py-0.5">http://{epStatus.listen}</code>
                      <span className="text-muted-foreground/60">（Claude Code 的 ANTHROPIC_BASE_URL）</span>
                    </>
                  ) : (
                    <>未运行{ep?.enabled ? "（点击保存后启动）" : ""}</>
                  )}
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => void fetchModels()}
                    disabled={loadingModels}
                    title="拉一次可用模型列表，客户端 model 字段照着填"
                  >
                    {loadingModels ? (
                      <Loader2 className="size-3.5 animate-spin" />
                    ) : (
                      <ListPlus className="size-3.5" />
                    )}
                    获取模型ID
                  </Button>
                  <Button size="sm" onClick={() => void save()} disabled={saving}>
                    {saving ? <Loader2 className="size-3.5 animate-spin" /> : <Save className="size-3.5" />}
                    保存
                  </Button>
                </div>
              </div>
            </>
          ) : (
            <div className="px-5 py-6 text-sm text-muted-foreground">加载中…</div>
          )}

          {msg ? (
            <Alert variant={msg.type === "err" ? "destructive" : "default"} className="!w-auto mx-4 my-4 sm:mx-5">
              <AlertDescription>{msg.text}</AlertDescription>
            </Alert>
          ) : null}
        </CardContent>
      </Card>

      <section className="mt-8" aria-labelledby="proxy-usage-title">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <h2 id="proxy-usage-title" className="text-base font-semibold tracking-tight">反代用量</h2>
            <p className="mt-1 text-xs text-muted-foreground">只统计经过本接口的请求，存在本地 proxy-usage.json 里</p>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            <Button
              variant="ghost"
              size="sm"
              className="h-8 px-2.5"
              onClick={() => void load()}
              aria-label="刷新用量"
            >
              <RefreshCw className="size-3.5" />
              刷新
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 px-2.5 text-destructive"
              onClick={() => void onResetUsage()}
              disabled={resettingUsage}
            >
              {resettingUsage ? <Loader2 className="size-3.5 animate-spin" /> : <Trash2 className="size-3.5" />}
              清空
            </Button>
          </div>
        </div>

        {usage ? (
          <>
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <StatCard
                label="总请求"
                value={num(usage.total.requests)}
                hint={usage.total.errors ? `失败 ${num(usage.total.errors)}` : undefined}
              />
              <StatCard label="输入 tokens" value={num(usage.total.promptTokens)} />
              <StatCard label="输出 tokens" value={num(usage.total.completionTokens)} />
              <StatCard label="累计消耗" value={num3(usage.total.credit)} hint="上游返回的 credit" />
            </div>

            {Object.keys(usage.byAccount ?? {}).length > 0 ? (
              <div className="mt-4 overflow-hidden rounded-xl border border-border">
                <div className="flex items-center justify-between gap-3 border-b border-border/50 bg-muted/40 px-3 py-1.5 text-xs text-muted-foreground">
                  <span>按账号</span>
                  <span>请求数 · 输入/输出 tokens</span>
                </div>
                <div className="divide-y divide-border/40">
                  {Object.entries(usage.byAccount).map(([uid, b]) =>
                    bucketRow(b.nickname || uid.slice(0, 8), b),
                  )}
                </div>
              </div>
            ) : null}

            {Object.keys(usage.byModel ?? {}).length > 0 ? (
              <div className="mt-3 overflow-hidden rounded-xl border border-border">
                <div className="flex items-center justify-between gap-3 border-b border-border/50 bg-muted/40 px-3 py-1.5 text-xs text-muted-foreground">
                  <span>按模型</span>
                  <span>请求数 · 输入/输出 tokens</span>
                </div>
                <div className="divide-y divide-border/40">
                  {Object.entries(usage.byModel).map(([m, b]) => bucketRow(m, b))}
                </div>
              </div>
            ) : null}

            {(usage.recent ?? []).length > 0 ? (
              <div className="mt-3 overflow-hidden rounded-xl border border-border">
                <div className="border-b border-border/50 bg-muted/40 px-3 py-1.5 text-xs text-muted-foreground">
                  最近 {Math.min((usage.recent ?? []).length, 8)} 条
                </div>
                <div className="divide-y divide-border/40">
                  {(usage.recent ?? []).slice(0, 8).map((r, i) => (
                    <div key={`${r.ts}-${i}`} className="flex items-center justify-between gap-3 px-3 py-2 text-[13px]">
                      <div className="min-w-0 flex-1">
                        <div className="truncate">
                          {r.nickname || r.uid.slice(0, 8)} · {r.model}
                          {r.stream ? " · 流式" : ""}
                          {r.ok ? "" : " · 失败"}
                        </div>
                        <div className="text-xs text-muted-foreground/70">
                          {new Date(r.ts).toLocaleString("zh-CN")}
                        </div>
                      </div>
                      <span className="shrink-0 tabular-nums text-muted-foreground">
                        {num(r.promptTokens)} / {num(r.completionTokens)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </>
        ) : (
          <div className="rounded-xl border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
            暂无用量数据。调用一次接口后这里就会有记录。
          </div>
        )}
      </section>

      <p className="mt-6 px-1 text-xs leading-5 text-muted-foreground/70">
        提示：如果开了梯子（系统代理），客户端连 127.0.0.1 失败时先设置 <code className="rounded bg-muted px-1">no_proxy=127.0.0.1,localhost</code>。
      </p>

      <Dialog open={modelsOpen} onOpenChange={setModelsOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>可用模型 ID（按版本分开）</DialogTitle>
            <DialogDescription>
              国内版和国际版是两套上游，模型列表不一样，所以分档列。客户端里的{" "}
              <code className="rounded bg-muted px-1">model</code> 字段填下面任意一个 ——
              反代会按这个 ID 自动挑对应版本的账号。
            </DialogDescription>
          </DialogHeader>

          <div className="max-h-[52vh] space-y-3 overflow-y-auto">
            {(models?.editions ?? []).map((g) => (
              <div key={g.edition} className="overflow-hidden rounded-lg border border-border/60">
                <div className="flex items-center justify-between gap-2 border-b border-border/40 bg-muted/40 px-3 py-1.5">
                  <span className="text-xs font-medium text-muted-foreground">
                    {g.label}
                    {g.source ? ` · ${g.source}` : ""}
                  </span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground/60">{g.models.length} 个</span>
                    {g.models.length > 0 ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-6 px-1.5 text-[11px]"
                        onClick={() => void copyText(g.models.join("\n"), `__all__${g.edition}`)}
                      >
                        {copied === `__all__${g.edition}` ? (
                          <Check className="size-3" />
                        ) : (
                          <Copy className="size-3" />
                        )}
                        复制
                      </Button>
                    ) : null}
                  </div>
                </div>
                {g.models.length > 0 ? (
                  <div className="divide-y divide-border/40">
                    {g.models.map((id) => (
                      <div key={`${g.edition}-${id}`} className="flex items-center justify-between gap-3 px-3 py-2">
                        <code className="min-w-0 flex-1 truncate text-[13px]">{id}</code>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 shrink-0 px-2"
                          onClick={() => void copyText(id, `${g.edition}-${id}`)}
                          aria-label={`复制 ${id}`}
                        >
                          {copied === `${g.edition}-${id}` ? (
                            <Check className="size-3.5" />
                          ) : (
                            <Copy className="size-3.5" />
                          )}
                        </Button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="px-3 py-2.5 text-xs leading-5 text-amber-600">
                    {g.error ?? "没拉到"}
                  </div>
                )}
                {g.sourceUrl ? (
                  <div className="border-t border-border/40 px-3 py-1.5 text-[11px] text-muted-foreground/70">
                    数据来自 <code className="break-all">{g.sourceUrl}</code>
                  </div>
                ) : null}
              </div>
            ))}
            {models && (models.editions?.length ?? 0) === 0 ? (
              <div className="px-3 py-2 text-xs text-muted-foreground">没有拉到任何模型。</div>
            ) : null}
          </div>

          <p className="text-xs leading-5 text-muted-foreground">
            列表外的 ID 也能直接发（上游支持就行）。两版都会各问一次上游，所以国际版账号缺失时那一档会标出原因。
          </p>

          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void copyText((models?.models ?? []).join("\n"), "__all__")}
            >
              {copied === "__all__" ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
              复制全部（两版并集）
            </Button>
            <Button size="sm" onClick={() => setModelsOpen(false)}>
              关闭
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
