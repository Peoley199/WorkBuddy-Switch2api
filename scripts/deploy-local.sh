#!/bin/bash
# 本地构建 + 部署：确认最新 → 版本号自检 → 构建 → 备份 → 停进程 → 替换 → 重启 → 探活
# 探活按「不比部署前更差」判定，失败自动回滚。
#
# 为什么需要它：手工部署要记住 6 件事（版本号清单、前端 dist 是否过期、备份命名、
# 进程占用导致覆盖失败、重启后哪个端口该在、探活用哪个 key），漏一件就是「换了但没生效」
# 或「换了启动不了」。
#
# 用法：
#   sh scripts/deploy-local.sh                # 全流程（替换前会问一次 y/N）
#   sh scripts/deploy-local.sh --yes          # 全流程，不询问
#   sh scripts/deploy-local.sh --dry-run      # 只做预检 + 构建，不碰运行中的程序
#   sh scripts/deploy-local.sh --no-build     # 跳过构建，直接部署已有的 target/release 产物
#
# 可配置（环境变量）：
#   WB_DEPLOY_DIR   部署目录，默认 D:/WorkBuddy-Switch2api；有进程在跑时以其实际路径为准
#   WB_APP_EXE      进程名，默认 wb-switch-rust.exe
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)

DEPLOY_DIR=${WB_DEPLOY_DIR:-D:/WorkBuddy-Switch2api}
APP_EXE=${WB_APP_EXE:-wb-switch-rust.exe}
BUILT="target/release/$APP_EXE"
CFG="${WB_PROXY_CFG:-$HOME/.wb-switch/proxy.json}"

DO_BUILD=1
DO_DEPLOY=1
YES=0
for a in "$@"; do
  case "$a" in
    --yes|-y) YES=1 ;;
    --dry-run) DO_DEPLOY=0 ;;
    --no-build) DO_BUILD=0 ;;
    *) echo "未知参数：$a"; exit 1 ;;
  esac
done

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()  { printf '\033[32m✓ %s\033[0m\n' "$*"; }
warn(){ printf '\033[33m⚠️  %s\033[0m\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*"; exit 1; }

pids_of() {
  tasklist //FI "IMAGENAME eq $APP_EXE" //FO CSV //NH 2>/dev/null \
    | awk -F'","' 'NF>1 {gsub(/"/,"",$2); print $2}' || true
}

# 探测配置里每个**已启用**入口的 /v1/models。输出每行 "edition<TAB>listen<TAB>code"。
probe_all() {
  [ -f "$CFG" ] || return 0
  local rows
  rows=$(CFG_PATH="$CFG" node -e '
    const c = JSON.parse(require("fs").readFileSync(process.env.CFG_PATH, "utf8"));
    for (const [k, v] of Object.entries(c))
      if (v && v.enabled) console.log([k, v.listen || "", v.api_key || ""].join("\t"));
  ' 2>/dev/null) || return 0
  while IFS=$'\t' read -r ed listen key; do
    [ -z "$listen" ] && continue
    local port=${listen##*:}
    local code
    # ⚠️ 不要写成 `curl ... || echo 000`：curl 失败时自己也会打印 000，
    # 两者拼接会得到 "000000"。用子串截断保住 3 位。
    code=$(curl -s -m 6 -o /dev/null -w "%{http_code}" \
      -H "Authorization: Bearer $key" "http://127.0.0.1:$port/v1/models" 2>/dev/null) || code="000"
    [ -z "$code" ] && code="000"
    code=$(printf '%.3s' "$code")
    printf '%s\t%s\t%s\n' "$ed" "$listen" "$code"
  done <<< "$rows"
}

# ---------------------------------------------------------------- 1. 预检

say "1/7 预检"

if [ -n "$(git status --porcelain | grep -v '^??' || true)" ]; then
  warn "工作区有未提交改动 —— 构建产物与仓库状态不一致，确认这是你想要的"
fi

if node scripts/version.mjs check >/dev/null 2>&1; then
  VER=$(node scripts/version.mjs 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "?")
  ok "版本号各处一致：$VER"
else
  node scripts/version.mjs check || true
  die "版本号不一致（见上）。修好再部署"
fi

if [ -d dist ] && [ -d src ]; then
  NEWER=$(find src -type f -newer dist/index.html 2>/dev/null | head -3 || true)
  if [ -n "$NEWER" ]; then
    die "前端源码比 dist 新，先 npm run build：\n$NEWER"
  fi
  ok "前端 dist 是最新的"
elif [ ! -d dist ]; then
  warn "没有 dist/ —— 纯 Rust 改动可忽略；界面相关改动请先 npm run build"
fi

# ---------------------------------------------------------------- 2. 构建

if [ "$DO_BUILD" = 1 ]; then
  say "2/7 构建 release"
  cargo build --release -p wb-switch-rust 2>&1 | tail -3
else
  say "2/7 跳过构建（--no-build）"
fi

[ -f "$BUILT" ] || die "找不到构建产物：$BUILT"
ok "产物：$BUILT（$(stat -c %s "$BUILT") 字节，$(date -r "$BUILT" '+%Y-%m-%d %H:%M')）"

# 🔴 部署前先验证「产物本身能不能启动」，把坏件挡在部署目录之外。
# 教训（2026-10-30 实测）：截断/损坏的 exe 一旦装进部署目录，下次部署的
# 「备份旧版」备份的就是坏件，回滚只能回到坏件 —— 备份链会被永久污染。
#
# 判据用「PE 头完整 + 能被系统加载」：
#   1. 文件头必须是 MZ（DOS stub），这是 PE 的硬性标志，截断/损坏最先丢的就是这个判据之外的东西；
#   2. 真跑一次 —— 坏 PE 会得到 "Exec format error"，好 PE 至少能走到单实例检测。
PE_SIG=$(head -c 2 "$BUILT" 2>/dev/null || true)
[ "$PE_SIG" = "MZ" ] || die "产物不是有效的 Windows 可执行文件（文件头不是 MZ），不会部署"

SMOKE_OUT=$(timeout 25 "$BUILT" --version 2>&1 || true)
if echo "$SMOKE_OUT" | grep -qiE "exec format error|not a valid|无法执行|不是有效的"; then
  echo "$SMOKE_OUT"
  die "产物无法运行（冒烟测试失败），不会部署。重新构建：cargo build --release -p wb-switch-rust"
fi
ok "产物冒烟测试通过（PE 头有效、可被系统加载）"

# ---------------------------------------------------------------- 3. 定位 + 部署前探活

say "3/7 定位实例 + 部署前探活"

RUN_PIDS=$(pids_of | tr '\n' ' ')
if [ -n "$RUN_PIDS" ]; then
  echo "  运行中 PID：$RUN_PIDS"
  RUN_PATH=$(powershell -NoProfile -Command "(Get-Process -Id $(echo $RUN_PIDS | awk '{print $1}')).Path" 2>/dev/null | tr -d '\r')
  if [ -n "$RUN_PATH" ]; then
    DEPLOY_DIR=$(dirname "$RUN_PATH")
    ok "按运行中进程定位部署目录：$DEPLOY_DIR"
  fi
else
  echo "  当前没有运行中的实例"
fi

[ -d "$DEPLOY_DIR" ] || die "部署目录不存在：$DEPLOY_DIR（用 WB_DEPLOY_DIR 指定）"
TARGET="$DEPLOY_DIR/$APP_EXE"

# 🔴 部署**前**先记录基线：哪些入口现在是好的。
# 后面对比「不能比之前更差」，而不是「全部必须 200」——
# 国际版可能因账号库没有可用号而本来就起不来（历史上长期如此），
# 那不是这次部署的锅，不该触发回滚。
BASELINE=$(probe_all || true)
if [ -n "$BASELINE" ]; then
  echo "  部署前状态："
  while IFS=$'\t' read -r ed listen code; do
    printf '    %-14s %-22s HTTP %s\n' "$ed" "$listen" "$code"
  done <<< "$BASELINE"
else
  warn "读不到 $CFG，跳过部署前探活（部署后只做进程存活检查）"
fi

# ---------------------------------------------------------------- 4. 确认

say "4/7 待执行动作"
cat <<EOF
  构建产物： $BUILT
  部署目标： $TARGET
  将执行：   备份旧 exe → 结束进程 → 覆盖 → 重启 → 探活（不比部署前更差；失败自动回滚）
EOF

if [ "$DO_DEPLOY" = 0 ]; then
  say "--dry-run：到此为止，未改动任何运行中的程序"
  exit 0
fi

if [ "$YES" != 1 ]; then
  printf '\n继续？[y/N] '
  read -r ANS
  case "$ANS" in y|Y) ;; *) echo "已取消"; exit 0 ;; esac
fi

# ---------------------------------------------------------------- 5. 备份 + 替换

say "5/7 备份并替换"

STAMP=$(date +%Y%m%d-%H%M%S)
BACKUP=""
if [ -f "$TARGET" ]; then
  OLD_VER=$(node scripts/version.mjs binary "$TARGET" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
  BACKUP="$TARGET.bak-$(date +%Y-%m-%d)-${OLD_VER:-unknown}"
  [ -f "$BACKUP" ] && BACKUP="$BACKUP-$STAMP"
  cp "$TARGET" "$BACKUP" && ok "旧版已备份：$(basename "$BACKUP")"
fi

if [ -n "$RUN_PIDS" ]; then
  taskkill //IM "$APP_EXE" //F >/dev/null 2>&1 || true
  sleep 2
  [ -z "$(pids_of)" ] || die "进程仍在运行，无法覆盖（请手工关闭后重试）"
  ok "旧进程已停止"
fi

cp "$BUILT" "$TARGET" || die "覆盖失败"

# Windows 上「刚结束的进程 + 杀软扫描」会让目标文件在被写出后短暂不可读/读不全，
# 立刻算 MD5 偶发对不上。重试几秒再判，避免把正常的替换误报成失败。
hash_of() {
  # 只取 32 位十六进制：绕过 md5sum 的 ' *' 前缀与可能的 CRLF 混入
  md5sum "$1" 2>/dev/null | grep -oE '^[0-9a-f]{32}'
}
A=$(hash_of "$BUILT")
B=""
for i in 1 2 3 4 5; do
  B=$(hash_of "$TARGET")
  [ -n "$B" ] && [ "$A" = "$B" ] && break
  sleep 1
done
if [ "$A" = "$B" ] && [ -n "$A" ]; then
  ok "替换完成，MD5 一致：$A"
else
  die "MD5 不一致（构建产物 $A / 部署目标 ${B:-<读不到>}），替换可能被拦截或被占用"
fi

# ---------------------------------------------------------------- 6. 重启

say "6/7 重启"
START_OK=1
REGRESSED=""
AFTER=""
powershell -NoProfile -Command \
  "Start-Process -FilePath '$TARGET' -WorkingDirectory '$DEPLOY_DIR'" >/dev/null 2>&1 || START_OK=0
sleep 8

NEW_PIDS=""
if [ "$START_OK" = 1 ]; then
  NEW_PIDS=$(pids_of | tr '\n' ' ')
fi

# 🔴 启动失败不能直接 die —— 那样坏 exe 会留在部署目录、服务停摆，比不部署更糟。
# 统一走下面的「失败 → 回滚」出口。
if [ "$START_OK" != 1 ] || [ -z "$NEW_PIDS" ]; then
  REGRESSED="\n  - 新版本无法启动（进程没起来）"
  say "7/7 跳过硬探活（进程未启动）"
else
  ok "已启动，PID：$NEW_PIDS"
  say "7/7 探活"
  AFTER=$(probe_all || true)
fi
if [ -n "$AFTER" ]; then
  while IFS=$'\t' read -r ed listen code; do
    [ -z "$ed" ] && continue
    before=$(echo "$BASELINE" | awk -F'\t' -v e="$ed" '$1==e {print $3}' | head -1)
    before=${before:-"（未测）"}
    if [ "$code" = "200" ]; then
      printf '  %-14s %-22s → HTTP 200 ✓\n' "$ed" "$listen"
      # 顺带报喜：部署前不通、部署后通了
      if [ "$before" != "200" ]; then
        printf '                  （部署前是 %s，本次已恢复）\n' "$before"
      fi
    elif [ "$before" = "200" ]; then
      printf '  %-14s %-22s → HTTP %s ✗（部署前是 200，本次回归）\n' "$ed" "$listen" "$code"
      REGRESSED="$REGRESSED\n  - $ed ($listen) 部署前 200 → 部署后 $code"
    else
      printf '  %-14s %-22s → HTTP %s（部署前也不是 200，非本次回归）\n' "$ed" "$listen" "$code"
    fi
  done <<< "$AFTER"
fi

# 🔴 额外硬闸：进程刚起来却连不上**任何**已启用入口，基本等于起坏了 ——
# 即使部署前本来就是坏的（基线全 000），也不能把一次「启动了但不可用」当成成功。
if [ -n "$AFTER" ]; then
  ANY_OK=$(echo "$AFTER" | awk -F'\t' '$3=="200"' | head -1)
  ENABLED_N=$(echo "$AFTER" | grep -c . || true)
  if [ -z "$ANY_OK" ] && [ "${ENABLED_N:-0}" -gt 0 ]; then
    REGRESSED="$REGRESSED\n  - 所有 $ENABLED_N 个已启用入口都不可用（进程起来了但服务没通）"
  fi
fi

if [ -z "$REGRESSED" ]; then
  say "部署完成"
  echo "  回滚方法：taskkill //IM $APP_EXE //F && cp \"$BACKUP\" \"$TARGET\" && 重启"
  exit 0
fi

# 有回归：回滚
say "探活发现回归，自动回滚"
printf "%b\n" "$REGRESSED"
if [ -n "$BACKUP" ] && [ -f "$BACKUP" ]; then
  taskkill //IM "$APP_EXE" //F >/dev/null 2>&1 || true
  sleep 2
  cp "$BACKUP" "$TARGET" && ok "已恢复备份文件：$(basename "$BACKUP")"
  powershell -NoProfile -Command "Start-Process -FilePath '$TARGET' -WorkingDirectory '$DEPLOY_DIR'" >/dev/null 2>&1 || true
  sleep 8

  # 🔴 回滚也必须验证：备份本身可能就是个坏件（例如上一次部署失败后，坏的 exe
  # 留在了部署目录，这一次的「备份旧版」备份的就是它）。不验证就宣布「已回滚」
  # 等于给用户一个更坏的状态：服务停了，且以为已经恢复。
  RB_PIDS=$(pids_of | tr '\n' ' ')
  RB_AFTER=$(probe_all || true)
  RB_OK=0
  if [ -n "$RB_PIDS" ]; then
    if [ -z "$RB_AFTER" ]; then
      RB_OK=1   # 无探活配置时，进程起来即视为回滚成功
    elif echo "$RB_AFTER" | awk -F'\t' '$3=="200"' | grep -q .; then
      RB_OK=1
    fi
  fi

  if [ "$RB_OK" = 1 ]; then
    ok "回滚成功，服务已恢复（PID：$RB_PIDS）"
    echo "  ⚠️ 新版本未能通过验证，已退回上一版本。排查：$DEPLOY_DIR\\stderr.log"
  else
    printf '\033[31m✗ 回滚后服务仍未起来 —— 备份件本身可能就是坏的\033[0m\n'
    echo "    备份：$BACKUP"
    echo "    请手工选择一份可用的历史备份覆盖 $TARGET："
    ls -1 "$DEPLOY_DIR"/"$APP_EXE".bak-* 2>/dev/null | while read -r f; do
      echo "      $(basename "$f")  ($(stat -c %s "$f") 字节, $(date -r "$f" '+%m-%d %H:%M'))"
    done
    echo "    线索：5000000 字节这种明显偏小的文件是截断件，别用。"
  fi
else
  warn "没有可回滚的备份 —— 部署目录当前是新版本，需手工处理"
fi
die "部署未通过验证（见上）"
