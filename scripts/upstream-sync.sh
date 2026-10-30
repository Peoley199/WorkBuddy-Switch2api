#!/bin/bash
# 上游同步的机械化前半程：调查 → 备份 → 开分支 → 基线验证 → 生成决策清单。
#
# 设计边界（重要）：
#   本脚本**只做可以机械判断的事**。「哪个提交原样移植、哪个改写、哪个不同步」
#   是仓库级决策，必须人（或 AI）逐条判定并写进 docs/UPSTREAM-SYNC.md ——
#   上游不知道本仓库的独有功能，整块取一侧会悄悄删掉保护清单里的东西。
#
# 用法：
#   sh scripts/upstream-sync.sh              # 完整跑一遍（调查 + 备份 + 开分支 + 基线）
#   sh scripts/upstream-sync.sh --check      # 只调查，不动仓库（随时可跑）
#   sh scripts/upstream-sync.sh --verify     # 移植完之后收尾验证（测试 / clippy 增量 / 记账）
#   sh scripts/upstream-sync.sh --no-branch  # 调查 + 备份，但不开分支（已手工开过）
#
# 跑完会输出：
#   /tmp/wb-upstream/<sha>.diff      每个未同步提交的 stat + 完整 diff（docs/测试/代码分开）
#   /tmp/wb-upstream/SUMMARY.md      给决策用的汇总（含自动风险扫描结果）
#
# 约定（与 docs/UPSTREAM-SYNC.md 一致）：
#   - 同步分支名 sync/upstream-<上游版本>，提交说明带 `Upstream: <repo>@<sha>`
#   - 备份分支 backup/pre-upstream-sync-<YYYYMMDD> + 仓库外 bundle ../_backups/
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
UPSTREAM_REMOTE=${UPSTREAM_REMOTE:-upstream}
UPSTREAM_REPO=${UPSTREAM_REPO:-cv-superding/WorkBuddy-Switch2api}
OUT=${WB_SYNC_OUT:-/tmp/wb-upstream}

MODE=full
case "${1:-}" in
  --check) MODE=check ;;
  --verify) MODE=verify ;;
  --no-branch) MODE=nobranch ;;
  "") ;;
  *) echo "未知参数：$1（可用：--check / --verify / --no-branch）"; exit 1 ;;
esac

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
warn() { printf '\033[33m⚠️  %s\033[0m\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*"; exit 1; }

# ---------------------------------------------------------------- 收尾验证

if [ "$MODE" = verify ]; then
  say "收尾验证"

  if [ -n "$(git status --porcelain)" ]; then
    die "工作区不干净 —— 先把移植改动全部落库再验证（未提交的改动不会被测试覆盖到基线对比里）"
  fi

  LEDGER=docs/UPSTREAM-SYNC.md
  BASE=${WB_VERIFY_BASE:-$(git for-each-ref --sort=-creatordate --format='%(refname:short)' 'refs/heads/backup/pre-upstream-sync-*' | head -1)}
  [ -z "$BASE" ] && die "找不到备份分支作为基线；可用 WB_VERIFY_BASE=<ref> 指定"

  if git diff --quiet "$BASE"..HEAD -- "$LEDGER"; then
    die "账本 $LEDGER 自基线 $BASE 以来没有更新 —— 每次同步必须记录（见仓库约定）"
  else
    echo "✓ 账本已更新（$BASE..HEAD）"
  fi

  # 提交说明必须带 Upstream:
  BAD=$(git log --format='%h %s' "$BASE"..HEAD | while read -r h _; do
    git log -1 --format=%B "$h" | grep -q '^Upstream:' || echo "  $h $(git log -1 --format=%s "$h")"
  done)
  if [ -n "$BAD" ]; then
    warn "以下提交缺少 \`Upstream: …@<sha>\` 说明（文档/杂项提交可忽略）："
    echo "$BAD"
  else
    echo "✓ 所有提交都带 Upstream 说明"
  fi

  say "测试"
  cargo test -p wb-switch-core 2>&1 | grep -E "^test result|^error" | head -3 || true
  cargo test -p wb-switch-core >/dev/null 2>&1 || die "测试未通过"

  say "clippy 增量对比（基线 $BASE vs HEAD）"
  if [ "${WB_SKIP_BASELINE:-0}" = "1" ]; then
    echo "  已按 WB_SKIP_BASELINE=1 跳过"
  else
    TMPWT="$(dirname "$ROOT")/_wb_clippy_base_$$"
    git worktree add -f "$TMPWT" "$BASE" >/dev/null 2>&1 || die "无法创建基线 worktree"
    # 忽略行号：只比较「哪个文件、什么类型的警告」出现了多少次
    norm() { sed 's/:[0-9]*:[0-9]*//' | sort; }
    ( cd "$TMPWT" && cargo clippy -p wb-switch-core --message-format=short 2>&1 \
        | grep "warning:" | grep -v "^warning: \`" | norm ) > /tmp/wb-clippy-base.txt || true
    ( cd "$ROOT" && cargo clippy -p wb-switch-core --message-format=short 2>&1 \
        | grep "warning:" | grep -v "^warning: \`" | norm ) > /tmp/wb-clippy-head.txt || true
    git worktree remove --force "$TMPWT" >/dev/null 2>&1 || rm -rf "$TMPWT"

    NEW=$(comm -13 /tmp/wb-clippy-base.txt /tmp/wb-clippy-head.txt)
    if [ -n "$NEW" ]; then
      die "出现新增 clippy 警告（同步不得引入）：\n$NEW"
    fi
    echo "✓ 无新增 clippy 警告（基线 $(wc -l < /tmp/wb-clippy-base.txt) 条，HEAD $(wc -l < /tmp/wb-clippy-head.txt) 条）"
  fi

  say "结论：可以合并"
  cat <<EOF
  git checkout main
  git merge --no-ff $(git rev-parse --abbrev-ref HEAD)
  # 再用 `-s ours` 记账，让 `git log upstream/main --not main` 归零：
  git merge -s ours upstream/main -m "merge(-s ours)：标记上游 <sha> 已同步（内容已按 docs/UPSTREAM-SYNC.md 手工移植）"
EOF
  exit 0
fi

# ---------------------------------------------------------------- 1. 远端

say "1/6 拉取上游"
git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1 \
  || die "没有 $UPSTREAM_REMOTE remote。先执行：git remote add $UPSTREAM_REMOTE https://github.com/$UPSTREAM_REPO.git && git remote set-url --push $UPSTREAM_REMOTE no-push://$UPSTREAM_REPO"
# fetch 失败（GitHub 抖动 / 断网）不该让整个流程崩掉：用本地已有的 upstream 引用继续，
# 但必须显式警告 —— 基于陈旧引用做出的「已同步」结论可能是错的。
# 15 秒硬超时：GitHub 连不通时 curl 的 connectTimeout 不一定生效，这里用 timeout 兜底，
# 断网时不该让同步流程卡半分钟。
if ! FETCH_OUT=$(timeout 15 git fetch "$UPSTREAM_REMOTE" --tags 2>&1); then
  UPDATED_AT=$(git log -1 --format=%cd --date=iso "$UPSTREAM_REMOTE/main" 2>/dev/null || echo "?")
  warn "拉取上游失败，改用本地已有的 $UPSTREAM_REMOTE/main（最后更新 $UPDATED_AT）"
  echo "     $(echo "$FETCH_OUT" | tail -1)"
  warn "结论可能不是最新；网络恢复后重跑本脚本再决定是否合并"
else
  echo "$FETCH_OUT" | tail -2
fi

UP="$UPSTREAM_REMOTE/main"
BASE=$(git merge-base main "$UP") || die "找不到与上游的共同祖先"
BEHIND=$(git log "$UP" --not main --oneline)
AHEAD=$(git log main --not "$UP" --oneline)

if [ -z "$BEHIND" ]; then
  say "结论：已经同步到最新（$(git log -1 --format=%h "$UP")）"
  echo "上游未同步提交：0；本地领先：$(echo "$AHEAD" | grep -c . || true) 个提交"
  exit 0
fi

mkdir -p "$OUT"
COUNT=$(echo "$BEHIND" | grep -c . || true)
LAST=$(git log -1 --format=%h "$UP")
say "2/6 未同步提交：$COUNT 个（上游 HEAD = $LAST）"
echo "$BEHIND" | sed 's/^/  /'

# ---------------------------------------------------------------- 2. 分提交导出

say "3/6 导出每个提交的差异到 $OUT/"

PROXY=crates/wb-switch-core/src/modules/proxy.rs

while read -r sha rest; do
  [ -z "$sha" ] && continue
  {
    echo "# $sha $rest"
    echo
    echo '## 文件统计'
    git show --stat --format="" "$sha"
    echo
    echo '## 代码差异（.rs）'
    git show "$sha" -- '*.rs'
    echo
    echo '## 其它文件差异（文档/清单/版本号）'
    git show "$sha" -- . ':(exclude)*.rs'
  } > "$OUT/$sha.diff"
done <<< "$BEHIND"

# 关键文件的累计差异：逐提交看过之后，这里看「最终会变成什么样」
git diff "$BASE..$UP" -- "$PROXY" > "$OUT/CUMULATIVE-proxy.diff" || true

# ---------------------------------------------------------------- 3. 自动风险扫描

say "4/6 自动风险扫描"

RISKS="$OUT/RISKS.txt"
: > "$RISKS"

# 风险 1：上游删掉了既有测试的 #[test] 属性（真实发生过：4ea798a 删了 3 个，
# 编译通过但测试静默不运行）。注释掉的属性不会有 diff 噪音，所以扫整段 diff 很可靠。
LOST_TESTS=$(git diff "$BASE..$UP" -- '*.rs' | grep -c '^- *\(#\[test\]\|#\[tokio::test\]\)' || true)
if [ "${LOST_TESTS:-0}" -gt 0 ]; then
  warn "上游删除了 $LOST_TESTS 个测试属性（#[test]）—— 移植时核对，别照搬"
  {
    echo "### 被删除的测试属性"
    git diff "$BASE..$UP" -- '*.rs' | grep -n '^- *\(#\[test\]\|#\[tokio::test\]\)'
    echo
    echo "受影响文件："
    git diff "$BASE..$UP" --stat -- '*.rs' | tail -n +1
  } >> "$RISKS"
fi

# 风险 2：保护清单里的文件被上游改动。这些必须逐项人工对齐，不能整块取。
PROTECTED=(
  package.json                       # build:app* 签名脚本
  src-tauri/tauri.conf.json          # updater pubkey
  Cargo.lock
  package-lock.json
)
for f in "${PROTECTED[@]}"; do
  if ! git diff --quiet "$BASE..$UP" -- "$f"; then
    echo "### 保护项被改动：$f（逐项对齐，勿整块取）" >> "$RISKS"
  fi
done

# 风险 3：本仓库独有功能的锚点是否被上游触碰（出现即需逐项比对）
ANCHORS=(
  "chat_headers"
  "is_channel_block"
  "has_upstream_code"
  "ban_kind"
  "X-Conversation-ID"
  "http_guard"
  "anthropic"
  "11140"
  "11128"
)
for a in "${ANCHORS[@]}"; do
  hits=$(git diff "$BASE..$UP" -- '*.rs' | grep -c "^[+-].*$a" || true)
  if [ "${hits:-0}" -gt 0 ]; then
    echo "### 独有功能锚点被触碰：$a（$hits 处改动）—— 需与本地实现逐项比对" >> "$RISKS"
  fi
done

if [ -s "$RISKS" ]; then
  cat "$RISKS"
else
  echo "  （无自动风险命中）"
fi

# ---------------------------------------------------------------- 4. 汇总

SUMMARY="$OUT/SUMMARY.md"
{
  echo "# 上游同步决策表（生成于 $(date '+%Y-%m-%d %H:%M')）"
  echo
  echo "- 上游：$UPSTREAM_REPO @ \`$LAST\`"
  echo "- 分叉基线：\`$BASE\`"
  echo "- 未同步提交：$COUNT"
  echo
  echo "## 逐提交决策（每行填：原样移植 / 移植+修正 / 不同步 + 理由）"
  echo
  echo "| # | 提交 | 标题 | 结论 | 理由 |"
  echo "|---|---|---|---|---|"
  i=0
  while read -r sha rest; do
    [ -z "$sha" ] && continue
    i=$((i+1))
    echo "| $i | \`$sha\` | $rest | ? | |"
  done <<< "$BEHIND"
  echo
  echo "## 自动风险扫描"
  echo
  if [ -s "$RISKS" ]; then cat "$RISKS"; else echo "（无命中）"; fi
  echo
  echo "## 决策依据（照 docs/UPSTREAM-SYNC.md 的口径）"
  echo
  echo "1. 保护清单里的文件，只接受「语义一致」的改动，不要整块 checkout。"
  echo "2. 本仓库独有功能（账本「本仓库独有」节）不得被覆盖；上游若有同类实现，逐项比对后择优。"
  echo "3. 版本号提交一般不同步（两仓库版本号各自独立）。"
  echo "4. 决策与理由必须写回 docs/UPSTREAM-SYNC.md，提交说明带 \`Upstream: …@<sha>\`。"
} > "$SUMMARY"

say "汇总：$SUMMARY"

if [ "$MODE" = check ]; then
  echo
  echo "--check 模式：未改仓库。决策完成后按提示开分支移植。"
  exit 0
fi

# ---------------------------------------------------------------- 5. 备份

say "5/6 备份"
STAMP=$(date +%Y%m%d)
BRANCH="backup/pre-upstream-sync-$STAMP"
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  echo "  备份分支已存在：$BRANCH（跳过）"
else
  git branch "$BRANCH" && echo "  已建备份分支：$BRANCH"
fi

BUNDLE_DIR=${WB_BACKUP_DIR:-../_backups}
mkdir -p "$BUNDLE_DIR"
BUNDLE="$BUNDLE_DIR/WorkBuddy-Switch2api-$STAMP.bundle"
if [ -f "$BUNDLE" ]; then
  echo "  bundle 已存在：$BUNDLE（跳过）"
else
  git bundle create "$BUNDLE" --all >/dev/null 2>&1 && echo "  已导出 bundle：$BUNDLE"
fi

# ---------------------------------------------------------------- 6. 分支 + 基线

if [ "$MODE" = nobranch ]; then
  say "已按 --no-branch 跳过建分支"
else
  say "6/6 开同步分支 + 基线验证"
  if [ -n "$(git status --porcelain)" ]; then
    warn "工作区不干净 —— 先把未提交改动落库再同步（否则 diff 会互相污染）"
  fi
  SYNC_BRANCH="sync/upstream-$LAST"
  if git show-ref --verify --quiet "refs/heads/$SYNC_BRANCH"; then
    echo "  分支已存在，直接切换：$SYNC_BRANCH"
    git checkout "$SYNC_BRANCH"
  else
    git checkout -b "$SYNC_BRANCH"
  fi
  cargo test -p wb-switch-core 2>&1 | grep -E "^test result|error\[" | head -5 || true
fi

cat <<EOF

────────────────────────────────────────────────────────
下一步（人工决策，脚本到此为止）：
  1. 逐条读 $OUT/<sha>.diff，把结论填进 $SUMMARY
  2. 按结论逐项移植，每个逻辑单元单独提交，说明带 `Upstream: …@<sha>`
     - 能用 cherry-pick 且无冲突的：git cherry-pick <sha> && git commit --amend（补 Upstream 行）
     - 需要改写的：手工编辑，别整块 checkout
     - 不要移植版本号提交
  3. 更新 docs/UPSTREAM-SYNC.md 的「已同步」与「本仓库独有」两节
  4. sh scripts/upstream-sync.sh --verify     ← 收尾验证（测试 + clippy 对比 + 记账）

基线数据（收尾时对比用）：
  git log -1 --format=%h   # 记下开分支时的 HEAD
  clippy 警告数：$(cd "$ROOT" && cargo clippy -p wb-switch-core --message-format=short 2>&1 | grep -c "warning:" || true)
────────────────────────────────────────────────────────
EOF
