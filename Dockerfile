# syntax=docker/dockerfile:1
#
# wb-switch 容器镜像：只跑 webui + API 反代这条「常驻」通道。
#
# 为什么分三个阶段：
#   1) 前端必须先构建好 —— `wb-switch-server` 用 rust-embed 把 `dist/` 编进二进制，
#      编译那一刻 dist 必须已经存在（`#[folder = "../../dist/"]`）。
#   2) Rust 端只需要 `wb-switch-server` 这个 crate（桌面端 src-tauri 要 GUI 依赖，
#      在 Linux 上编译它得装一整套 webkit2gtk，容器里完全不需要）。
#   3) 运行阶段只带二进制 + CA 证书，镜像小、攻击面小。
#
# 两个基础镜像做成 ARG，方便换国内镜像源，也方便本地在拉不到某个 tag 时换一组配对的
# （⚠️ 构建镜像与运行镜像的发行版**必须配对**：trixie 的 glibc 2.41 编出来的二进制
#   丢进 bookworm（2.36）会跑不起来）。
ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

# ---------------------------------------------------------------------------
# 阶段 1：构建前端
# ---------------------------------------------------------------------------
FROM node:22-bookworm-slim AS frontend
WORKDIR /app

# 先只拷清单，让 npm ci 这一层能被缓存住（源码改动不会让它失效）
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY . .
RUN npm run build

# ---------------------------------------------------------------------------
# 阶段 2：构建后端二进制
# ---------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS backend
WORKDIR /app

# 只需要源码 + 依赖清单，具体拷什么交给 .dockerignore 决定
COPY . .

# 🔴 dist 用阶段 1 的产物覆盖掉（.dockerignore 已排除宿主机的 dist）
COPY --from=frontend /app/dist ./dist

# cargo 的 registry 与 target 都挂成缓存，二次构建快很多。
# `cp` 必须写在同一个 RUN 里 —— cache mount 只在这次 RUN 期间存在。
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p wb-switch-server \
    && cp target/release/wb-switch /usr/local/bin/wb-switch

# ---------------------------------------------------------------------------
# 阶段 3：运行
# ---------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE}

# ca-certificates：rustls 要校验上游证书
# tzdata：签到 / 自动轮换按本地时间算，容器默认 UTC 会差 8 小时
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# 非 root 运行。UID 固定 10000，方便用 bind mount 时对齐属主。
RUN useradd --uid 10000 --create-home --shell /usr/sbin/nologin wbswitch

COPY --from=backend /usr/local/bin/wb-switch /usr/local/bin/wb-switch

# 数据全落这里（accounts.json / proxy.json / proxy-usage.json …），挂卷出来即可持久化
RUN mkdir -p /data && chown -R wbswitch:wbswitch /data
VOLUME ["/data"]

USER wbswitch
WORKDIR /home/wbswitch

# 让 core 把所有数据目录指到 /data/.wb-switch。
# 这就是选 WB_SWITCH_HOME_OVERRIDE 而不是 HOME 的原因：它绕开 dirs::home_dir()
# 的平台差异，一个变量管住全部路径。
ENV WB_SWITCH_HOME_OVERRIDE=/data \
    WB_SWITCH_HOST=0.0.0.0 \
    WB_SWITCH_PORT=57890 \
    TZ=Asia/Shanghai

# 57890 = webui；7863 / 7864 = 国内版 / 国际版 API 反代
# ⚠️ 反代那两个的监听地址在 webui 里配，默认是 127.0.0.1:786X —— 想在容器外访问，
#    要改成 0.0.0.0:786X（见 docs/DOCKER.md）。
EXPOSE 57890 7863 7864

# 用 bash 的 /dev/tcp 探活，省得为了 healthcheck 再装 curl。
# 探的是静态首页（不需要口令），有响应就算活着。
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${WB_SWITCH_PORT}' || exit 1

ENTRYPOINT ["wb-switch"]
CMD ["serve", "--no-open"]
