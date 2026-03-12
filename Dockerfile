# ─── 构建阶段 ────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# 安装 TA-Lib C 库编译所需的系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make wget ca-certificates \
    && wget -q https://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr \
    && make -j"$(nproc)" \
    && make install \
    && cd /build \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ─── 运行阶段 ────────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# 复制 TA-Lib 共享库
COPY --from=builder /usr/lib/libta_lib* /usr/lib/
COPY --from=builder /usr/include/ta-lib/ /usr/include/ta-lib/

# 复制 Python 依赖
COPY --from=builder /install /usr/local

# 复制项目源码（数据目录通过 Volume 挂载，不打入镜像）
COPY configs/  ./configs/
COPY scripts/  ./scripts/
COPY tests/    ./tests/
COPY conftest.py ./

# 数据目录（行情缓存、审计日志）以外部卷挂载持久化
VOLUME ["/app/datas"]

# 非 root 用户运行，提高安全性
RUN useradd --no-create-home --shell /bin/false appuser \
    && chown -R appuser /app
USER appuser

# 默认入口：每日收盘后触发信号生成（通过 Docker Cron 或宿主机 crontab 调用）
CMD ["python", "scripts/strategy/signal_generator.py"]
