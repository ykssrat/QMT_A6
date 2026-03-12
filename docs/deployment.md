# 部署指南

## 目录

1. [系统要求](#1-系统要求)
2. [本地 Python 环境部署](#2-本地-python-环境部署)
3. [Docker 容器部署（推荐）](#3-docker-容器部署推荐)
4. [定时任务配置](#4-定时任务配置)
5. [环境变量与敏感配置](#5-环境变量与敏感配置)
6. [监控与告警](#6-监控与告警)
7. [数据持久化与备份](#7-数据持久化与备份)
8. [升级与回滚](#8-升级与回滚)

---

## 1. 系统要求

| 项目 | 最低要求 |
|------|---------|
| 操作系统 | Linux（推荐 Ubuntu 22.04） / macOS / Windows 10+ |
| Python | 3.11+ |
| 内存 | 2 GB（运行时峰值约 500 MB） |
| 磁盘 | 5 GB（含行情缓存） |
| 网络 | 可访问 AkShare 数据源（每日约 50 MB 流量） |

---

## 2. 本地 Python 环境部署

### 2.1 安装 TA-Lib C 库

TA-Lib Python 包依赖底层 C 库，需先安装：

**Linux（Ubuntu/Debian）**：

```bash
sudo apt-get install -y gcc g++ make wget
wget https://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib && ./configure --prefix=/usr && make && sudo make install
```

**macOS（Homebrew）**：

```bash
brew install ta-lib
```

**Windows**：

从 [ta-lib 预编译包](https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib) 下载对应版本的 `.whl` 文件后：

```powershell
pip install TA_Lib-0.4.28-cp311-cp311-win_amd64.whl
```

### 2.2 安装 Python 依赖

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 2.3 验证安装

```bash
python -m pytest tests/unit/ -v
```

全部单元测试通过即表示环境正常。

---

## 3. Docker 容器部署（推荐）

Docker 方式无需手动安装 TA-Lib，镜像内已完成编译。

### 3.1 构建镜像

```bash
# 首次构建约需 5 – 10 分钟（编译 TA-Lib）
docker build -t qmt_a6:latest .
```

### 3.2 运行容器

```bash
# 将宿主机 datas/ 目录挂载为数据卷，实现行情缓存与日志持久化
docker run --rm \
    -v "$(pwd)/datas:/app/datas" \
    -v "$(pwd)/configs:/app/configs" \
    qmt_a6:latest
```

> **说明**：挂载 `configs/` 卷可在不重新构建镜像的情况下修改标的池和策略参数。

### 3.3 多次运行（缓存复用）

行情数据缓存在 `datas/raw/`，只要 Volume 挂载不变，后续运行会跳过已缓存的日期，显著加快启动速度。

---

## 4. 定时任务配置

A 股收盘时间为 15:00，建议在 **15:30** 之后触发数据更新与信号生成。

### 4.1 Linux / macOS（crontab）

```bash
crontab -e
```

添加以下行（每个工作日 15:30 执行）：

```cron
30 15 * * 1-5 docker run --rm \
    -v /path/to/qmt_a6/datas:/app/datas \
    -v /path/to/qmt_a6/configs:/app/configs \
    qmt_a6:latest >> /path/to/qmt_a6/datas/logs/cron.log 2>&1
```

### 4.2 Windows（任务计划程序）

使用 PowerShell 创建计划任务（以管理员身份运行）：

```powershell
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "15:30"
$action  = New-ScheduledTaskAction -Execute "docker" `
    -Argument 'run --rm -v "D:\QMT\QMT_A6\datas:/app/datas" -v "D:\QMT\QMT_A6\configs:/app/configs" qmt_a6:latest'
Register-ScheduledTask -TaskName "QMT_A6_DailySignal" -Trigger $trigger -Action $action -RunLevel Highest
```

---

## 5. 环境变量与敏感配置

所有凭证**不得写入代码或配置文件**，通过环境变量传入：

| 变量名 | 用途 | 示例 |
|--------|------|------|
| `ALERT_EMAIL` | 告警收件邮箱 | `user@example.com` |
| `ALERT_DINGTALK_WEBHOOK` | 钉钉群机器人 Webhook URL | `https://oapi.dingtalk.com/robot/send?access_token=xxx` |

Docker 运行时传入环境变量：

```bash
docker run --rm \
    -e ALERT_EMAIL="user@example.com" \
    -e ALERT_DINGTALK_WEBHOOK="https://..." \
    -v "$(pwd)/datas:/app/datas" \
    -v "$(pwd)/configs:/app/configs" \
    qmt_a6:latest
```

---

## 6. 监控与告警

### 6.1 审计日志

运行后日志自动写入 `datas/logs/audit_YYYY-MM-DD.jsonl`，包含：
- 每条信号（因子快照 + 触发原因）
- 每笔成交（价格 / 金额 / 盈亏）
- 每日资产快照
- 脚本异常（含完整 traceback）

### 6.2 异常监控

脚本运行过程中遇到未捕获异常时，会：
1. 将完整 traceback 写入当日 `audit_*.jsonl`（`event_type: error`）
2. 打印 ERROR 级别日志到 stdout（容器日志可通过 `docker logs` 查看）

建议配合 Linux `logwatch` 或云端日志服务（如 Grafana Loki）监控 `cron.log` 输出。

### 6.3 合规检测

`signal_generator.py` 运行后，可按以下规则手动核查：
- **单日信号频率**：`audit_*.jsonl` 中 `event_type=signal` 的条数不超过 10 条
- **单标的集中度**：单只标的建议金额 / 总资产 ≤ `capital.max_position_count` 的倒数

---

## 7. 数据持久化与备份

| 目录 | 内容 | 备份建议 |
|------|------|---------|
| `datas/raw/` | 行情 Parquet 缓存 | 可重新从 AkShare 获取，不须高频备份 |
| `datas/logs/` | 审计日志 | **每日备份**，合规要求保留至少 1 年 |
| `configs/` | 策略与数据配置 | 纳入 Git 版本控制（不含密钥） |

建议将 `datas/logs/` 同步到对象存储（OSS / S3）：

```bash
# 示例：rsync 到远端服务器
rsync -avz datas/logs/ user@backup-server:/backup/qmt_a6/logs/
```

---

## 8. 升级与回滚

### 升级步骤

```bash
git pull origin main
docker build -t qmt_a6:latest .
# 新镜像构建成功后再停止旧容器
```

### 回滚

```bash
# 查看历史镜像标签
docker images qmt_a6

# 回到指定版本
git checkout <tag_or_commit>
docker build -t qmt_a6:rollback .
docker run --rm -v "$(pwd)/datas:/app/datas" qmt_a6:rollback
```

建议每次发布前打 Git Tag（如 `v1.0.0`），便于快速定位可回滚版本。
