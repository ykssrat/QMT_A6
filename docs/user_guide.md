# 用户手册

## 目录

1. [快速开始](#1-快速开始)
2. [目录结构](#2-目录结构)
3. [配置说明](#3-配置说明)
4. [运行策略信号](#4-运行策略信号)
5. [运行历史回测](#5-运行历史回测)
6. [读取信号输出](#6-读取信号输出)
7. [审计日志](#7-审计日志)
8. [常见问题](#8-常见问题)

---

## 1. 快速开始

**环境要求**：Python 3.11+，或直接使用 Docker（推荐）。

### 使用 Python 本地运行

```bash
# 1. 克隆项目
git clone <repo_url>
cd QMT_A6

# 2. 安装依赖（TA-Lib 需先安装 C 库，参见 docs/deployment.md）
pip install -r requirements.txt

# 3. 配置标的池（编辑 configs/strategy_config.yaml）
#    在 capital.holdings / capital.watchlist 中填入股票或基金代码

# 4. 运行信号生成
python scripts/strategy/signal_generator.py
```

### 使用 Docker 运行

```bash
docker build -t qmt_a6 .
docker run --rm -v "$(pwd)/datas:/app/datas" qmt_a6
```

---

## 2. 目录结构

```
QMT_A6/
├── configs/
│   ├── data_config.yaml       # 数据源、缓存、费率配置
│   └── strategy_config.yaml   # 策略参数、标的池配置
├── datas/
│   ├── raw/                   # 本地行情缓存（Parquet）
│   └── logs/                  # 审计日志（JSON Lines）
├── scripts/
│   ├── processed/             # 数据拉取与清洗
│   ├── features/              # 技术因子计算
│   ├── strategy/              # 策略决策与信号生成
│   ├── backtest/              # 历史回测引擎
│   ├── portfolio/             # 组合权重优化
│   └── utils/                 # 审计日志等工具
├── tests/
│   ├── unit/                  # 单元测试
│   └── integration/           # 集成测试
└── docs/                      # 文档
```

---

## 3. 配置说明

### 3.1 数据配置 `configs/data_config.yaml`

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `data_source.retry_times` | AkShare 请求失败重试次数 | `3` |
| `storage.raw_dir` | 行情缓存目录 | `datas/raw` |
| `cost_model.stock.commission` | 股票单边佣金率 | `0.0003` |
| `cost_model.stock.stamp_duty` | 印花税（仅卖出） | `0.001` |

### 3.2 策略配置 `configs/strategy_config.yaml`

**Livermore 核心参数**：

| 参数 | 含义 | 建议范围 |
|------|------|---------|
| `livermore.m` | 初始建仓比例 | 0.05 – 0.15 |
| `livermore.c` | 止损/回调阈值 | 0.05 – 0.10 |
| `livermore.h` | 加仓解锁阈值（需 > c） | 0.08 – 0.15 |
| `livermore.k` | 加仓系数 | 0.3 – 0.7 |
| `signal.confidence_threshold` | 信心因子 Z 最低入场阈值 | 1.0 – 2.0 |

**标的池配置**（三类来源，取并集）：

```yaml
capital:
  # 当前实际持有的标的——无论 Z 值高低，每日必须参与计算
  holdings:
    - "000001"    # 平安银行（A 股股票）
    - "510300"    # 沪深300 ETF

  # 自选关注列表——每日扫描，信号仅供参考
  watchlist:
    - "600519"    # 贵州茅台
    - "100032"    # 某场外基金
```

> **基金代码说明**：
> - ETF（场内）：6 位纯数字，如 `510300`
> - LOF（场内）：6 位纯数字，如 `160706`
> - 场外基金：6 位纯数字，如 `100032`（通过 AkShare `fund_open_fund_info_em` 拉取净值）

---

## 4. 运行策略信号

信号生成脚本每次运行输出**当日**交易建议，无需额外参数：

```bash
python scripts/strategy/signal_generator.py
```

也可以在代码中调用：

```python
from scripts.strategy.signal_generator import get_latest_signals, resolve_symbol_pool
from scripts.strategy.livermore import Portfolio

# 自动从配置读取持仓 + 自选标的池
portfolio = Portfolio(cash=100_000.0)
signals = get_latest_signals(
    portfolio=portfolio,
    start_date="2024-01-01",      # 因子预热起始日（建议至少 80 个交易日前）
    signal_date="2024-12-31",     # 默认省略时取当日
)
```

输出示例：

```
2024-12-31 15:30:01 [INFO] ============================================================
2024-12-31 15:30:01 [INFO] [2024-12-31] 共 2 条交易建议：
2024-12-31 15:30:01 [INFO]   1. [BUY] 600519  金额: 10000.00 元  原因: 建仓：信心因子 Z=1.83 >= 阈值 1.5
2024-12-31 15:30:01 [INFO]   2. [SELL] 000001  金额: 5200.00 元  原因: 止损：亏损率 -7.50% >= 7.00%
2024-12-31 15:30:01 [INFO] ============================================================
```

---

## 5. 运行历史回测

```python
from scripts.backtest.engine import run_backtest

result = run_backtest(
    symbols=["000001", "600519", "300750"],
    capital=100_000,
    start_date="2022-01-01",
    end_date="2024-12-31",
    risk_free_rate=0.02,
)

metrics = result["metrics"]
print(f"总收益率:   {metrics['total_return']:.2%}")
print(f"年化收益率: {metrics['annual_return']:.2%}")
print(f"夏普比率:   {metrics['sharpe_ratio']:.2f}")
print(f"最大回撤:   {metrics['max_drawdown']:.2%}")
print(f"年化波动率: {metrics['annual_vol']:.2%}")
print(f"胜率:       {metrics['win_rate']:.2%}")

# 净值曲线（pd.Series，DatetimeIndex）
equity = result["equity_curve"]
equity.plot(title="净值曲线")
```

---

## 6. 读取信号输出

每条信号为字典，字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | `str` | 标的代码 |
| `action` | `str` | `buy`（建仓）/ `sell`（止损平仓）/ `add`（加仓） |
| `amount` | `float` | 建议交易金额（元） |
| `reason` | `str` | 信号触发原因描述 |

**注意**：系统只输出建议，最终下单需手动操作，下单前请核实当前价格与可用资金。

---

## 7. 审计日志

审计日志自动写入 `datas/logs/audit_YYYY-MM-DD.jsonl`，格式为 JSON Lines（每行一条）。

日志类型：

| `event_type` | 触发时机 | 主要字段 |
|---|---|---|
| `signal` | 每条信号生成时 | symbol, action, amount, reason, factors |
| `trade` | 回测每笔成交时 | symbol, action, shares, price, amount, pnl |
| `daily_summary` | 每日收盘汇总 | equity, daily_pnl, signal_count, trade_count |
| `error` | 脚本异常时 | message, traceback |

读取指定日期日志：

```python
from scripts.utils.audit_logger import read_audit_log

records = read_audit_log("2024-12-31")
for r in records:
    print(r["event_type"], r.get("symbol"), r.get("action"))
```

---

## 8. 常见问题

**Q: AkShare 请求超时或限流怎么办？**
A: 增大 `configs/data_config.yaml` 中的 `data_source.retry_times`，或在请求间添加随机延迟。

**Q: 某只标的无数据或列不足 60 个交易日？**
A: 该标的会被自动跳过（`clean_data.py` 过滤），日志中会打印 WARNING 提示。

**Q: 信心因子 Z 一直没有有效值？**
A: 因子计算需要至少 60 个交易日的历史数据预热，`start_date` 应设置在 `signal_date` 之前约 3～4 个月。

**Q: 如何增加新的技术因子？**
A: 在 `scripts/features/calc_features.py` 中新增 `add_xxx(df)` 函数，并在 `build_all_features` 中调用，最后在 `calc_confidence_z` 中按需纳入合成。
