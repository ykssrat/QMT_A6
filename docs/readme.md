# 项目概述

基于 Python + AkShare 的 A 股量化投资系统，面向单人使用。

| 属性 | 说明 |
|------|------|
| 资金规模 | 1k – 100 万人民币 |
| 资产范围 | 沪深 A 股股票、公募基金（ETF、LOF、场外基金） |
| 交易频率 | 日线级（非高频） |
| 执行方式 | 仅输出建议，手动下单 |
| 核心策略 | Livermore 原则：**浮亏不加仓、盈利加仓** |
| 绩效指标 | 收益率、夏普比率、最大回撤、波动率、胜率 |
| 合规框架 | 遵守证监会及沪深交易所程序化交易规定 |

# 系统架构

各模块职责及数据流：

```mermaid
flowchart LR
    数据源[AkShare] --> 数据清洗
    数据清洗 --> 因子计算
    因子计算 --> 策略决策
    策略决策 --> 回测引擎
    回测引擎 --> 绩效分析
    策略决策 --> 组合管理
    组合管理 --> 投资建议输出
```

| 模块 | 职责 |
|------|------|
| 数据接入 | AkShare 拉取沪深 A 股日线行情及基金净值（ETF / LOF / 场外），覆盖**用户持仓 + 用户自选 +（可选）市场扫描候选**三类标的，支持重试与本地缓存 |
| 数据清洗 | 停牌填充、复权、异常值过滤、交易日对齐 |
| 因子计算 | MA/EMA/MACD/RSI/布林带/动量/量比，合成信心因子 Z |
| 策略决策 | Livermore 建仓 / 止损 / 加仓规则，输出交易信号 |
| 组合管理 | 多标的权重优化（等权 / 风险平价） |
| 回测引擎 | 模拟历史交易，计算净值曲线与绩效指标 |
| 结果输出 | 生成交易建议与绩效报告 |

# 技术选型

| 分类 | 选用方案 | 说明 |
|------|----------|------|
| 语言 | Python | 量化生态成熟（Pandas / NumPy / TA-Lib） |
| 数据接口 | AkShare | 免费、覆盖 A 股与基金，调用有频率限制 |
| 数据存储 | Parquet + YAML + JSONL | 行情缓存使用 Parquet，策略配置使用 YAML，审计日志按 JSONL 追加写入 |
| 机器学习 | Scikit-Learn / LightGBM | 辅助预测趋势，按需引入 |
| 容器化 | Docker | 保证环境一致性，支持 Cron 定时任务 |
| CI/CD | GitHub Actions | checkout → 安装依赖 → pytest → flake8 |
| 敏感配置 | 环境变量 | 凭证不入代码仓库 |

# 数据规范

**数据来源**：仅使用 AkShare 接口，覆盖沪深 A 股日线行情与公募基金净值（ETF、LOF、场外基金）。

**标的池来源**（三类取并集）：

| 来源 | 说明 | 配置位置 |
|------|------|----------|
| 模型候选 | 运行信号脚本时可选开启市场优选（活跃 ETF + 沪深300 先筛候选，再按利弗莫尔历史表现只推荐 1 个代码） | 运行参数 `--market-scan` + `strategy_config.yaml` → `signal.*` |
| 用户持仓 | 当前实际持有的股票或基金，每日必须纳入计算 | `strategy_config.yaml` → `capital.holdings` |
| 用户自选 | 手动维护的关注列表，无论是否满足 Z 阈值均参与计算 | `strategy_config.yaml` → `capital.watchlist` |

**配置补充（当前实现）**：
- `capital.current_positions`：真实持仓明细（成本价、份额、峰值、资产类型）
- `capital.watchlist_metadata`：自选标的元信息（名称、资产类型），用于识别股票 / ETF / 场外基金
- `livermore.y_threshold`：Y 因子阈值（决定是否触发转仓卖出）
- `signal.scan_etf_top_n` / `signal.scan_stock_top_n` / `signal.scan_eval_days`：市场优选推荐参数

**基金类型说明**：

| 类型 | 交易方式 | AkShare 接口 |
|------|----------|--------------|
| ETF | 场内实时撮合，使用日线行情价格 | `fund_etf_hist_em` |
| LOF | 场内可交易，亦可通过基金公司申赎 | `fund_lof_hist_em` |
| 场外基金 | 按日净值申购/赎回，T+1 确认 | `fund_open_fund_info_em` |

**清洗规则**：
- 停牌缺失：前值填充（ffill）
- 复权方式：前复权（qfq）
- 异常值：单日涨跌幅超过阈值的行标记后前填充
- 有效性：不足 60 个交易日的标的直接过滤

**回测费率模型**：

| 费用类型 | 数值 |
|----------|------|
| 股票佣金（双边） | 0.03% |
| 滑点 | 0.02% |
| 印花税（卖出） | 0.1% |
| 基金申购费 | 1.5% |

# Livermore 策略规则

**参数说明**：

| 参数 | 含义 | 配置键 |
|------|------|--------|
| *m* | 初始建仓比例（占总资金） | `livermore.m` |
| *c* | 止损 / 回调阈值 | `livermore.c` |
| *h* | 加仓解锁盈利阈值 | `livermore.h` |
| *k* | 加仓系数，$a = k \times r$ | `livermore.k` |
| *Z* | 信心因子（多因子合成） | `signal.confidence_threshold` |

**决策流程**：

```mermaid
flowchart TD
    Z["信心因子 Z ≥ 阈值?"] -->|否| 空仓
    Z -->|是| 建仓["建仓（m 比例）"]
    建仓 --> 止损{"亏损率 ≥ c?"}
    止损 -->|是| 止损平仓
    止损 -->|否| 盈利{"盈利率 r ≥ h?"}
    盈利 -->|否| 持仓
    盈利 -->|是| 回调{"回调幅度 ≤ c?"}
    回调 -->|是| 加仓["加仓（a = k×r）"]
    回调 -->|否| 持仓
```

**资金不足时（Y 因子）**：
- 先由全市场信号（confidence_z）合成 Y 因子
- 当 $Y \ge y\_threshold$：卖出当前最差持仓 1 只进行转仓
- 当 $Y < y\_threshold$：不强制卖出补齐，仅使用当前现金（有多少用多少）

**同日卖出信号去重（当前实现）**：止损优先于 Y 因子卖出；若某标的当日已触发止损，不会再重复生成 Y 因子卖出信号。

**绩效指标**：回测输出以下五大指标，基准为沪深 300（000300）。

| 指标 | 定义 |
|------|------|
| 总收益率 | $(V_{end} - V_{start}) / V_{start}$ |
| 年化收益率 | 按 252 交易日折算的复利收益率 |
| 夏普比率 | $(R_{annualized} - R_f) / \sigma_{annualized}$ |
| 最大回撤 | 净值曲线峰值到谷值的最大跌幅 |
| 年化波动率 | 日收益率标准差 × $\sqrt{252}$ |
| 胜率 | 回测期间盈利平仓笔数 / 总平仓笔数，$W = N_{win} / N_{total}$ |

**参数优化目标（当前实现）**：
- 自动调优只优化三项目标：收益率、夏普比率、胜率
- 评分函数：$score = total\_return + sharpe\_ratio + win\_rate$

# 因子体系

| 类别 | 因子 |
|------|------|
| 均线 | MA(5/10/20/60)、EMA(12/26) |
| 趋势 | MACD(DIF/DEA/柱)、布林带(20,2σ) |
| 动量 | ROC、N 日收益率(5/10/20) |
| 量价 | 成交量均线、量比 |
| 综合 | 信心因子 Z（上述因子滚动标准化等权合成） |

# 开发规范

**分支模型**：GitHub Flow — `main` 保持可部署，功能在 `feature/*` 分支开发，发布打 Tag。

**测试要求**：
- 单元测试（pytest）：覆盖因子计算、策略决策、回测逻辑的边界与异常情况
- 集成测试：用标准测试数据集做端到端回测验证
- 代码检查：flake8 静态检查，所有测试通过后方可合并

**可复现性**：固定随机种子，行情数据与参数纳入版本控制。

# 部署与运维

- **运行环境**：Docker 容器，每日收盘后 Cron 触发数据更新
- **监控**：记录每日回测损益、脚本异常，邮件 / 钉钉告警
- **合规**：全量审计日志（时间戳、信号类型、因子值、价格、金额），只追加写入，支持事后回溯

# 快速运行

以下命令均在项目根目录执行（Windows PowerShell）：

```powershell
# 1) 安装依赖（首次）
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2) 生成今日交易建议（仅持仓+自选）
.\.venv\Scripts\python.exe scripts\strategy\signal_generator.py

# 3) 生成今日交易建议（开启市场优选，仅推荐 1 个新机会代码）
.\.venv\Scripts\python.exe scripts\strategy\signal_generator.py --market-scan

# 4) 运行历史回测并输出绩效报告
.\.venv\Scripts\python.exe scripts\backtest\run_backtest_report.py

# 5) 运行单元测试
.\.venv\Scripts\python.exe -m pytest tests\unit -q

# 6) 参数自动调优（网格搜索 m/c/h/k/z/y）
.\.venv\Scripts\python.exe scripts\backtest\auto_tune_params.py

# 7) 参数自动调优并写回配置
.\.venv\Scripts\python.exe scripts\backtest\auto_tune_params.py --apply
```

运行结果说明：
- 建议输出：终端按 `[BUY] / [ADD] / [SELL]` 展示代码、金额与触发原因；开启 `--market-scan` 时会额外打印 1 个优选推荐代码
- 回测输出：结束日自动取最近交易日；除组合总指标外，还会输出每个代码的单标的收益率/夏普/胜率与已实现盈亏

# 合规要点

遵守证监会及沪深交易所量化交易规定，系统自动检测以下异常并发出警告：
- 单日建议频率过高
- 单标的资金集中度超限

# 里程碑

| # | 阶段 | 交付物 | 验收标准 |
|---|------|--------|----------|
| 1 | 需求与设计 | 系统设计文档 | 设计评审通过 |
| 2 | 数据与环境 | 清洗脚本、回测环境配置 | 示例数据成功清洗并可回测 |
| 3 | 核心策略 | Livermore 策略代码 | 历史回测符合预期收益/风险指标 |
| 4 | 辅助模型与优化 | 组合优化模块 | 回测结果优于基准 |
| 5 | 测试与 CI | 单元/集成测试、CI 配置 | 全部测试通过，流程自动化 |
| 6 | 文档与部署 | 用户手册、部署指南 | 系统稳定运行，无未文档功能 |

# 附录

## 目录结构

```
QMT_A6/
├── docs/               # 文档
├── configs/            # 配置文件（data_config.yaml、strategy_config.yaml）
├── datas/raw/          # 原始行情数据缓存
├── scripts/
│   ├── processed/      # 数据获取与清洗（fetch_data.py、clean_data.py）
│   ├── features/       # 因子计算（calc_features.py）
│   ├── strategy/       # 策略决策（livermore.py、signal_generator.py）
│   ├── backtest/       # 回测引擎
│   ├── portfolio/      # 组合优化
│   └── utils/          # 通用工具（asset_loader.py、market_scanner.py、audit_logger.py）
└── tests/
    ├── unit/           # 单元测试
    └── integration/    # 集成测试
```

## 关键接口

```python
# 数据层
fetch_stock_price(symbol, start_date, end_date, adjust="qfq") -> DataFrame
fetch_fund_nav(fund_code, start_date, end_date) -> DataFrame
fetch_trade_calendar(start_date, end_date) -> list[str]
get_latest_trade_date(ref_date=None) -> str

# 因子层
build_all_features(df) -> DataFrame  # 追加所有因子列

# 策略层
LivermoreStrategy().generate_signals(portfolio, prices, confidence_scores) -> list[dict]
get_latest_signals(portfolio, start_date, symbols=None, signal_date=None, market_scan=False) -> list[dict]

# 市场扫描层
recommend_best_candidate(exclude_symbols=None, etf_top_n=8, stock_top_n=8, eval_days=365) -> dict | None

# 回测层
run_backtest(symbols, capital, start_date, end_date) -> {
    "equity_curve": pd.Series,        # 每日净值曲线
    "metrics": {                       # 绩效指标
        "total_return": float,
        "annual_return": float,
        "sharpe_ratio": float,
        "max_drawdown": float,
        "annual_vol": float,
        "win_rate": float,             # 胜率 = 盈利平仓笔数 / 总平仓笔数
    },
    "trade_log": list[dict],           # 逐笔交易记录
}
```

## 参考链接

**合规**
- [上海证券交易所（SSE）](http://www.sse.com.cn)
- [深圳证券交易所（SZSE）](http://www.szse.cn)

**数据接口**
- [AkShare](https://akshare.akfamily.xyz) — 本系统指定数据接口

**Python 库**
- [Pandas](https://pandas.pydata.org)
- [NumPy](https://numpy.org)
- [Scikit-Learn](https://scikit-learn.org)
- [LightGBM](https://lightgbm.readthedocs.io)

**DevOps**
- [GitHub Actions](https://docs.github.com/actions)
- [Docker](https://www.docker.com)

**金融理论**
- [Sharpe Ratio（夏普比率）](https://en.wikipedia.org/wiki/Sharpe_ratio)
- [Modern Portfolio Theory（马科维茨组合理论）](https://en.wikipedia.org/wiki/Modern_portfolio_theory)
- [Risk Parity（风险平价）](https://en.wikipedia.org/wiki/Risk_parity)
