"""
回测引擎：逐日模拟 Livermore 策略的历史交易，输出净值曲线与绩效指标。

对外主接口 run_backtest() 与 docs/readme.md 关键接口定义保持一致。
"""

import logging
import os
import sys

import numpy as np
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.processed.fetch_data import fetch_trade_calendar
from scripts.features.calc_features import build_all_features
from scripts.strategy.livermore import LivermoreStrategy, Portfolio, Position
from scripts.utils.asset_loader import build_asset_metadata, fetch_asset_history

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 交易成本参数（与 configs/data_config.yaml 中 cost_model 保持一致）
_COMMISSION = 0.0003   # 单边佣金率
_SLIPPAGE   = 0.0002   # 滑点
_STAMP_DUTY = 0.001    # 印花税（仅卖出）
_LOT_SIZE   = 100      # A 股最小交易单位（股/手）


# ────────────────── 信号执行 ──────────────────

def execute_signals(
    signals: list[dict],
    portfolio: Portfolio,
    prices: dict[str, float],
) -> list[dict]:
    """
    按「先卖后买」顺序执行信号列表，更新组合状态。

    参数：
        signals:   由 LivermoreStrategy.generate_signals() 返回的信号列表
        portfolio: 当前组合（原地修改）
        prices:    {symbol: 当日收盘价}

    返回：
        实际成交记录列表，每条含 symbol/action/shares/price/amount/reason
    """
    sells = [s for s in signals if s["action"] == "sell"]
    buys  = [s for s in signals if s["action"] in ("buy", "add")]
    trade_log: list[dict] = []

    for sig in sells + buys:
        sym    = sig["symbol"]
        action = sig["action"]
        amount = sig["amount"]
        price  = prices.get(sym)
        if not price or price <= 0:
            continue

        if action == "sell":
            if sym not in portfolio.positions:
                continue
            pos = portfolio.positions[sym]
            exec_price = price * (1 - _SLIPPAGE)
            proceeds   = pos.shares * exec_price * (1 - _COMMISSION - _STAMP_DUTY)
            cost_basis = pos.cost_price * pos.shares
            portfolio.cash += proceeds
            trade_log.append({
                "symbol": sym, "action": "sell",
                "shares": pos.shares, "price": round(exec_price, 4),
                "amount": round(proceeds, 2), "reason": sig["reason"],
                "pnl": round(proceeds - cost_basis, 2),  # 该笔平仓盈亏（元）
            })
            del portfolio.positions[sym]

        elif action in ("buy", "add"):
            exec_price  = price * (1 + _SLIPPAGE)
            # 整手计算目标股数
            target_shares = int(amount / exec_price / _LOT_SIZE) * _LOT_SIZE
            if target_shares <= 0:
                continue
            cost = target_shares * exec_price * (1 + _COMMISSION)
            # 资金不足时按实际可买量修正
            if cost > portfolio.cash:
                target_shares = int(
                    portfolio.cash / exec_price / (1 + _COMMISSION) / _LOT_SIZE
                ) * _LOT_SIZE
                if target_shares <= 0:
                    continue
                cost = target_shares * exec_price * (1 + _COMMISSION)

            portfolio.cash -= cost

            if action == "buy" or sym not in portfolio.positions:
                portfolio.positions[sym] = Position(
                    symbol=sym,
                    cost_price=exec_price,
                    shares=target_shares,
                    peak_price=exec_price,
                )
            else:
                # 加仓：加权平均更新持仓均价
                pos = portfolio.positions[sym]
                total_cost   = pos.cost_price * pos.shares + exec_price * target_shares
                pos.shares  += target_shares
                pos.cost_price = total_cost / pos.shares

            trade_log.append({
                "symbol": sym, "action": action,
                "shares": target_shares, "price": round(exec_price, 4),
                "amount": round(cost, 2), "reason": sig["reason"],
            })

    return trade_log


# ────────────────── 绩效计算 ──────────────────

def calc_metrics(equity: pd.Series, risk_free_rate: float = 0.02, trade_log: list[dict] | None = None) -> dict:
    """
    计算回测绩效指标。

    参数：
        equity:          DatetimeIndex 索引的每日总资产序列
        risk_free_rate:  年化无风险利率
        trade_log:       逐笔交易记录（用于计算胜率）

    返回：
        包含 total_return / annual_return / sharpe_ratio / max_drawdown / annual_vol / win_rate 的字典
    """
    if equity.empty or len(equity) < 2:
        return {}

    trading_days_per_year = 252
    daily_returns = equity.pct_change().dropna()
    total_days    = len(equity)

    total_return  = float(equity.iloc[-1] / equity.iloc[0]) - 1
    annual_return = float((1 + total_return) ** (trading_days_per_year / total_days) - 1)
    annual_vol    = float(daily_returns.std() * np.sqrt(trading_days_per_year))

    daily_rf = risk_free_rate / trading_days_per_year
    sharpe   = float(
        (daily_returns.mean() - daily_rf) / daily_returns.std() * np.sqrt(trading_days_per_year)
        if daily_returns.std() > 0 else 0.0
    )

    rolling_max  = equity.cummax()
    max_drawdown = float(((equity - rolling_max) / rolling_max).min())

    # 胜率：盈利平仓笔数 / 总平仓笔数
    win_rate = 0.0
    if trade_log:
        sell_records = [t for t in trade_log if t["action"] == "sell"]
        if sell_records:
            win_count = sum(1 for t in sell_records if t.get("pnl", 0) > 0)
            win_rate  = round(win_count / len(sell_records), 4)

    return {
        "total_return":  round(total_return,  4),
        "annual_return": round(annual_return, 4),
        "sharpe_ratio":  round(sharpe,        4),
        "max_drawdown":  round(max_drawdown,  4),
        "annual_vol":    round(annual_vol,    4),
        "win_rate":      win_rate,
    }


# ────────────────── 主接口 ──────────────────

def run_backtest(
    symbols: list[str],
    capital: float,
    start_date: str,
    end_date: str,
    risk_free_rate: float = 0.02,
) -> dict:
    """
    运行 Livermore 策略历史回测。

    参数：
        symbols:        待回测标的代码列表（A 股代码，如 "000001"）
        capital:        初始资金（元）
        start_date:     回测开始日期，格式 "YYYY-MM-DD"
        end_date:       回测结束日期，格式 "YYYY-MM-DD"
        risk_free_rate: 年化无风险利率，用于夏普比率计算

    返回：
        {
            "equity_curve": pd.Series,   # DatetimeIndex，每日总资产
            "metrics":      dict,        # total_return / annual_return / sharpe_ratio / max_drawdown / annual_vol / win_rate
            "trade_log":    list[dict],  # 逐笔成交记录
        }
    """
    if not symbols:
        raise ValueError("symbols 不能为空")
    if capital <= 0:
        raise ValueError("capital 必须大于 0")

    # ── 1. 拉取并预处理特征数据（含预热期） ──
    trade_dates = fetch_trade_calendar(start_date, end_date)
    if not trade_dates:
        raise ValueError(f"日期范围 {start_date} ~ {end_date} 内无可用交易日")

    # 因子计算最少需要 80 个交易日预热
    all_dates     = fetch_trade_calendar("2010-01-01", end_date)
    backtest_idx  = len(all_dates) - len(trade_dates)
    warmup_start  = all_dates[max(0, backtest_idx - 80)]

    asset_meta = build_asset_metadata()
    warmup_trade_dates = fetch_trade_calendar(warmup_start, end_date)
    features_map: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        logger.info("准备特征数据：%s", sym)
        cleaned = fetch_asset_history(
            symbol=sym,
            start_date=warmup_start,
            end_date=end_date,
            trade_dates=warmup_trade_dates,
            asset_meta=asset_meta,
        )
        if cleaned is None:
            logger.warning("标的 %s 数据不足，已跳过", sym)
            continue
        features_map[sym] = build_all_features(cleaned)

    if not features_map:
        raise ValueError("没有可用的标的数据，请检查 symbols 和日期范围")

    # ── 2. 初始化组合与策略 ──
    portfolio     = Portfolio(cash=capital)
    strategy      = LivermoreStrategy()
    equity_list:  list[tuple] = []
    all_trades:   list[dict]  = []

    # ── 3. 逐日模拟 ──
    for date_str in trade_dates:
        date = pd.Timestamp(date_str)

        prices:     dict[str, float] = {}
        confidence: dict[str, float] = {}
        for sym, df in features_map.items():
            if date not in df.index:
                continue
            row = df.loc[date]
            prices[sym] = float(row["close"])
            z = row.get("confidence_z", float("nan"))
            confidence[sym] = float(z) if not pd.isna(z) else 0.0

        # 当日无行情时（停牌等），仅记录净值
        if not prices:
            nav = portfolio.cash + sum(
                p.shares * p.cost_price for p in portfolio.positions.values()
            )
            equity_list.append((date, nav))
            continue

        # 生成信号 → 执行交易
        signals = strategy.generate_signals(portfolio, prices, confidence)
        if signals:
            day_trades = execute_signals(signals, portfolio, prices)
            for t in day_trades:
                t["date"] = date_str
            all_trades.extend(day_trades)

        # 记录收盘后总资产
        nav = portfolio.cash + sum(
            pos.shares * prices.get(sym, pos.cost_price)
            for sym, pos in portfolio.positions.items()
        )
        equity_list.append((date, nav))

    # ── 4. 汇总结果 ──
    equity_curve = pd.Series(
        {d: v for d, v in equity_list},
        name="equity",
        dtype=float,
    )
    equity_curve.index = pd.DatetimeIndex(equity_curve.index)

    metrics = calc_metrics(equity_curve, risk_free_rate=risk_free_rate, trade_log=all_trades)
    logger.info(
        "回测完成 %s ~ %s | 收益率 %.2f%% | 夏普 %.2f | 最大回撤 %.2f%%",
        start_date, end_date,
        metrics.get("total_return", 0) * 100,
        metrics.get("sharpe_ratio", 0),
        metrics.get("max_drawdown", 0) * 100,
    )

    return {
        "equity_curve": equity_curve,
        "metrics":      metrics,
        "trade_log":    all_trades,
    }


if __name__ == "__main__":
    result = run_backtest(
        symbols=["000001", "600519"],
        capital=100_000.0,
        start_date="2022-01-01",
        end_date="2023-12-31",
    )
    print(result["metrics"])
    print(result["equity_curve"].tail())
