"""
回测报告入口：从配置读取标的池与参数，执行回测并输出绩效报告。
"""

import logging
import os
import sys
from collections import defaultdict

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.backtest.engine import run_backtest
from scripts.processed.fetch_data import get_latest_trade_date
from scripts.strategy.signal_generator import resolve_symbol_pool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")

_DATA_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")
_STRATEGY_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _calc_realized_pnl_by_symbol(trade_log: list[dict]) -> dict[str, float]:
    """按代码汇总已实现盈亏（来自 sell 记录的 pnl 字段）。"""
    result: dict[str, float] = defaultdict(float)
    for t in trade_log:
        if t.get("action") != "sell":
            continue
        sym = str(t.get("symbol", "")).strip()
        if not sym:
            continue
        result[sym] += float(t.get("pnl", 0.0) or 0.0)
    return dict(result)


def _calc_win_rate_by_symbol(trade_log: list[dict]) -> dict[str, tuple[int, int, float | None]]:
    """
    按代码统计胜率。

    返回：
        {symbol: (win_count, sell_count, win_rate_or_none)}
    """
    sells: dict[str, int] = defaultdict(int)
    wins: dict[str, int] = defaultdict(int)

    for t in trade_log:
        if t.get("action") != "sell":
            continue
        sym = str(t.get("symbol", "")).strip()
        if not sym:
            continue
        sells[sym] += 1
        if float(t.get("pnl", 0.0) or 0.0) > 0:
            wins[sym] += 1

    result: dict[str, tuple[int, int, float | None]] = {}
    symbols = set(list(sells.keys()) + list(wins.keys()))
    for sym in symbols:
        sell_count = sells.get(sym, 0)
        win_count = wins.get(sym, 0)
        if sell_count == 0:
            result[sym] = (win_count, sell_count, None)
        else:
            result[sym] = (win_count, sell_count, round(win_count / sell_count, 4))
    return result


def _print_symbol_breakdown(
    symbols: list[str],
    start_date: str,
    end_date: str,
    capital: float,
    risk_free_rate: float,
    realized_pnl_by_symbol: dict[str, float],
    win_rate_by_symbol: dict[str, tuple[int, int, float | None]],
) -> None:
    """输出每个代码的单标的回测指标与已实现盈亏。"""
    rows: list[dict] = []

    root_logger = logging.getLogger()
    prev_disable = root_logger.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        for sym in symbols:
            try:
                single = run_backtest(
                    symbols=[sym],
                    capital=capital,
                    start_date=start_date,
                    end_date=end_date,
                    risk_free_rate=risk_free_rate,
                )
                metrics = single.get("metrics", {})
                if not metrics:
                    continue
                rows.append({
                    "symbol": sym,
                    "total_return": float(metrics.get("total_return", 0.0)),
                    "sharpe": float(metrics.get("sharpe_ratio", 0.0)),
                    "realized_pnl": float(realized_pnl_by_symbol.get(sym, 0.0)),
                    "win_stat": win_rate_by_symbol.get(sym, (0, 0, None)),
                })
            except Exception as e:
                logger.warning("单标的分解失败：%s - %s", sym, e)
    finally:
        logging.disable(prev_disable)

    if not rows:
        print("分代码表现: 无可用数据")
        return

    rows.sort(
        key=lambda x: (
            x["total_return"],
            x["sharpe"],
            (x["win_stat"][2] if x["win_stat"][2] is not None else -1.0),
        ),
        reverse=True,
    )

    print("分代码表现（单标的收益/夏普 + 组合成交胜率）：")
    print("代码      收益率      夏普    胜率(赢/平仓)      已实现盈亏(元)")
    for r in rows:
        win_count, sell_count, win_rate = r["win_stat"]
        if win_rate is None:
            win_text = "N/A"
        else:
            win_text = f"{win_rate:.2%}({win_count}/{sell_count})"
        print(
            f"{r['symbol']:<8} {r['total_return']:>8.2%}  {r['sharpe']:>7.4f}  "
            f"{win_text:>14}  {r['realized_pnl']:>14.2f}"
        )


def main() -> None:
    logging.disable(logging.CRITICAL)

    data_cfg = _load_yaml(_DATA_CONFIG_PATH)
    strategy_cfg = _load_yaml(_STRATEGY_CONFIG_PATH)

    backtest_cfg = data_cfg.get("backtest", {})
    evaluation_cfg = strategy_cfg.get("evaluation", {})
    capital_cfg = strategy_cfg.get("capital", {})

    symbols = resolve_symbol_pool()
    if not symbols:
        raise ValueError("标的池为空，请先在 strategy_config.yaml 中配置 holdings/watchlist/current_positions")

    start_date = backtest_cfg.get("start_date", "2015-01-01")
    end_date = get_latest_trade_date()

    result = run_backtest(
        symbols=symbols,
        capital=float(capital_cfg.get("total", 100000)),
        start_date=start_date,
        end_date=end_date,
        risk_free_rate=float(evaluation_cfg.get("risk_free_rate", 0.02)),
    )

    metrics = result["metrics"]
    trade_log = result.get("trade_log", [])
    realized_pnl_by_symbol = _calc_realized_pnl_by_symbol(trade_log)
    win_rate_by_symbol = _calc_win_rate_by_symbol(trade_log)
    capital = float(capital_cfg.get("total", 100000))
    risk_free_rate = float(evaluation_cfg.get("risk_free_rate", 0.02))

    print("=" * 60)
    print("Livermore 回测绩效报告")
    print("=" * 60)
    print(f"回测区间: {start_date} ~ {end_date}（结束日自动取最近交易日）")
    print(f"标的池: {', '.join(symbols)}")
    print(f"总收益率:   {metrics.get('total_return', 0.0):.2%}")
    print(f"年化收益率: {metrics.get('annual_return', 0.0):.2%}")
    print(f"夏普比率:   {metrics.get('sharpe_ratio', 0.0):.4f}")
    print(f"最大回撤:   {metrics.get('max_drawdown', 0.0):.2%}")
    print(f"年化波动率: {metrics.get('annual_vol', 0.0):.2%}")
    print(f"胜率:       {metrics.get('win_rate', 0.0):.2%}")
    print(f"成交笔数:   {len(trade_log)}")
    print("-" * 60)
    _print_symbol_breakdown(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        capital=capital,
        risk_free_rate=risk_free_rate,
        realized_pnl_by_symbol=realized_pnl_by_symbol,
        win_rate_by_symbol=win_rate_by_symbol,
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
