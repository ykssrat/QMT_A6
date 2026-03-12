"""
回测报告入口：从配置读取标的池与参数，执行回测并输出绩效报告。
"""

import logging
import os
import sys

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.backtest.engine import run_backtest
from scripts.strategy.signal_generator import resolve_symbol_pool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

_DATA_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")
_STRATEGY_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    data_cfg = _load_yaml(_DATA_CONFIG_PATH)
    strategy_cfg = _load_yaml(_STRATEGY_CONFIG_PATH)

    backtest_cfg = data_cfg.get("backtest", {})
    evaluation_cfg = strategy_cfg.get("evaluation", {})
    capital_cfg = strategy_cfg.get("capital", {})

    symbols = resolve_symbol_pool()
    if not symbols:
        raise ValueError("标的池为空，请先在 strategy_config.yaml 中配置 holdings/watchlist/current_positions")

    result = run_backtest(
        symbols=symbols,
        capital=float(capital_cfg.get("total", 100000)),
        start_date=backtest_cfg.get("start_date", "2015-01-01"),
        end_date=backtest_cfg.get("end_date", "2024-12-31"),
        risk_free_rate=float(evaluation_cfg.get("risk_free_rate", 0.02)),
    )

    metrics = result["metrics"]
    print("=" * 60)
    print("Livermore 回测绩效报告")
    print("=" * 60)
    print(f"标的池: {', '.join(symbols)}")
    print(f"总收益率:   {metrics.get('total_return', 0.0):.2%}")
    print(f"年化收益率: {metrics.get('annual_return', 0.0):.2%}")
    print(f"夏普比率:   {metrics.get('sharpe_ratio', 0.0):.4f}")
    print(f"最大回撤:   {metrics.get('max_drawdown', 0.0):.2%}")
    print(f"年化波动率: {metrics.get('annual_vol', 0.0):.2%}")
    print(f"胜率:       {metrics.get('win_rate', 0.0):.2%}")
    print(f"成交笔数:   {len(result.get('trade_log', []))}")
    print("=" * 60)


if __name__ == "__main__":
    main()
