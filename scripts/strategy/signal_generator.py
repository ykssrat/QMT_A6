"""
信号生成入口：将数据管线（清洗 → 因子计算）与 Livermore 策略整合，
输出当日交易建议列表。
"""

import logging
import os
import sys
from datetime import date

import pandas as pd

# 将项目根目录加入模块搜索路径，支持直接运行
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.processed.fetch_data import fetch_stock_price, fetch_trade_calendar
from scripts.processed.clean_data import clean_stock_data
from scripts.features.calc_features import build_all_features
from scripts.strategy.livermore import LivermoreStrategy, Portfolio, Position

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def prepare_features(
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, pd.DataFrame]:
    """
    批量拉取、清洗并计算因子，返回 {symbol: features_df} 字典。
    """
    trade_dates = fetch_trade_calendar(start_date, end_date)
    result = {}
    for sym in symbols:
        logger.info("处理标的：%s", sym)
        raw = fetch_stock_price(sym, start_date, end_date)
        cleaned = clean_stock_data(raw, trade_dates=trade_dates)
        if cleaned is None:
            logger.warning("标的 %s 数据不足，跳过", sym)
            continue
        result[sym] = build_all_features(cleaned)
    return result


def get_latest_signals(
    symbols: list[str],
    portfolio: Portfolio,
    start_date: str,
    signal_date: str | None = None,
) -> list[dict]:
    """
    获取指定日期（默认当日）的交易信号建议。

    参数：
        symbols: 待扫描的标的代码列表
        portfolio: 当前持仓与资金状态
        start_date: 历史数据起始日期（用于因子计算，建议至少 60 个交易日前）
        signal_date: 信号日期，格式 "YYYY-MM-DD"，默认取今日

    返回：
        信号列表（参见 LivermoreStrategy.generate_signals 返回格式）
    """
    if signal_date is None:
        signal_date = date.today().strftime("%Y-%m-%d")

    features_map = prepare_features(symbols, start_date, signal_date)

    # 提取信号日当天的最新价格与信心因子
    prices: dict[str, float] = {}
    confidence_scores: dict[str, float] = {}

    for sym, df in features_map.items():
        if df.empty or signal_date not in df.index.strftime("%Y-%m-%d"):
            # 取最后一行数据（最近可用交易日）
            last_row = df.iloc[-1]
        else:
            last_row = df.loc[signal_date]

        prices[sym] = float(last_row["close"])
        if "confidence_z" in last_row.index:
            confidence_scores[sym] = float(last_row["confidence_z"])
        else:
            confidence_scores[sym] = 0.0

    strategy = LivermoreStrategy()
    signals = strategy.generate_signals(portfolio, prices, confidence_scores)

    _print_signals(signals, signal_date)
    return signals


def _print_signals(signals: list[dict], signal_date: str) -> None:
    """格式化输出信号列表。"""
    if not signals:
        logger.info("[%s] 无交易信号", signal_date)
        return

    logger.info("=" * 60)
    logger.info("[%s] 共 %d 条交易建议：", signal_date, len(signals))
    for i, sig in enumerate(signals, 1):
        logger.info(
            "  %d. [%s] %s  金额: %.2f 元  原因: %s",
            i, sig["action"].upper(), sig["symbol"], sig["amount"], sig["reason"],
        )
    logger.info("=" * 60)


if __name__ == "__main__":
    # 示例：扫描几只股票，以空组合（10万资金）为基础生成当日信号
    demo_symbols = ["000001", "600519", "300750"]
    demo_portfolio = Portfolio(cash=100_000.0)

    get_latest_signals(
        symbols=demo_symbols,
        portfolio=demo_portfolio,
        start_date="2023-01-01",
        signal_date="2023-12-29",
    )
