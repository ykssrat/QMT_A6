"""
信号生成入口：将数据管线（清洗 → 因子计算）与 Livermore 策略整合，
输出当日交易建议列表。
"""

import logging
import os
import sys
from datetime import date, timedelta

import pandas as pd

# 将项目根目录加入模块搜索路径，支持直接运行
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.processed.fetch_data import fetch_trade_calendar
from scripts.features.calc_features import build_all_features
from scripts.strategy.livermore import LivermoreStrategy, Portfolio, Position
from scripts.utils.asset_loader import build_asset_metadata, fetch_asset_history

import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_strategy_config() -> dict:
    """加载策略配置文件，返回完整配置字典。"""
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_symbol_pool(extra_symbols: list[str] | None = None) -> list[str]:
    """
    合并三类标的来源，返回去重后的完整标的池。

    来源说明：
        - 用户持仓（holdings）：来自 strategy_config.yaml capital.holdings
        - 用户自选（watchlist）：来自 strategy_config.yaml capital.watchlist
        - 额外传入（extra_symbols）：调用方临时追加，用于模型候选扩展

    模型候选标的（Z >= threshold）在 generate_signals 阶段由策略自动过滤，
    本函数只负责汇集"必须进入计算"的标的集合。

    参数：
        extra_symbols: 调用方传入的额外标的列表（可选）

    返回：
        去重保序的标的代码列表
    """
    cfg = _load_strategy_config()
    capital = cfg.get("capital", {})
    holdings = capital.get("holdings") or []
    watchlist = capital.get("watchlist") or []
    current_positions = capital.get("current_positions") or []
    position_symbols = [item.get("symbol") for item in current_positions if item.get("symbol")]
    extra    = extra_symbols or []

    # 按持仓 → 自选 → 额外的顺序合并，dict.fromkeys 去重同时保持顺序
    merged = list(dict.fromkeys(holdings + position_symbols + watchlist + extra))
    return merged


def load_portfolio_from_config() -> Portfolio:
    """
    从 strategy_config.yaml 加载当前真实组合状态。

    配置来源：
        capital.available_cash
        capital.current_positions

    返回：
        Portfolio 对象，可直接传入 get_latest_signals()
    """
    cfg = _load_strategy_config()
    capital_cfg = cfg.get("capital", {})
    available_cash = capital_cfg.get("available_cash")
    if available_cash is None:
        available_cash = float(capital_cfg.get("stock_available_cash", 0.0) or 0.0) + float(
            capital_cfg.get("fund_available_cash", 0.0) or 0.0
        )
    else:
        available_cash = float(available_cash or 0.0)
    positions_cfg = capital_cfg.get("current_positions") or []

    portfolio = Portfolio(cash=available_cash)
    for item in positions_cfg:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            continue

        cost_price = float(item.get("cost_price", 0.0) or 0.0)
        shares = float(item.get("shares", 0) or 0)
        peak_price = float(item.get("peak_price", cost_price) or cost_price)
        add_unlocked = bool(item.get("add_unlocked", False))

        if cost_price <= 0 or shares <= 0:
            logger.warning("持仓 %s 配置无效，已跳过（cost_price=%s, shares=%s）", symbol, cost_price, shares)
            continue

        portfolio.positions[symbol] = Position(
            symbol=symbol,
            cost_price=cost_price,
            shares=shares,
            peak_price=peak_price,
            add_unlocked=add_unlocked,
        )

    return portfolio


def prepare_features(
    symbols: list[str],
    start_date: str,
    end_date: str,
    extra_meta: dict[str, dict] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    批量拉取、清洗并计算因子，返回 {symbol: features_df} 字典。

    参数：
        extra_meta: 调用方传入的额外资产元信息（如市场扫描的 ETF/股票），
                    会覆盖 build_asset_metadata() 中同名 symbol 的类型信息。
    """
    trade_dates = fetch_trade_calendar(start_date, end_date)
    asset_meta = build_asset_metadata(extra_meta=extra_meta)
    result = {}
    for sym in symbols:
        cleaned = fetch_asset_history(
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            asset_meta=asset_meta,
        )
        if cleaned is None:
            logger.warning("标的 %s 数据不足，跳过", sym)
            continue
        result[sym] = build_all_features(cleaned)
    return result


def get_latest_signals(
    portfolio: Portfolio,
    start_date: str,
    symbols: list[str] | None = None,
    signal_date: str | None = None,
) -> list[dict]:
    """
    获取指定日期（默认当日）的交易信号建议。

    参数：
        symbols: 待扫描的标的代码列表；为 None 时自动读取配置中的持仓与自选列表
        portfolio: 当前持仓与资金状态
        start_date: 历史数据起始日期（用于因子计算，建议至少 60 个交易日前）
        signal_date: 信号日期，格式 "YYYY-MM-DD"，默认取今日

    返回：
        信号列表（参见 LivermoreStrategy.generate_signals 返回格式）
    """
    if signal_date is None:
        signal_date = date.today().strftime("%Y-%m-%d")

    if not symbols:
        symbols = resolve_symbol_pool()
    if not symbols:
        logger.warning("标的池为空，请在 strategy_config.yaml 中配置 holdings 或 watchlist")
        return []

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
        print(f"[{signal_date}] 无交易信号")
        return

    print("=" * 60)
    print(f"[{signal_date}] 共 {len(signals)} 条交易建议：")
    for i, sig in enumerate(signals, 1):
        print(
            f"  {i}. [{sig['action'].upper()}] {sig['symbol']}  金额: {sig['amount']:.2f} 元  原因: {sig['reason']}"
        )
    print("=" * 60)


if __name__ == "__main__":
    signal_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")
    runtime_portfolio = load_portfolio_from_config()
    get_latest_signals(
        portfolio=runtime_portfolio,
        start_date=start_date,
        signal_date=signal_date,
    )
