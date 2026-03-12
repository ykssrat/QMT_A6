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
from scripts.utils.market_scanner import recommend_best_candidate

import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
    logger.info(
        "标的池：%d 条（持仓列表 %d / 持仓明细 %d / 自选 %d / 额外 %d）",
        len(merged), len(holdings), len(position_symbols), len(watchlist), len(extra),
    )
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

    logger.info("已从配置加载组合：现金 %.2f 元，持仓 %d 只", portfolio.cash, len(portfolio.positions))
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
        logger.info("处理标的：%s", sym)
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
    market_scan: bool = False,
) -> list[dict]:
    """
    获取指定日期（默认当日）的交易信号建议。

    参数：
        symbols: 待扫描的标的代码列表；为 None 时自动读取配置中的持仓与自选列表
        portfolio: 当前持仓与资金状态
        start_date: 历史数据起始日期（用于因子计算，建议至少 60 个交易日前）
        signal_date: 信号日期，格式 "YYYY-MM-DD"，默认取今日
        market_scan: 是否开启市场优选推荐（从候选池中按利弗莫尔历史表现推荐 1 个代码），
                 开启后会额外进行候选回测，耗时较长

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

    cfg = _load_strategy_config()
    lv_cfg = cfg.get("livermore", {})
    signal_cfg = cfg.get("signal", {})

    strategy_params = {
        "m": float(lv_cfg.get("m", 0.1)),
        "c": float(lv_cfg.get("c", 0.07)),
        "h": float(lv_cfg.get("h", 0.10)),
        "k": float(lv_cfg.get("k", 0.5)),
        "z_threshold": float(signal_cfg.get("confidence_threshold", 1.5)),
        "y_threshold": float(lv_cfg.get("y_threshold", 0.55)),
    }

    # 市场扫描：只推荐 1 个在利弗莫尔策略下历史表现较优的候选代码
    scan_meta: dict[str, dict] | None = None
    if market_scan:
        logger.info("开启市场扫描，正在计算单一优选推荐代码...")
        existing = set(symbols)
        best = recommend_best_candidate(
            exclude_symbols=existing,
            etf_top_n=int(signal_cfg.get("scan_etf_top_n", 8)),
            stock_top_n=int(signal_cfg.get("scan_stock_top_n", 8)),
            eval_days=int(signal_cfg.get("scan_eval_days", 365)),
            strategy_params=strategy_params,
            risk_free_rate=float(cfg.get("evaluation", {}).get("risk_free_rate", 0.02)),
        )
        if best:
            scan_meta = {
                best["symbol"]: {
                    "name": best["symbol"],
                    "asset_type": best.get("asset_type", "stock"),
                }
            }
            symbols = list(dict.fromkeys(symbols + [best["symbol"]]))
            logger.info("市场优选推荐代码：%s", best["symbol"])
        else:
            logger.warning("市场优选未找到有效候选，本次仅使用持仓+自选标的")

    features_map = prepare_features(symbols, start_date, signal_date, extra_meta=scan_meta)

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
    import argparse

    parser = argparse.ArgumentParser(description="生成当日 Livermore 策略交易信号")
    parser.add_argument(
        "--market-scan",
        action="store_true",
        default=False,
        help="开启市场优选推荐（从候选池中只推荐 1 个代码并参与信号计算，耗时较长）",
    )
    args = parser.parse_args()

    signal_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")
    runtime_portfolio = load_portfolio_from_config()
    get_latest_signals(
        portfolio=runtime_portfolio,
        start_date=start_date,
        signal_date=signal_date,
        market_scan=args.market_scan,
    )
