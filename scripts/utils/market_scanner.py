"""
市场候选扫描与优选推荐。

功能：
    1) 扫描候选：从活跃 ETF 与沪深 300 成分股中筛选候选
    2) 优选推荐：对候选逐一做利弗莫尔单标的回测，输出 1 个最优代码
"""

import logging
import os
import sys
from datetime import date, timedelta

import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import akshare as ak
from scripts.backtest.engine import run_backtest

logger = logging.getLogger(__name__)

# ETF 最低成交额过滤阈值（元），过滤掉流动性极差的标的
_MIN_ETF_AMOUNT = 5_000_000  # 500 万元
_DEFAULT_EVAL_CAPITAL = 100000


def scan_etf_candidates(
    top_n: int = 30,
    exclude_symbols: set[str] | None = None,
) -> list[str]:
    """
    从场内 ETF 列表中，按当日成交额降序筛选活跃 ETF，返回代码列表。

    参数：
        top_n: 最多返回的候选标的数量
        exclude_symbols: 已持有或已在监控的标的代码集合，跳过这些标的

    返回：
        ETF 代码列表（6 位字符串）
    """
    exclude = exclude_symbols or set()
    try:
        df = ak.fund_etf_spot_em()
        if df.empty or "代码" not in df.columns:
            logger.warning("fund_etf_spot_em 返回数据为空或字段缺失，跳过 ETF 扫描")
            return []

        # 按成交额降序排列，过滤低流动性标的
        if "成交额" in df.columns:
            df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce").fillna(0)
            df = df[df["成交额"] >= _MIN_ETF_AMOUNT]
            df = df.sort_values("成交额", ascending=False)

        candidates = [
            str(code)
            for code in df["代码"].tolist()
            if str(code) not in exclude
        ]
        result = candidates[:top_n]
        logger.info("ETF 候选扫描：筛出 %d 只（原始 %d 只，排除 %d 只）", len(result), len(df), len(exclude))
        return result

    except Exception as e:
        logger.warning("ETF 候选扫描失败：%s", e)
        return []


def scan_stock_candidates(
    top_n: int = 20,
    exclude_symbols: set[str] | None = None,
) -> list[str]:
    """
    从沪深 300 成分股中，按当日成交额筛选活跃个股，返回代码列表。

    使用沪深 300 成分股作为候选域，在兼顾流动性的同时限制扫描规模。

    参数：
        top_n: 最多返回的候选标的数量
        exclude_symbols: 已持有或已在监控的标的代码集合

    返回：
        股票代码列表（6 位字符串）
    """
    exclude = exclude_symbols or set()
    try:
        # 获取沪深 300 成分股代码
        df_index = ak.index_stock_cons(symbol="000300")
        if df_index.empty:
            logger.warning("沪深 300 成分股列表为空，跳过股票扫描")
            return []

        hs300_codes = set(df_index.iloc[:, 0].astype(str).tolist())

        # 获取 A 股实时行情，过滤出沪深 300 成分股
        df_spot = ak.stock_zh_a_spot_em()
        if df_spot.empty or "代码" not in df_spot.columns:
            logger.warning("stock_zh_a_spot_em 返回数据为空或字段缺失，跳过股票扫描")
            return []

        df_spot = df_spot[df_spot["代码"].isin(hs300_codes)].copy()

        if "成交额" in df_spot.columns:
            df_spot["成交额"] = pd.to_numeric(df_spot["成交额"], errors="coerce").fillna(0)
            df_spot = df_spot.sort_values("成交额", ascending=False)

        candidates = [
            str(code)
            for code in df_spot["代码"].tolist()
            if str(code) not in exclude
        ]
        result = candidates[:top_n]
        logger.info("个股候选扫描：筛出 %d 只（沪深 300 共 %d 只）", len(result), len(hs300_codes))
        return result

    except Exception as e:
        logger.warning("个股候选扫描失败：%s", e)
        return []


def get_market_candidates(
    etf_top_n: int = 30,
    stock_top_n: int = 20,
    exclude_symbols: set[str] | None = None,
) -> dict[str, dict]:
    """
    综合扫描市场候选标的（ETF + 沪深 300 个股）。

    返回的字典格式与 build_asset_metadata() 保持一致，
    可直接作为 extra_meta 传入 build_asset_metadata() 以覆盖资产类型。

    参数：
        etf_top_n: ETF 候选数量上限
        stock_top_n: 个股候选数量上限
        exclude_symbols: 需要排除的标的集合

    返回：
        {symbol: {"name": str, "asset_type": str}} 字典
    """
    exclude = exclude_symbols or set()

    etf_codes = scan_etf_candidates(top_n=etf_top_n, exclude_symbols=exclude)
    stock_codes = scan_stock_candidates(top_n=stock_top_n, exclude_symbols=exclude)

    result: dict[str, dict] = {}
    for code in etf_codes:
        result[code] = {"name": code, "asset_type": "etf"}
    for code in stock_codes:
        result[code] = {"name": code, "asset_type": "stock"}

    logger.info(
        "市场候选扫描完成：ETF %d 只 + 个股 %d 只 = 合计 %d 只",
        len(etf_codes),
        len(stock_codes),
        len(result),
    )
    return result


def _score_backtest(metrics: dict) -> float:
    """
    将回测结果压缩为单一评分，兼顾收益、夏普、回撤。

    分数越高越优：
        score = total_return + 0.3 * sharpe_ratio + 0.2 * win_rate + 0.2 * max_drawdown
    注意：max_drawdown 为负值，天然形成惩罚项。
    """
    total_return = float(metrics.get("total_return", 0.0))
    sharpe = float(metrics.get("sharpe_ratio", 0.0))
    win_rate = float(metrics.get("win_rate", 0.0))
    max_drawdown = float(metrics.get("max_drawdown", 0.0))
    return total_return + 0.3 * sharpe + 0.2 * win_rate + 0.2 * max_drawdown


def recommend_best_candidate(
    exclude_symbols: set[str] | None = None,
    etf_top_n: int = 8,
    stock_top_n: int = 8,
    eval_days: int = 365,
    strategy_params: dict | None = None,
    risk_free_rate: float = 0.02,
) -> dict | None:
    """
    推荐 1 个在利弗莫尔策略下历史表现较优的候选代码。

    步骤：
        1. 扫描候选（ETF + 沪深300）
        2. 对每个候选做单标的回测
        3. 按评分函数排序，返回第 1 名

    返回：
        {
            "symbol": "xxxxxx",
            "asset_type": "stock|etf",
            "score": float,
            "metrics": {...}
        }
        若无有效候选则返回 None
    """
    exclude = exclude_symbols or set()
    universe_meta = get_market_candidates(
        etf_top_n=etf_top_n,
        stock_top_n=stock_top_n,
        exclude_symbols=exclude,
    )
    if not universe_meta:
        logger.warning("市场优选：候选集合为空")
        return None

    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=eval_days)).strftime("%Y-%m-%d")

    best: dict | None = None
    for symbol, meta in universe_meta.items():
        try:
            result = run_backtest(
                symbols=[symbol],
                capital=_DEFAULT_EVAL_CAPITAL,
                start_date=start_date,
                end_date=end_date,
                risk_free_rate=risk_free_rate,
                strategy_params=strategy_params,
                asset_meta_override={symbol: meta},
            )
            metrics = result.get("metrics", {})
            if not metrics:
                continue

            score = _score_backtest(metrics)
            candidate = {
                "symbol": symbol,
                "asset_type": meta.get("asset_type", "stock"),
                "score": score,
                "metrics": metrics,
            }
            if best is None or candidate["score"] > best["score"]:
                best = candidate

        except Exception as e:
            logger.warning("市场优选回测失败：%s - %s", symbol, e)
            continue

    if best:
        logger.info(
            "市场优选推荐：%s（score=%.4f, return=%.2f%%, sharpe=%.2f, mdd=%.2f%%）",
            best["symbol"],
            best["score"],
            100 * float(best["metrics"].get("total_return", 0.0)),
            float(best["metrics"].get("sharpe_ratio", 0.0)),
            100 * float(best["metrics"].get("max_drawdown", 0.0)),
        )
    else:
        logger.warning("市场优选：未找到可用推荐")

    return best
