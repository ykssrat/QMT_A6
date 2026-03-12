"""
市场候选标的扫描器。

从场内 ETF 市场和沪深 300 成分股中，按流动性筛选候选标的，
供信号生成器扩展标的池，使策略能够发现持仓以外的潜在买入机会。

主要流程：
    1. scan_etf_candidates   - 按成交额筛选活跃 ETF
    2. scan_stock_candidates - 在沪深 300 内按成交额筛选活跃个股
    3. get_market_candidates - 合并两类候选，返回 {symbol: meta} 映射
"""

import logging
import os
import sys

import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import akshare as ak

logger = logging.getLogger(__name__)

# ETF 最低成交额过滤阈值（元），过滤掉流动性极差的标的
_MIN_ETF_AMOUNT = 5_000_000  # 500 万元


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
