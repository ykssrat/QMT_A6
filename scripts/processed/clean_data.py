"""
数据清洗模块：对原始行情数据进行质量检查、缺失值填充、异常值过滤及交易日对齐。
"""

import logging
import yaml
import os
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")


def _load_config() -> dict:
    """加载数据配置文件。"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fill_missing_prices(df: pd.DataFrame, method: str = "ffill") -> pd.DataFrame:
    """
    填充停牌或缺失的价格数据。

    参数：
        df: 含价格列的 DataFrame，DatetimeIndex
        method: 填充方式，ffill=前值填充，bfill=后值填充

    返回：
        填充后的 DataFrame
    """
    if df.empty:
        return df

    price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
    before = df[price_cols].isna().sum().sum()

    if method == "ffill":
        df[price_cols] = df[price_cols].ffill()
    elif method == "bfill":
        df[price_cols] = df[price_cols].bfill()
    else:
        raise ValueError(f"不支持的填充方式：{method}")

    # 成交量停牌时置零
    if "volume" in df.columns:
        df["volume"] = df["volume"].fillna(0)

    after = df[price_cols].isna().sum().sum()
    logger.info("缺失值填充完成：填充前 %d 处，填充后 %d 处", before, after)
    return df


def remove_price_anomalies(df: pd.DataFrame, price_limit: float = 0.11) -> pd.DataFrame:
    """
    过滤单日涨跌幅超出合理范围的异常行数据（涨跌停外的极端值视为脏数据）。

    参数：
        df: 含 close 列的 DataFrame，DatetimeIndex
        price_limit: 单日最大涨跌幅阈值，默认 0.11（11%）

    返回：
        过滤后的 DataFrame
    """
    if "close" not in df.columns or df.empty:
        return df

    pct = df["close"].pct_change().abs()
    # 允许略超涨跌停（个股到 20% 科创板、北交所），此处保守取 price_limit + 10%
    threshold = price_limit + 0.10
    anomaly_mask = pct > threshold
    anomaly_count = anomaly_mask.sum()

    if anomaly_count > 0:
        logger.warning("检测到 %d 条涨跌幅异常记录（阈值 %.0f%%），已标记为 NaN 后前值填充", anomaly_count, threshold * 100)
        df.loc[anomaly_mask, ["open", "high", "low", "close"]] = np.nan
        df = fill_missing_prices(df, method="ffill")

    return df


def align_to_trade_calendar(df: pd.DataFrame, trade_dates: list[str]) -> pd.DataFrame:
    """
    将数据对齐到标准交易日历，确保每个交易日都有一行记录。

    参数：
        df: 原始行情 DataFrame，DatetimeIndex
        trade_dates: 交易日期字符串列表，格式 "YYYY-MM-DD"

    返回：
        对齐后的 DataFrame（停牌日数据用前值填充）
    """
    if df.empty or not trade_dates:
        return df

    calendar_index = pd.DatetimeIndex(trade_dates)
    df = df.reindex(calendar_index)
    df = fill_missing_prices(df, method="ffill")
    df["volume"] = df["volume"].fillna(0)
    return df


def filter_insufficient_data(df: pd.DataFrame, min_trade_days: int = 60) -> pd.DataFrame | None:
    """
    过滤有效交易天数不足的股票数据。

    参数：
        df: 行情 DataFrame
        min_trade_days: 最少有效交易天数

    返回：
        有效则返回原 DataFrame，不足则返回 None
    """
    if df.empty:
        return None

    valid_days = df["close"].notna().sum() if "close" in df.columns else 0
    if valid_days < min_trade_days:
        logger.warning("有效交易天数 %d < 最低要求 %d，跳过该标的", valid_days, min_trade_days)
        return None
    return df


def clean_stock_data(
    df: pd.DataFrame,
    trade_dates: list[str] | None = None,
) -> pd.DataFrame | None:
    """
    股票行情数据完整清洗流程：缺失填充 → 异常过滤 → 交易日对齐 → 有效性校验。

    参数：
        df: 原始行情 DataFrame（由 fetch_data 模块获取）
        trade_dates: 标准交易日历列表，为空则不做对齐

    返回：
        清洗后的 DataFrame，若数据不符合要求则返回 None
    """
    if df is None or df.empty:
        logger.warning("输入数据为空，跳过清洗")
        return None

    config = _load_config()
    clean_cfg = config.get("clean", {})
    fill_method = clean_cfg.get("fill_method", "ffill")
    price_limit = clean_cfg.get("price_limit", 0.11)
    min_trade_days = clean_cfg.get("min_trade_days", 60)

    df = fill_missing_prices(df, method=fill_method)
    df = remove_price_anomalies(df, price_limit=price_limit)

    if trade_dates:
        df = align_to_trade_calendar(df, trade_dates)

    df = filter_insufficient_data(df, min_trade_days=min_trade_days)
    return df


def clean_fund_data(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    基金净值数据清洗：处理缺失净值，确保单调递增日期索引。

    参数：
        df: 原始基金净值 DataFrame（nav 列）

    返回：
        清洗后的 DataFrame
    """
    if df is None or df.empty:
        return None

    df = df[~df.index.duplicated(keep="last")].sort_index()

    if "nav" in df.columns:
        df["nav"] = df["nav"].ffill()

    return df


if __name__ == "__main__":
    # 简单功能验证
    from scripts.processed.fetch_data import fetch_stock_price, fetch_trade_calendar

    raw = fetch_stock_price("000001", "2023-01-01", "2023-12-31")
    calendar = fetch_trade_calendar("2023-01-01", "2023-12-31")
    cleaned = clean_stock_data(raw, trade_dates=calendar)
    if cleaned is not None:
        print(cleaned.tail())
