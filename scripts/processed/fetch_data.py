"""
数据获取模块：通过 AkShare 拉取 A 股股票及公募基金历史数据，并缓存到本地。
"""

import os
import time
import logging
import yaml
import pandas as pd
import akshare as ak

# 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 项目根目录（此文件位于 scripts/processed/，向上两级为根目录）
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")


def _load_config() -> dict:
    """加载数据配置文件。"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_cache_path(name: str, fmt: str) -> str:
    """根据名称和格式返回缓存文件完整路径。"""
    config = _load_config()
    raw_dir = os.path.join(ROOT_DIR, config["storage"]["raw_dir"])
    os.makedirs(raw_dir, exist_ok=True)
    return os.path.join(raw_dir, f"{name}.{fmt}")


def _save_cache(df: pd.DataFrame, name: str) -> None:
    """将 DataFrame 保存到本地缓存。"""
    config = _load_config()
    fmt = config["storage"]["cache_format"]
    path = _get_cache_path(name, fmt)
    if fmt == "parquet":
        df.to_parquet(path, index=True)
    else:
        df.to_csv(path, index=True, encoding="utf-8-sig")
    logger.info("缓存已保存：%s", path)


def _load_cache(name: str) -> pd.DataFrame | None:
    """从本地缓存读取数据，不存在则返回 None。"""
    config = _load_config()
    fmt = config["storage"]["cache_format"]
    path = _get_cache_path(name, fmt)
    if not os.path.exists(path):
        return None
    logger.info("从缓存加载：%s", path)
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, index_col=0, parse_dates=True, encoding="utf-8-sig")


def fetch_stock_price(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    获取单只股票的日线行情（开高低收量）。

    参数：
        symbol: 股票代码，如 "000001"
        start_date: 开始日期，格式 "YYYY-MM-DD"
        end_date: 结束日期，格式 "YYYY-MM-DD"
        adjust: 复权方式，qfq=前复权，hfq=后复权，空串=不复权
        use_cache: 是否优先读取本地缓存

    返回：
        DatetimeIndex 索引的 DataFrame，列包含 open/high/low/close/volume/turnover
    """
    if not symbol or not start_date or not end_date:
        raise ValueError("symbol、start_date、end_date 均不能为空")

    cache_name = f"stock_{symbol}_{adjust}_{start_date}_{end_date}"
    config = _load_config()

    if use_cache and config["storage"]["cache_enabled"]:
        cached = _load_cache(cache_name)
        if cached is not None:
            return cached

    retry_times = config["data_source"]["retry_times"]
    retry_interval = config["data_source"]["retry_interval"]
    # AkShare 日期格式为 YYYYMMDD
    start_fmt = start_date.replace("-", "")
    end_fmt = end_date.replace("-", "")

    df = None
    for attempt in range(1, retry_times + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_fmt,
                end_date=end_fmt,
                adjust=adjust,
            )
            break
        except Exception as e:
            logger.warning("第 %d 次请求失败（%s）：%s", attempt, symbol, e)
            if attempt < retry_times:
                time.sleep(retry_interval)

    if df is None or df.empty:
        logger.error("无法获取股票数据：%s", symbol)
        return pd.DataFrame()

    # 统一列名并设置日期索引
    df = df.rename(columns={
        "日期": "date", "开盘": "open", "最高": "high",
        "最低": "low", "收盘": "close", "成交量": "volume",
        "成交额": "turnover", "振幅": "amplitude",
        "涨跌幅": "pct_change", "涨跌额": "price_change", "换手率": "turnover_rate",
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    if use_cache and config["storage"]["cache_enabled"]:
        _save_cache(df, cache_name)

    return df


def fetch_fund_nav(
    fund_code: str,
    start_date: str,
    end_date: str,
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    获取公募基金历史净值数据。

    参数：
        fund_code: 基金代码，如 "000001"
        start_date: 开始日期，格式 "YYYY-MM-DD"
        end_date: 结束日期，格式 "YYYY-MM-DD"
        use_cache: 是否优先读取本地缓存

    返回：
        DatetimeIndex 索引的 DataFrame，列包含 nav（单位净值）/ acc_nav（累计净值）
    """
    if not fund_code or not start_date or not end_date:
        raise ValueError("fund_code、start_date、end_date 均不能为空")

    cache_name = f"fund_{fund_code}_{start_date}_{end_date}"
    config = _load_config()

    if use_cache and config["storage"]["cache_enabled"]:
        cached = _load_cache(cache_name)
        if cached is not None:
            return cached

    retry_times = config["data_source"]["retry_times"]
    retry_interval = config["data_source"]["retry_interval"]

    df = None
    for attempt in range(1, retry_times + 1):
        try:
            df = ak.fund_open_fund_info_em(fund=fund_code, indicator="单位净值走势")
            break
        except Exception as e:
            logger.warning("第 %d 次请求失败（%s）：%s", attempt, fund_code, e)
            if attempt < retry_times:
                time.sleep(retry_interval)

    if df is None or df.empty:
        logger.error("无法获取基金数据：%s", fund_code)
        return pd.DataFrame()

    df = df.rename(columns={"净值日期": "date", "单位净值": "nav", "日增长率": "daily_return"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    # 按日期范围过滤
    df = df.loc[start_date:end_date]

    if use_cache and config["storage"]["cache_enabled"]:
        _save_cache(df, cache_name)

    return df


def fetch_trade_calendar(start_date: str, end_date: str) -> list[str]:
    """
    获取沪深交易所交易日历。

    返回：
        交易日期字符串列表，格式 "YYYY-MM-DD"
    """
    df = ak.tool_trade_date_hist_sina()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    mask = (df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)
    return df.loc[mask, "trade_date"].dt.strftime("%Y-%m-%d").tolist()


if __name__ == "__main__":
    # 简单功能验证
    df = fetch_stock_price("000001", "2023-01-01", "2023-12-31")
    print(df.tail())
