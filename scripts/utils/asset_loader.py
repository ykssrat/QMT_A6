"""
资产加载工具：统一处理股票、场内 ETF/LOF、场外基金的历史数据读取与标准化。
"""

import os
import sys

import pandas as pd
import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.processed.fetch_data import fetch_stock_price, fetch_etf_price, fetch_fund_nav
from scripts.processed.clean_data import clean_stock_data

_STRATEGY_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_strategy_config() -> dict:
    with open(_STRATEGY_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_asset_metadata(extra_meta: dict[str, dict] | None = None) -> dict[str, dict]:
    """
    从策略配置中构建资产元信息映射。

    读取来源（优先级从低到高，相同 symbol 后者覆盖前者）：
        1. current_positions - 当前持仓明细
        2. watchlist_metadata - 自选标的元信息
        3. extra_meta - 调用方传入的额外覆盖（用于市场扫描候选标的）
    """
    cfg = _load_strategy_config()
    capital_cfg = cfg.get("capital", {})

    result: dict[str, dict] = {}

    # 1. 从持仓明细读取
    for item in capital_cfg.get("current_positions") or []:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            continue
        result[symbol] = {
            "name": item.get("name", symbol),
            "asset_type": item.get("asset_type", "stock"),
        }

    # 2. 从自选标的元信息读取（watchlist_metadata）
    for symbol, meta in (capital_cfg.get("watchlist_metadata") or {}).items():
        symbol = str(symbol).strip()
        if not symbol:
            continue
        result[symbol] = {
            "name": meta.get("name", symbol),
            "asset_type": meta.get("asset_type", "stock"),
        }

    # 3. 用调用方传入的额外元信息覆盖（市场扫描候选标的）
    if extra_meta:
        for symbol, meta in extra_meta.items():
            result[str(symbol).strip()] = meta

    return result


def get_asset_type(symbol: str, asset_meta: dict[str, dict] | None = None) -> str:
    """获取资产类型；未配置时默认按股票/场内基金日线处理。"""
    if asset_meta and symbol in asset_meta:
        return str(asset_meta[symbol].get("asset_type", "stock"))
    return "stock"


def normalize_fund_nav(df: pd.DataFrame) -> pd.DataFrame:
    """
    将基金净值序列标准化为策略统一使用的 OHLCV 结构。

    规则：
        - open/high/low/close 均使用 nav
        - volume/turnover 置 0
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if "nav" not in df.columns:
        raise ValueError("基金数据缺少 nav 列，无法标准化")

    result = pd.DataFrame(index=df.index.copy())
    result["open"] = df["nav"]
    result["high"] = df["nav"]
    result["low"] = df["nav"]
    result["close"] = df["nav"]
    result["volume"] = 0.0
    result["turnover"] = 0.0
    return result


def fetch_asset_history(
    symbol: str,
    start_date: str,
    end_date: str,
    trade_dates: list[str],
    asset_meta: dict[str, dict] | None = None,
) -> pd.DataFrame | None:
    """
    统一获取并清洗单只资产历史数据。

    支持类型：
        - stock: A 股股票
        - etf: 场内 ETF
        - lof: 场内 LOF
        - fund_open: 场外开放式基金
    """
    asset_type = get_asset_type(symbol, asset_meta)

    if asset_type == "stock":
        raw = fetch_stock_price(symbol, start_date, end_date)
        return clean_stock_data(raw, trade_dates=trade_dates)

    if asset_type in {"etf", "lof"}:
        raw = fetch_etf_price(symbol, start_date, end_date)
        return clean_stock_data(raw, trade_dates=trade_dates)

    if asset_type == "fund_open":
        raw = fetch_fund_nav(symbol, start_date, end_date)
        normalized = normalize_fund_nav(raw)
        return clean_stock_data(normalized, trade_dates=trade_dates)

    raise ValueError(f"不支持的资产类型：{asset_type}（symbol={symbol}）")
