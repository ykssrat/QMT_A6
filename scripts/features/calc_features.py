"""
因子计算模块：基于日线行情计算常用技术指标与量价因子。
所有函数均接受 DataFrame（DatetimeIndex，含 open/high/low/close/volume 列），
返回追加了新列的 DataFrame，不修改原始数据。
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _safe_abs_corr(a: pd.Series, b: pd.Series) -> float:
    """计算绝对相关系数，异常时返回 0。"""
    pair = pd.concat([a, b], axis=1).dropna()
    if len(pair) < 20:
        return 0.0
    corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])
    if pd.isna(corr):
        return 0.0
    return float(abs(corr))


def _select_components_by_pareto(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    """
    对候选指标做质量评估，并剔除被彻底帕累托支配的指标。

    质量维度（越大越好）：
        1) predictiveness: 与未来 1 日收益绝对相关
        2) stability: 稳定性（1 / (1 + 波动率)）
        3) coverage: 有效值覆盖率
    """
    if not candidates:
        return []

    forward_ret = df["close"].pct_change().shift(-1)
    quality: dict[str, tuple[float, float, float]] = {}
    for col in candidates:
        if col not in df.columns:
            continue
        s = df[col]
        predictiveness = _safe_abs_corr(s, forward_ret)
        vol = s.diff().std(skipna=True)
        if pd.isna(vol):
            vol = 0.0
        stability = float(1.0 / (1.0 + float(vol)))
        coverage = float(s.notna().mean())
        quality[col] = (predictiveness, stability, coverage)

    if not quality:
        return candidates

    non_dominated: list[str] = []
    names = list(quality.keys())
    for name in names:
        target = quality[name]
        dominated = False
        for other in names:
            if other == name:
                continue
            probe = quality[other]
            if (
                probe[0] >= target[0]
                and probe[1] >= target[1]
                and probe[2] >= target[2]
                and (
                    probe[0] > target[0]
                    or probe[1] > target[1]
                    or probe[2] > target[2]
                )
            ):
                dominated = True
                break
        if not dominated:
            non_dominated.append(name)

    return non_dominated or candidates


# ────────────────────────── 均线类 ──────────────────────────

def add_ma(df: pd.DataFrame, windows: list[int] = [5, 10, 20, 60]) -> pd.DataFrame:
    """
    添加简单移动平均线（SMA）。

    参数：
        df: 行情 DataFrame，需含 close 列
        windows: 均线窗口列表

    返回：
        追加 ma_N 列的 DataFrame
    """
    df = df.copy()
    for w in windows:
        df[f"ma_{w}"] = df["close"].rolling(window=w, min_periods=1).mean()
    return df


def add_ema(df: pd.DataFrame, windows: list[int] = [12, 26]) -> pd.DataFrame:
    """
    添加指数移动平均线（EMA）。
    """
    df = df.copy()
    for w in windows:
        df[f"ema_{w}"] = df["close"].ewm(span=w, adjust=False).mean()
    return df


# ────────────────────────── 趋势类 ──────────────────────────

def add_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    添加 MACD 指标：DIF、DEA、MACD 柱。

    列名：macd_dif / macd_dea / macd_bar
    """
    df = df.copy()
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["macd_dif"] = ema_fast - ema_slow
    df["macd_dea"] = df["macd_dif"].ewm(span=signal, adjust=False).mean()
    df["macd_bar"] = (df["macd_dif"] - df["macd_dea"]) * 2
    return df


def add_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """
    添加相对强弱指数（RSI）。

    列名：rsi_N
    """
    df = df.copy()
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=window - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=window - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df[f"rsi_{window}"] = 100 - (100 / (1 + rs))
    return df


def add_boll(
    df: pd.DataFrame,
    window: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    """
    添加布林带（Bollinger Bands）：中轨、上轨、下轨。

    列名：boll_mid / boll_upper / boll_lower
    """
    df = df.copy()
    mid = df["close"].rolling(window=window, min_periods=1).mean()
    std = df["close"].rolling(window=window, min_periods=1).std()
    df["boll_mid"] = mid
    df["boll_upper"] = mid + num_std * std
    df["boll_lower"] = mid - num_std * std
    return df


# ────────────────────────── 动量类 ──────────────────────────

def add_momentum(df: pd.DataFrame, windows: list[int] = [5, 10, 20]) -> pd.DataFrame:
    """
    添加价格动量因子（N 日收益率）。

    列名：mom_N
    """
    df = df.copy()
    for w in windows:
        df[f"mom_{w}"] = df["close"].pct_change(periods=w)
    return df


def add_rate_of_change(df: pd.DataFrame, window: int = 12) -> pd.DataFrame:
    """
    添加变动率指标（ROC）。

    列名：roc_N
    """
    df = df.copy()
    df[f"roc_{window}"] = (df["close"] - df["close"].shift(window)) / df["close"].shift(window)
    return df


# ────────────────────────── 成交量/换手率类 ──────────────────────────

def add_volume_ma(df: pd.DataFrame, windows: list[int] = [5, 20]) -> pd.DataFrame:
    """
    添加成交量均线。

    列名：vol_ma_N
    """
    df = df.copy()
    for w in windows:
        df[f"vol_ma_{w}"] = df["volume"].rolling(window=w, min_periods=1).mean()
    return df


def add_volume_ratio(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    添加量比因子：当日成交量 / 近 N 日平均成交量。

    列名：vol_ratio
    """
    df = df.copy()
    avg_vol = df["volume"].rolling(window=window, min_periods=1).mean()
    df["vol_ratio"] = df["volume"] / avg_vol.replace(0, np.nan)
    return df


# ────────────────────────── 信心因子 Z ──────────────────────────

def calc_confidence_z(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算综合信心因子 Z（用于 Livermore 入场判断）。

    Z 由以下因子线性合成（等权）：
        - 短期动量 mom_5（标准化）
        - MACD 柱 macd_bar（标准化）
        - 量比 vol_ratio（标准化）

    为避免低质量指标污染信号，会先做指标质量评估：
        - 若某指标在预测性、稳定性、覆盖率三维上被其他指标彻底帕累托支配
        - 则该指标会被剔除，不参与当期 confidence_z 合成

    列名：confidence_z

    注意：本函数依赖 add_momentum / add_macd / add_volume_ratio 已被调用。
    """
    required = ["mom_5", "macd_bar", "vol_ratio"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"缺少前置因子列：{missing}，请先调用对应 add_* 函数")

    df = df.copy()
    selected = _select_components_by_pareto(df, required)
    if not selected:
        selected = required

    components = []
    for col in selected:
        s = df[col]
        std = s.rolling(window=60, min_periods=10).std()
        mean = s.rolling(window=60, min_periods=10).mean()
        z = (s - mean) / std.replace(0, np.nan)
        components.append(z)

    df["confidence_z"] = pd.concat(components, axis=1).mean(axis=1)
    return df


# ────────────────────────── 便捷汇总函数 ──────────────────────────

def build_all_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    一键计算所有因子，返回追加了全部因子列的 DataFrame。

    参数：
        df: 含 open/high/low/close/volume 的日线行情

    返回：
        追加因子列后的 DataFrame
    """
    if df is None or df.empty:
        logger.warning("输入数据为空，跳过因子计算")
        return df

    df = add_ma(df)
    df = add_ema(df)
    df = add_macd(df)
    df = add_rsi(df)
    df = add_boll(df)
    df = add_momentum(df)
    df = add_rate_of_change(df)
    df = add_volume_ma(df)
    df = add_volume_ratio(df)
    df = calc_confidence_z(df)
    return df


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[2]))
    from scripts.processed.fetch_data import fetch_stock_price
    from scripts.processed.clean_data import clean_stock_data

    raw = fetch_stock_price("000001", "2022-01-01", "2023-12-31")
    cleaned = clean_stock_data(raw)
    if cleaned is not None:
        result = build_all_features(cleaned)
        print(result[["close", "ma_20", "macd_bar", "rsi_14", "confidence_z"]].tail(10))
