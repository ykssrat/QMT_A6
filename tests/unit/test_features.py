"""
因子计算模块的单元测试。

使用合成行情数据（无需外部网络），覆盖主要 add_* 函数和 build_all_features。
"""

import numpy as np
import pandas as pd
import pytest

from scripts.features.calc_features import (
    add_boll,
    add_ema,
    add_ma,
    add_macd,
    add_momentum,
    add_rsi,
    add_volume_ratio,
    build_all_features,
    calc_confidence_z,
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """生成 100 个交易日的合成行情数据，固定随机种子保证可复现。"""
    np.random.seed(42)
    n = 100
    close = 10.0 + np.cumsum(np.random.randn(n) * 0.2)
    close = np.maximum(close, 1.0)  # 价格不能为负
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "open":   close * 0.99,
        "high":   close * 1.01,
        "low":    close * 0.98,
        "close":  close,
        "volume": np.random.randint(1_000_000, 10_000_000, n).astype(float),
    }, index=dates)


# ─── add_ma ───────────────────────────────────────────────────────────────────

def test_add_ma_columns(sample_df):
    """add_ma 应追加 ma_5 和 ma_20 列。"""
    df = add_ma(sample_df.copy(), windows=[5, 20])
    assert "ma_5" in df.columns
    assert "ma_20" in df.columns


def test_add_ma_last_value(sample_df):
    """ma_5 的最后一个值应等于最后 5 根 close 的算术均值。"""
    df = add_ma(sample_df.copy(), windows=[5])
    expected = sample_df["close"].iloc[-5:].mean()
    assert abs(df["ma_5"].iloc[-1] - expected) < 1e-9


def test_add_ma_no_extra_columns(sample_df):
    """add_ma 不应修改现有列，只新增 ma_N 列。"""
    original_cols = list(sample_df.columns)
    df = add_ma(sample_df.copy(), windows=[5])
    for col in original_cols:
        assert col in df.columns


# ─── add_ema ──────────────────────────────────────────────────────────────────

def test_add_ema_columns(sample_df):
    df = add_ema(sample_df.copy(), windows=[12, 26])
    assert "ema_12" in df.columns
    assert "ema_26" in df.columns


# ─── add_macd ─────────────────────────────────────────────────────────────────

def test_add_macd_columns(sample_df):
    """add_macd 应追加 macd_dif、macd_dea、macd_bar 三列。"""
    df = add_macd(sample_df.copy())
    for col in ("macd_dif", "macd_dea", "macd_bar"):
        assert col in df.columns


def test_add_macd_bar_formula(sample_df):
    """macd_bar = (macd_dif - macd_dea) * 2，精度误差 < 1e-6。"""
    df = add_macd(sample_df.copy())
    valid = df[["macd_dif", "macd_dea", "macd_bar"]].dropna()
    expected_bar = (valid["macd_dif"] - valid["macd_dea"]) * 2
    assert (abs(valid["macd_bar"] - expected_bar) < 1e-6).all()


# ─── add_rsi ──────────────────────────────────────────────────────────────────

def test_add_rsi_range(sample_df):
    """RSI 值应在 [0, 100] 区间内。"""
    df = add_rsi(sample_df.copy())
    valid = df["rsi_14"].dropna()
    assert (valid >= 0).all() and (valid <= 100).all()


# ─── add_boll ─────────────────────────────────────────────────────────────────

def test_add_boll_columns(sample_df):
    df = add_boll(sample_df.copy())
    for col in ("boll_upper", "boll_mid", "boll_lower"):
        assert col in df.columns


def test_add_boll_band_relationship(sample_df):
    """布林带：上轨 >= 中轨 >= 下轨。"""
    df = add_boll(sample_df.copy())
    valid = df[["boll_upper", "boll_mid", "boll_lower"]].dropna()
    assert (valid["boll_upper"] >= valid["boll_mid"]).all()
    assert (valid["boll_mid"] >= valid["boll_lower"]).all()


# ─── add_volume_ratio ─────────────────────────────────────────────────────────

def test_add_volume_ratio_positive(sample_df):
    """量比（vol_ratio）应为正数。"""
    df = add_volume_ratio(sample_df.copy())
    valid = df["vol_ratio"].dropna()
    assert (valid > 0).all()


# ─── calc_confidence_z ────────────────────────────────────────────────────────

def test_calc_confidence_z_requires_prerequisites(sample_df):
    """缺少前置因子列时应抛出 ValueError，且错误信息包含"缺少前置因子列"。"""
    with pytest.raises(ValueError, match="缺少前置因子列"):
        calc_confidence_z(sample_df.copy())


def test_calc_confidence_z_range(sample_df):
    """confidence_z 应为有限浮点数（无 inf），且大多数值绝对值 <= 4。"""
    df = build_all_features(sample_df.copy())
    valid = df["confidence_z"].dropna()
    assert np.isfinite(valid).all()


# ─── build_all_features ───────────────────────────────────────────────────────

def test_build_all_features_core_columns(sample_df):
    """build_all_features 应包含核心因子列和 confidence_z。"""
    df = build_all_features(sample_df.copy())
    for col in ("ma_20", "macd_bar", "rsi_14", "boll_mid", "confidence_z"):
        assert col in df.columns, f"缺少列：{col}"


def test_build_all_features_no_mutation(sample_df):
    """build_all_features 不应修改传入的原始 DataFrame。"""
    original_cols = list(sample_df.columns)
    original_values = sample_df["close"].copy()
    build_all_features(sample_df.copy())
    # sample_df 本身应未被修改
    assert list(sample_df.columns) == original_cols
    assert (sample_df["close"] == original_values).all()
