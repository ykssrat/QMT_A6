"""
组合优化模块：对多标的持仓进行权重分配。

支持两种方式：
  - equal_weight：等权分配，每个标的权重相同
  - risk_parity：风险平价（逆波动率简化版），权重与历史波动率成反比
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def equal_weight(symbols: list[str]) -> dict[str, float]:
    """
    等权分配：所有标的权重相同。

    参数：
        symbols: 标的代码列表

    返回：
        {symbol: weight}，所有权重之和为 1.0
    """
    if not symbols:
        return {}
    w = 1.0 / len(symbols)
    return {sym: round(w, 6) for sym in symbols}


def risk_parity(returns: pd.DataFrame) -> dict[str, float]:
    """
    风险平价（逆波动率简化版）：权重与各标的历史波动率成反比，
    使各标的对组合风险的贡献近似相等。

    参数：
        returns: 日收益率 DataFrame，列名为标的代码，DatetimeIndex

    返回：
        {symbol: weight}，所有权重之和为 1.0
    """
    if returns.empty:
        return {}

    vols = returns.std().replace(0, np.nan).dropna()
    if vols.empty:
        logger.warning("所有标的波动率为零，回退为等权分配")
        return equal_weight(list(returns.columns))

    inv_vols = 1.0 / vols
    weights  = inv_vols / inv_vols.sum()
    return {sym: round(float(w), 6) for sym, w in weights.items()}


def calc_returns(prices_map: dict[str, pd.Series]) -> pd.DataFrame:
    """
    将多标的价格序列转换为对齐后的日收益率矩阵。

    参数：
        prices_map: {symbol: close_price_series}

    返回：
        日收益率 DataFrame（行：日期，列：标的代码），对齐并丢弃 NaN 行
    """
    if not prices_map:
        return pd.DataFrame()

    price_df  = pd.DataFrame(prices_map)
    return_df = price_df.pct_change().dropna(how="all")
    return return_df


def allocate_capital(
    portfolio_value: float,
    weights: dict[str, float],
    prices: dict[str, float],
    lot_size: int = 100,
) -> dict[str, int]:
    """
    根据目标权重和当前价格，将资金分配为整手股数。

    参数：
        portfolio_value: 总资金（元）
        weights:         各标的目标权重
        prices:          各标的当前价格
        lot_size:        最小交易单位（默认 100 股/手）

    返回：
        {symbol: shares}，shares 为整手数（lot_size 的整数倍）
    """
    if portfolio_value <= 0:
        raise ValueError("portfolio_value 必须大于 0")

    allocation: dict[str, int] = {}
    for sym, w in weights.items():
        price = prices.get(sym)
        if not price or price <= 0:
            logger.warning("标的 %s 价格无效，跳过分配", sym)
            continue
        target_amount = portfolio_value * w
        shares = int(target_amount / price / lot_size) * lot_size
        if shares > 0:
            allocation[sym] = shares
    return allocation


def rebalance(
    portfolio_value: float,
    symbols: list[str],
    prices: dict[str, float],
    returns: pd.DataFrame | None = None,
    method: str = "equal_weight",
) -> dict[str, int]:
    """
    一键执行组合再平衡：计算权重 → 分配整手股数。

    参数：
        portfolio_value: 总资金（元）
        symbols:         目标持仓标的列表
        prices:          各标的当前价格
        returns:         历史日收益率矩阵（risk_parity 方式需要）
        method:          权重方法，"equal_weight" 或 "risk_parity"

    返回：
        {symbol: shares}
    """
    if method == "equal_weight":
        weights = equal_weight(symbols)
    elif method == "risk_parity":
        if returns is None or returns.empty:
            logger.warning("risk_parity 方式缺少 returns，回退为 equal_weight")
            weights = equal_weight(symbols)
        else:
            weights = risk_parity(returns[symbols] if all(s in returns.columns for s in symbols) else returns)
    else:
        raise ValueError(f"不支持的权重方法：{method}，可选 equal_weight / risk_parity")

    return allocate_capital(portfolio_value, weights, prices)


if __name__ == "__main__":
    # 简单功能验证
    syms = ["000001", "600519", "300750"]
    print("等权：", equal_weight(syms))

    # 构造模拟收益率
    import numpy as np
    np.random.seed(0)
    fake_returns = pd.DataFrame(
        np.random.randn(100, 3) * 0.01,
        columns=syms,
    )
    print("风险平价：", risk_parity(fake_returns))

    prices = {"000001": 12.5, "600519": 1800.0, "300750": 250.0}
    print("分配股数：", allocate_capital(1_000_000, equal_weight(syms), prices))
