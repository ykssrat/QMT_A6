"""
Livermore 策略核心逻辑的单元测试。

使用 unittest.mock.patch 屏蔽配置文件读取，确保测试完全离线、参数可控。
"""

import pytest
from unittest.mock import patch

from scripts.strategy.livermore import LivermoreStrategy, Portfolio, Position

# ─── 测试用固定策略参数 ────────────────────────────────────────────────────────
_TEST_CONFIG = {
    "livermore": {
        "m": 0.1,    # 建仓比例 10%
        "c": 0.07,   # 止损/回调阈值 7%
        "h": 0.10,   # 加仓解锁盈利阈值 10%
        "k": 0.5,    # 加仓系数
        "y_threshold": 0.55,  # Y 因子阈值
    },
    "signal": {"confidence_threshold": 1.5},
    "capital": {"max_position_count": 10},
}


@pytest.fixture
def strategy() -> LivermoreStrategy:
    """使用固定测试参数创建策略实例，屏蔽真实配置文件。"""
    with patch("scripts.strategy.livermore._load_config", return_value=_TEST_CONFIG):
        yield LivermoreStrategy()


# ─── Position 数值计算 ────────────────────────────────────────────────────────

class TestPosition:
    def test_profit_rate_gain(self):
        """盈利情形：profit_rate = (12 - 10) / 10 = 0.2。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=10.0)
        assert abs(pos.profit_rate(12.0) - 0.2) < 1e-9

    def test_profit_rate_loss(self):
        """亏损情形：profit_rate = (8 - 10) / 10 = -0.2。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=10.0)
        assert abs(pos.profit_rate(8.0) - (-0.2)) < 1e-9

    def test_profit_rate_zero_cost_returns_zero(self):
        """cost_price 为 0 时返回 0.0，不抛异常。"""
        pos = Position(symbol="000001", cost_price=0.0, shares=100, peak_price=0.0)
        assert pos.profit_rate(10.0) == 0.0

    def test_drawdown_from_peak(self):
        """回撤 = (12 - 11) / 12 ≈ 0.0833。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=12.0)
        expected = (12.0 - 11.0) / 12.0
        assert abs(pos.drawdown_from_peak(11.0) - expected) < 1e-9

    def test_drawdown_at_peak_is_zero(self):
        """当前价等于峰值时回撤应为 0。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=12.0)
        assert abs(pos.drawdown_from_peak(12.0)) < 1e-9

    def test_update_peak_increases(self):
        """update_peak：新高时更新峰值。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=10.0)
        pos.update_peak(15.0)
        assert pos.peak_price == 15.0

    def test_update_peak_not_decreases(self):
        """update_peak：价格下跌时峰值不降低。"""
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=15.0)
        pos.update_peak(12.0)
        assert pos.peak_price == 15.0


# ─── Portfolio 数值计算 ───────────────────────────────────────────────────────

class TestPortfolio:
    def test_total_assets_cash_only(self):
        """无持仓时，total_assets = cash。"""
        portfolio = Portfolio(cash=100_000.0)
        assert portfolio.total_assets == 100_000.0

    def test_total_assets_with_position(self):
        """total_assets = cash + 持仓市值。"""
        portfolio = Portfolio(cash=50_000.0)
        portfolio.positions["000001"] = Position(
            symbol="000001", cost_price=10.0, shares=1000, peak_price=10.0
        )
        # market_value 使用 cost_price，total_assets = 50000 + 10*1000 = 60000
        assert abs(portfolio.total_assets - 60_000.0) < 1e-6


# ─── 建仓信号 ─────────────────────────────────────────────────────────────────

class TestBuySignal:
    def test_buy_signal_when_z_above_threshold(self, strategy):
        """Z=2.0 > 阈值 1.5，应生成 buy 信号。"""
        portfolio = Portfolio(cash=100_000.0)
        signals = strategy.generate_signals(portfolio, {"000001": 10.0}, {"000001": 2.0})
        buy_signals = [s for s in signals if s["action"] == "buy"]
        assert len(buy_signals) == 1
        assert buy_signals[0]["symbol"] == "000001"

    def test_no_buy_when_z_below_threshold(self, strategy):
        """Z=1.0 < 阈值 1.5，不应生成 buy 信号。"""
        portfolio = Portfolio(cash=100_000.0)
        signals = strategy.generate_signals(portfolio, {"000001": 10.0}, {"000001": 1.0})
        buy_signals = [s for s in signals if s["action"] == "buy"]
        assert len(buy_signals) == 0

    def test_no_buy_when_max_positions_reached(self, strategy):
        """持仓数量已达上限（10），不应生成 buy 信号。"""
        portfolio = Portfolio(cash=100_000.0)
        for i in range(10):
            sym = f"{i:06d}"
            portfolio.positions[sym] = Position(sym, 10.0, 100, 10.0)
        signals = strategy.generate_signals(portfolio, {"999999": 10.0}, {"999999": 3.0})
        buy_signals = [s for s in signals if s["action"] == "buy"]
        assert len(buy_signals) == 0


# ─── 止损信号 ─────────────────────────────────────────────────────────────────

class TestStopLoss:
    def test_stop_loss_triggered(self, strategy):
        """亏损 10% > c=7%，应触发止损 sell 信号。"""
        portfolio = Portfolio(cash=0.0)
        portfolio.positions["000001"] = Position(
            symbol="000001", cost_price=10.0, shares=100, peak_price=10.0
        )
        signals = strategy.generate_signals(portfolio, {"000001": 9.0}, {})
        sell_sigs = [s for s in signals if s["action"] == "sell" and s["symbol"] == "000001"]
        assert len(sell_sigs) == 1

    def test_stop_loss_not_triggered_within_tolerance(self, strategy):
        """亏损 5% < c=7%，不应触发止损。"""
        portfolio = Portfolio(cash=0.0)
        portfolio.positions["000001"] = Position(
            symbol="000001", cost_price=10.0, shares=100, peak_price=10.0
        )
        signals = strategy.generate_signals(portfolio, {"000001": 9.5}, {})
        sell_sigs = [s for s in signals if s["action"] == "sell" and s["symbol"] == "000001"]
        assert len(sell_sigs) == 0

    def test_stop_loss_at_exact_threshold(self, strategy):
        """亏损精确等于 c=7%（即 profit_rate = -0.07），应触发止损。"""
        portfolio = Portfolio(cash=0.0)
        portfolio.positions["000001"] = Position(
            symbol="000001", cost_price=10.0, shares=100, peak_price=10.0
        )
        # profit_rate = (9.3 - 10) / 10 = -0.07
        signals = strategy.generate_signals(portfolio, {"000001": 9.3}, {})
        sell_sigs = [s for s in signals if s["action"] == "sell" and s["symbol"] == "000001"]
        assert len(sell_sigs) == 1


# ─── 加仓信号 ─────────────────────────────────────────────────────────────────

class TestAddPosition:
    def test_add_position_when_unlocked_and_price_at_peak(self, strategy):
        """
        add_unlocked=True，当前价等于峰值（回撤=0% <= c=7%），
        且盈利 20% >= h=10%，应生成 add 信号。
        """
        portfolio = Portfolio(cash=100_000.0)
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=12.0)
        pos.add_unlocked = True
        portfolio.positions["000001"] = pos
        signals = strategy.generate_signals(portfolio, {"000001": 12.0}, {})
        add_sigs = [s for s in signals if s["action"] == "add"]
        assert len(add_sigs) == 1
        assert add_sigs[0]["symbol"] == "000001"

    def test_add_position_blocked_when_drawdown_exceeds_c(self, strategy):
        """
        回撤 (12-11)/12 ≈ 8.3% > c=7%，即使 add_unlocked=True 也不应加仓。
        """
        portfolio = Portfolio(cash=100_000.0)
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=12.0)
        pos.add_unlocked = True
        portfolio.positions["000001"] = pos
        signals = strategy.generate_signals(portfolio, {"000001": 11.0}, {})
        add_sigs = [s for s in signals if s["action"] == "add"]
        assert len(add_sigs) == 0

    def test_add_position_requires_unlocked_flag(self, strategy):
        """add_unlocked=False 时即使盈利，也不应加仓。"""
        portfolio = Portfolio(cash=100_000.0)
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=12.0)
        pos.add_unlocked = False
        portfolio.positions["000001"] = pos
        signals = strategy.generate_signals(portfolio, {"000001": 12.0}, {})
        add_sigs = [s for s in signals if s["action"] == "add"]
        assert len(add_sigs) == 0

    def test_unlock_trigger(self, strategy):
        """当盈利率 >= h=10% 时，应将 add_unlocked 置为 True（先止损检查后解锁）。"""
        portfolio = Portfolio(cash=0.0)
        pos = Position(symbol="000001", cost_price=10.0, shares=100, peak_price=10.0)
        pos.add_unlocked = False
        portfolio.positions["000001"] = pos
        # 盈利 15%，但现金为 0 无法加仓（不会生成 add 信号，但应解锁）
        strategy.generate_signals(portfolio, {"000001": 11.5}, {})
        assert pos.add_unlocked is True


# ─── Y 因子资金行为（新规则） ────────────────────────────────────────────────

class TestYFactorBehavior:
    def test_y_low_no_forced_sell_use_cash_only(self, strategy):
        """
        Y < 阈值时，不应触发转仓卖出；资金不足时只使用现有现金。
        """
        portfolio = Portfolio(cash=1_000.0)
        # 提高总资产，确保建仓目标金额 > 现金
        portfolio.positions["000001"] = Position("000001", 10.0, 1000, 10.0)
        portfolio.positions["000002"] = Position("000002", 10.0, 1000, 10.0)

        prices = {"000001": 9.0, "000002": 11.0, "999999": 20.0}
        # 待建仓标的 z 达标，但通过多个弱信号将市场 Y 压低到阈值以下
        z_map = {"999999": 2.0, "weak1": -2.0, "weak2": -2.0, "weak3": -2.0, "weak4": -2.0}
        signals = strategy.generate_signals(portfolio, prices, z_map)

        y_sells = [s for s in signals if s["action"] == "sell" and "Y 因子转仓" in s["reason"]]
        buy_sigs = [s for s in signals if s["action"] == "buy" and s["symbol"] == "999999"]

        assert len(y_sells) == 0
        assert len(buy_sigs) == 1
        assert buy_sigs[0]["amount"] == portfolio.cash

    def test_y_high_rotate_worst_one(self, strategy):
        """
        Y >= 阈值且资金不足时，触发一次 Y 因子转仓，且卖出最差持仓仅 1 只。
        """
        portfolio = Portfolio(cash=1_000.0)
        # 提高总资产，确保建仓目标金额 > 现金
        # 000001 更差（-5%），但不触发止损（c=7%）
        portfolio.positions["000001"] = Position("000001", 10.0, 1000, 10.0)
        portfolio.positions["000002"] = Position("000002", 10.0, 1000, 10.0)

        prices = {"000001": 9.5, "000002": 10.5, "999999": 20.0}
        # z 显著高于阈值 => Y 较高
        z_map = {"999999": 4.0}

        signals = strategy.generate_signals(portfolio, prices, z_map)
        y_sells = [s for s in signals if s["action"] == "sell" and "Y 因子转仓" in s["reason"]]

        assert len(y_sells) == 1
        assert y_sells[0]["symbol"] == "000001"
