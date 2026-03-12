"""
Livermore 策略核心模块。

规则摘要（参数来自 configs/strategy_config.yaml）：
    m  - 初始建仓比例（占总资金）
    c  - 止损 / 回调阈值
    h  - 盈利加仓解锁阈值
    k  - 加仓比例系数，加仓比 a = k * r

决策流程：
    1. 建仓：信心因子 Z >= threshold 时，按 m 比例建仓。
    2. 优胜劣汰（Y 因子）：资金不足时，卖出评分最差的持仓补足资金。
    3. 止损：亏损率 >= c 时立即清仓。
    4. 盈利加仓：盈利率 r >= h，且价格未回调超过 c，执行加仓 a = k * r。
"""

import logging
import os
import yaml
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class Position:
    """单只标的持仓信息。"""
    symbol: str
    cost_price: float          # 持仓均价（元/股或元/份）
    shares: float              # 持仓数量（股票用股，基金用份额）
    peak_price: float          # 持仓期间最高价（用于回调判断）
    add_unlocked: bool = False  # 是否已解锁加仓权限

    @property
    def market_value(self) -> float:
        """持仓市值。"""
        return self.cost_price * self.shares

    def profit_rate(self, current_price: float) -> float:
        """当前盈利率。"""
        if self.cost_price <= 0:
            return 0.0
        return (current_price - self.cost_price) / self.cost_price

    def drawdown_from_peak(self, current_price: float) -> float:
        """从持仓最高价的回调幅度（正值表示回调）。"""
        if self.peak_price <= 0:
            return 0.0
        return (self.peak_price - current_price) / self.peak_price

    def update_peak(self, current_price: float) -> None:
        """更新持仓期间最高价。"""
        if current_price > self.peak_price:
            self.peak_price = current_price


@dataclass
class Portfolio:
    """组合状态：现金 + 持仓字典。"""
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)

    @property
    def total_market_value(self) -> float:
        """所有持仓市值合计。"""
        return sum(p.market_value for p in self.positions.values())

    @property
    def total_assets(self) -> float:
        """总资产（现金 + 持仓市值）。"""
        return self.cash + self.total_market_value

    def position_profit_rates(self, prices: dict[str, float]) -> dict[str, float]:
        """计算各持仓当前盈利率，用于 Y 因子排序。"""
        return {
            sym: pos.profit_rate(prices.get(sym, pos.cost_price))
            for sym, pos in self.positions.items()
        }


class LivermoreStrategy:
    """
    实现 Livermore 建仓 / 止损 / 加仓决策逻辑。

    用法：
        strategy = LivermoreStrategy()
        signals = strategy.generate_signals(portfolio, prices, confidence_scores)
    """

    def __init__(self) -> None:
        cfg = _load_config()
        lv = cfg["livermore"]
        self.m: float = lv["m"]       # 建仓比例
        self.c: float = lv["c"]       # 止损/回调阈值
        self.h: float = lv["h"]       # 加仓解锁阈值
        self.k: float = lv["k"]       # 加仓系数
        self.z_threshold: float = cfg["signal"]["confidence_threshold"]
        self.max_positions: int = cfg["capital"]["max_position_count"]

    # ────────────── 对外主接口 ──────────────

    def generate_signals(
        self,
        portfolio: Portfolio,
        prices: dict[str, float],
        confidence_scores: dict[str, float],
    ) -> list[dict]:
        """
        根据当前组合状态、最新价格和信心因子，生成交易信号列表。

        参数：
            portfolio: 当前组合（含现金与持仓）
            prices: {symbol: 当前价格}
            confidence_scores: {symbol: 信心因子 Z 值}

        返回：
            信号列表，每个信号为字典，包含：
                symbol   - 标的代码
                action   - "buy" / "sell" / "add"
                reason   - 信号原因描述
                amount   - 建议出入金额（元）
        """
        signals: list[dict] = []
        planned_sell_symbols: set[str] = set()
        position_state: dict[str, tuple[Position, float, float, float]] = {}

        # 1. 先统一更新峰值，并预先标记止损持仓，避免同一标的被 Y 因子与止损重复卖出
        for sym, pos in list(portfolio.positions.items()):
            price = prices.get(sym)
            if price is None:
                continue

            pos.update_peak(price)
            profit = pos.profit_rate(price)
            drawdown = pos.drawdown_from_peak(price)
            position_state[sym] = (pos, price, profit, drawdown)

            if profit <= -self.c:
                signals.append({
                    "symbol": sym,
                    "action": "sell",
                    "reason": f"止损：亏损率 {profit:.2%} >= {self.c:.2%}",
                    "amount": pos.market_value,
                })
                planned_sell_symbols.add(sym)

        # 2. 再处理剩余持仓的解锁与加仓逻辑
        for sym, (pos, _price, profit, drawdown) in position_state.items():
            if sym in planned_sell_symbols:
                continue

            # ── 解锁加仓 ──
            if profit >= self.h:
                pos.add_unlocked = True

            # ── 加仓 ──
            if pos.add_unlocked and drawdown <= self.c:
                add_ratio = self.k * profit
                add_amount = portfolio.total_assets * add_ratio
                if add_amount > portfolio.cash:
                    # 现金不足，触发 Y 因子优化
                    y_signals = self._y_factor_sell(
                        portfolio,
                        prices,
                        required_amount=add_amount - portfolio.cash,
                        excluded_symbols=planned_sell_symbols | {sym},
                    )
                    signals.extend(y_signals)
                    planned_sell_symbols.update(sig["symbol"] for sig in y_signals)
                signals.append({
                    "symbol": sym,
                    "action": "add",
                    "reason": f"盈利加仓：盈利率 {profit:.2%}，回调 {drawdown:.2%}，加仓比 {add_ratio:.2%}",
                    "amount": add_amount,
                })
                pos.add_unlocked = False  # 加仓后重置，防止连续加仓

        # 2. 扫描待入场的新标的
        existing_symbols = set(portfolio.positions.keys())
        for sym, z in confidence_scores.items():
            if sym in existing_symbols:
                continue
            if len(portfolio.positions) >= self.max_positions:
                break
            if z < self.z_threshold:
                continue

            build_amount = portfolio.total_assets * self.m
            if build_amount > portfolio.cash:
                y_signals = self._y_factor_sell(
                    portfolio,
                    prices,
                    required_amount=build_amount - portfolio.cash,
                    excluded_symbols=planned_sell_symbols,
                )
                signals.extend(y_signals)
                planned_sell_symbols.update(sig["symbol"] for sig in y_signals)

            signals.append({
                "symbol": sym,
                "action": "buy",
                "reason": f"建仓：信心因子 Z={z:.2f} >= 阈值 {self.z_threshold}",
                "amount": build_amount,
            })

        return signals

    # ────────────── 内部方法 ──────────────

    def _y_factor_sell(
        self,
        portfolio: Portfolio,
        prices: dict[str, float],
        required_amount: float,
        excluded_symbols: set[str] | None = None,
    ) -> list[dict]:
        """
        Y 因子优化：按盈利率从低到高排序，依次卖出持仓以补足所需资金。

        参数：
            required_amount: 需要补足的资金量（元）
            excluded_symbols: 已经计划卖出的标的集合，避免重复生成卖出信号

        返回：
            卖出信号列表
        """
        if required_amount <= 0:
            return []

        excluded_symbols = excluded_symbols or set()
        profit_rates = portfolio.position_profit_rates(prices)
        # 按盈利率升序排列（最差的先卖）
        sorted_positions = sorted(profit_rates.items(), key=lambda x: x[1])

        signals = []
        accumulated = 0.0
        for sym, rate in sorted_positions:
            if sym in excluded_symbols:
                continue
            if accumulated >= required_amount:
                break
            pos = portfolio.positions[sym]
            current_price = prices.get(sym, pos.cost_price)
            sell_value = pos.shares * current_price
            signals.append({
                "symbol": sym,
                "action": "sell",
                "reason": f"Y 因子清仓：补足资金，持仓盈利率 {rate:.2%}",
                "amount": sell_value,
            })
            accumulated += sell_value
            excluded_symbols.add(sym)

        return signals
