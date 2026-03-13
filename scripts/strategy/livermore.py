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
import math
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

    def __init__(self, params: dict | None = None) -> None:
        cfg = _load_config()
        params = params or {}
        lv = cfg["livermore"]
        lv_asset_params = (lv.get("asset_params") or {})

        stock_lv = (lv_asset_params.get("stock") or lv_asset_params.get("exchange") or {})
        etf_lv = (lv_asset_params.get("etf") or lv_asset_params.get("exchange") or stock_lv)
        fund_lv = (lv_asset_params.get("fund_open") or stock_lv)

        # 默认值优先从 stock 分组读取，再回退到旧键与硬编码常量
        self.m: float = float(params.get("m", stock_lv.get("m", lv.get("m", 0.1))))
        self.c: float = float(params.get("c", stock_lv.get("c", lv.get("c", 0.07))))
        self.h: float = float(params.get("h", stock_lv.get("h", lv.get("h", 0.10))))
        self.k: float = float(params.get("k", stock_lv.get("k", lv.get("k", 0.5))))
        self.max_positions: int = int(params.get("max_positions", cfg["capital"]["max_position_count"]))

        # 按资产类型参数（场内/场外两套），缺失时回退到默认参数，兼容旧配置
        param_asset_params = (params.get("asset_params") or {})
        param_exchange = (param_asset_params.get("exchange") or {})
        param_stock = (param_asset_params.get("stock") or param_exchange)
        param_etf = (param_asset_params.get("etf") or param_exchange)
        param_fund = (param_asset_params.get("fund_open") or {})

        stock_defaults = {
            "m": float(stock_lv.get("m", self.m)),
            "c": float(stock_lv.get("c", self.c)),
            "h": float(stock_lv.get("h", self.h)),
            "k": float(stock_lv.get("k", self.k)),
        }
        etf_defaults = {
            "m": float(etf_lv.get("m", stock_defaults["m"])),
            "c": float(etf_lv.get("c", stock_defaults["c"])),
            "h": float(etf_lv.get("h", stock_defaults["h"])),
            "k": float(etf_lv.get("k", stock_defaults["k"])),
        }
        fund_defaults = {
            "m": float(fund_lv.get("m", stock_defaults["m"])),
            "c": float(fund_lv.get("c", stock_defaults["c"])),
            "h": float(fund_lv.get("h", stock_defaults["h"])),
            "k": float(fund_lv.get("k", stock_defaults["k"])),
        }

        self.asset_params: dict[str, dict[str, float]] = {
            "stock": {
                "m": float(
                    param_stock.get(
                        "m", stock_defaults["m"]
                    )
                ),
                "c": float(
                    param_stock.get(
                        "c", stock_defaults["c"]
                    )
                ),
                "h": float(
                    param_stock.get(
                        "h", stock_defaults["h"]
                    )
                ),
                "k": float(
                    param_stock.get(
                        "k", stock_defaults["k"]
                    )
                ),
            },
            "etf": {
                "m": float(
                    param_etf.get(
                        "m", etf_defaults["m"]
                    )
                ),
                "c": float(
                    param_etf.get(
                        "c", etf_defaults["c"]
                    )
                ),
                "h": float(
                    param_etf.get(
                        "h", etf_defaults["h"]
                    )
                ),
                "k": float(
                    param_etf.get(
                        "k", etf_defaults["k"]
                    )
                ),
            },
            "fund_open": {
                "m": float(
                    param_fund.get(
                        "m", fund_defaults["m"]
                    )
                ),
                "c": float(
                    param_fund.get(
                        "c", fund_defaults["c"]
                    )
                ),
                "h": float(
                    param_fund.get(
                        "h", fund_defaults["h"]
                    )
                ),
                "k": float(
                    param_fund.get(
                        "k", fund_defaults["k"]
                    )
                ),
            },
        }

    def _asset_group(self, asset_type: str | None) -> str:
        """将资产类型映射为参数组。"""
        if asset_type == "fund_open":
            return "fund_open"
        if asset_type in {"etf", "lof"}:
            return "etf"
        return "stock"

    def _param_for_symbol(self, symbol: str, asset_types: dict[str, str], key: str) -> float:
        """获取单标的参数值。"""
        group = self._asset_group(asset_types.get(symbol))
        return float(self.asset_params[group][key])

    # ────────────── 对外主接口 ──────────────

    def generate_signals(
        self,
        portfolio: Portfolio,
        prices: dict[str, float],
        confidence_scores: dict[str, float],
        asset_types: dict[str, str] | None = None,
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
        asset_types = asset_types or {}

        market_regime_by_group = {
            "stock": self._compute_market_regime(
                confidence_scores=confidence_scores,
                include_groups={"stock"},
                asset_types=asset_types,
            ),
            "etf": self._compute_market_regime(
                confidence_scores=confidence_scores,
                include_groups={"etf"},
                asset_types=asset_types,
            ),
            "fund_open": self._compute_market_regime(
                confidence_scores=confidence_scores,
                include_groups={"fund_open"},
                asset_types=asset_types,
            ),
        }

        # 1. 先统一更新峰值，并预先标记止损持仓，避免同一标的被 Y 因子与止损重复卖出
        for sym, pos in list(portfolio.positions.items()):
            price = prices.get(sym)
            if price is None:
                continue

            pos.update_peak(price)
            profit = pos.profit_rate(price)
            drawdown = pos.drawdown_from_peak(price)
            position_state[sym] = (pos, price, profit, drawdown)
            c_for_symbol = self._param_for_symbol(sym, asset_types, "c")

            if profit <= (-c_for_symbol + 1e-12):
                signals.append({
                    "symbol": sym,
                    "action": "sell",
                    "reason": f"止损：亏损率 {profit:.2%} >= {c_for_symbol:.2%}",
                    "amount": pos.market_value,
                })
                planned_sell_symbols.add(sym)

        # 2. 再处理剩余持仓的解锁与加仓逻辑
        for sym, (pos, _price, profit, drawdown) in position_state.items():
            if sym in planned_sell_symbols:
                continue

            h_for_symbol = self._param_for_symbol(sym, asset_types, "h")
            c_for_symbol = self._param_for_symbol(sym, asset_types, "c")
            k_for_symbol = self._param_for_symbol(sym, asset_types, "k")
            group = self._asset_group(asset_types.get(sym))
            y_for_group = float(market_regime_by_group[group]["market_y"])
            y_trigger_for_group = float(market_regime_by_group[group]["y_trigger"])

            # ── 解锁加仓 ──
            was_unlocked = pos.add_unlocked
            if profit >= h_for_symbol:
                pos.add_unlocked = True

            # ── 加仓 ──
            # 仅当此前已经解锁时执行加仓，避免同一根K线“刚解锁就立刻加仓”
            if was_unlocked and drawdown <= c_for_symbol:
                add_ratio = k_for_symbol * profit
                add_amount = portfolio.total_assets * add_ratio

                # Y 因子资金决策：
                # - Y >= 阈值：卖出最差持仓做转仓
                # - Y <  阈值：仅使用现有现金，不强制补足
                planned_amount, y_signals = self._plan_amount_with_y_factor(
                    target_amount=add_amount,
                    cash=portfolio.cash,
                    portfolio=portfolio,
                    prices=prices,
                    market_y=y_for_group,
                    y_trigger=y_trigger_for_group,
                    excluded_symbols=planned_sell_symbols | {sym},
                )

                if y_signals:
                    signals.extend(y_signals)
                    planned_sell_symbols.update(sig["symbol"] for sig in y_signals)

                if planned_amount <= 0:
                    continue

                signals.append({
                    "symbol": sym,
                    "action": "add",
                    "reason": (
                        f"盈利加仓：盈利率 {profit:.2%}，回调 {drawdown:.2%}，加仓比 {add_ratio:.2%}，"
                        f"Y={y_for_group:.2f}（动态触发线 {y_trigger_for_group:.2f}）"
                    ),
                    "amount": planned_amount,
                })
                pos.add_unlocked = False  # 加仓后重置，防止连续加仓

        # 2. 扫描待入场的新标的
        existing_symbols = set(portfolio.positions.keys())
        for sym, z in confidence_scores.items():
            if sym in existing_symbols:
                continue
            if len(portfolio.positions) >= self.max_positions:
                break
            group = self._asset_group(asset_types.get(sym))
            z_for_symbol = float(market_regime_by_group[group]["entry_z"])
            m_for_symbol = self._param_for_symbol(sym, asset_types, "m")
            y_trigger_for_group = float(market_regime_by_group[group]["y_trigger"])
            y_for_group = float(market_regime_by_group[group]["market_y"])

            if z < z_for_symbol:
                continue

            build_amount = portfolio.total_assets * m_for_symbol
            planned_amount, y_signals = self._plan_amount_with_y_factor(
                target_amount=build_amount,
                cash=portfolio.cash,
                portfolio=portfolio,
                prices=prices,
                market_y=y_for_group,
                y_trigger=y_trigger_for_group,
                excluded_symbols=planned_sell_symbols,
            )

            if y_signals:
                signals.extend(y_signals)
                planned_sell_symbols.update(sig["symbol"] for sig in y_signals)

            if planned_amount <= 0:
                continue

            signals.append({
                "symbol": sym,
                "action": "buy",
                "reason": (
                    f"建仓：信心因子 Z={z:.2f} >= 动态阈值 {z_for_symbol:.2f}，"
                    f"Y={y_for_group:.2f}（动态触发线 {y_trigger_for_group:.2f}）"
                ),
                "amount": planned_amount,
            })

        return signals

    # ────────────── 内部方法 ──────────────

    def _compute_market_regime(
        self,
        confidence_scores: dict[str, float],
        include_groups: set[str],
        asset_types: dict[str, str],
    ) -> dict[str, float]:
        """
        基于全市场信号动态合成 Z/Y 两个决策因子。

        返回：
            entry_z   - 入场动态阈值（分位数）
            market_y  - 市场强度（sigmoid 聚合）
            y_trigger - 触发转仓所需市场强度（动态）
        """
        if not confidence_scores:
            return {"entry_z": 0.0, "market_y": 0.0, "y_trigger": 0.6}

        values = [
            z
            for sym, z in confidence_scores.items()
            if self._asset_group(asset_types.get(sym)) in include_groups
        ]
        if not values:
            return {"entry_z": 0.0, "market_y": 0.0, "y_trigger": 0.6}

        sorted_values = sorted(float(v) for v in values)
        q_idx = int(round((len(sorted_values) - 1) * 0.65))
        entry_z = sorted_values[max(0, min(q_idx, len(sorted_values) - 1))]

        transformed = [1.0 / (1.0 + math.exp(-(z - entry_z))) for z in sorted_values]
        market_y = float(sum(transformed) / len(transformed))

        bullish_ratio = float(sum(1 for z in sorted_values if z >= entry_z) / len(sorted_values))
        y_trigger = 0.55 + (0.5 - bullish_ratio) * 0.2
        y_trigger = float(max(0.45, min(0.75, y_trigger)))

        return {
            "entry_z": float(entry_z),
            "market_y": market_y,
            "y_trigger": y_trigger,
        }

    def _plan_amount_with_y_factor(
        self,
        target_amount: float,
        cash: float,
        portfolio: Portfolio,
        prices: dict[str, float],
        market_y: float,
        y_trigger: float,
        excluded_symbols: set[str] | None = None,
    ) -> tuple[float, list[dict]]:
        """
        根据 Y 因子决定资金方案。

        规则：
            - 目标金额 <= 现金：直接使用目标金额
            - Y >= 动态触发线：卖出最差持仓 1 只后转仓
            - Y < 动态触发线：仅使用现有现金，不补齐
        """
        if target_amount <= 0:
            return 0.0, []

        if target_amount <= cash:
            return target_amount, []

        if market_y >= y_trigger:
            y_signals = self._y_factor_rotate_one(
                portfolio=portfolio,
                prices=prices,
                excluded_symbols=excluded_symbols,
            )
            if not y_signals:
                return cash, []
            rotated_cash = cash + sum(sig["amount"] for sig in y_signals)
            return min(target_amount, rotated_cash), y_signals

        return cash, []

    def _y_factor_rotate_one(
        self,
        portfolio: Portfolio,
        prices: dict[str, float],
        excluded_symbols: set[str] | None = None,
    ) -> list[dict]:
        """
        Y 因子转仓：卖出当前最差持仓（仅 1 只），为新建仓/加仓提供资金。

        参数：
            excluded_symbols: 已经计划卖出的标的集合，避免重复生成卖出信号

        返回：
            长度为 0 或 1 的卖出信号列表
        """
        excluded_symbols = excluded_symbols or set()
        profit_rates = portfolio.position_profit_rates(prices)
        candidates = [(sym, rate) for sym, rate in profit_rates.items() if sym not in excluded_symbols]
        if not candidates:
            return []

        worst_sym, worst_rate = min(candidates, key=lambda x: x[1])
        pos = portfolio.positions[worst_sym]
        current_price = prices.get(worst_sym, pos.cost_price)
        sell_value = pos.shares * current_price
        return [{
            "symbol": worst_sym,
            "action": "sell",
            "reason": f"Y 因子转仓：市场偏强，卖出最差持仓（盈利率 {worst_rate:.2%}）",
            "amount": sell_value,
        }]
