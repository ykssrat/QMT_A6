"""
临时测试：验证止损信号去重————相同标的不同时出现在止损和 Y 因子卖出中
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from scripts.strategy.livermore import LivermoreStrategy, Portfolio, Position

# 场景：159309 亏损 -15%（触发止损），同时 600108 满足加仓但现金不足（触发 Y 因子）
p = Portfolio(cash=500)
p.positions["159309"] = Position("159309", cost_price=1.925, shares=1700, peak_price=1.626)
p.positions["600108"] = Position("600108", cost_price=5.995, shares=200, peak_price=6.96, add_unlocked=True)

prices = {"159309": 1.626, "600108": 6.96}
z_scores = {"600108": 2.0}

strat = LivermoreStrategy()
sigs = strat.generate_signals(p, prices, z_scores)

for sig in sigs:
    print(f"[{sig['action'].upper()}] {sig['symbol']}  {sig['reason'][:60]}")

n = sum(1 for s in sigs if s["symbol"] == "159309")
print(f"\n159309 信号数: {n}（预期 1）")
assert n == 1, f"去重失败！159309 出现了 {n} 次"
print("PASS")
