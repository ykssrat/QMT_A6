"""
回测报告入口：按代码独立回测并输出分代码表现表。

默认行为：
- 每个代码独立运行多次实验（默认 100 次）
- 仅使用最近 120 个交易日做滚动回测
- 按收益率 + 夏普 + 胜率的综合得分排序
- 仅输出分代码表，不输出组合总指标
"""

import argparse
import concurrent.futures
import logging
import multiprocessing as mp
import os
import random
import sys

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.backtest.engine import run_backtest
from scripts.processed.fetch_data import fetch_trade_calendar, get_latest_trade_date
from scripts.strategy.signal_generator import resolve_symbol_pool
from scripts.utils.asset_loader import build_asset_metadata, fetch_asset_history

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")

_DATA_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")
_STRATEGY_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")
_MAX_BACKTEST_TRADE_DAYS = 120


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _calc_sell_win_stat(trade_log: list[dict]) -> tuple[int, int]:
    """统计一轮回测中的卖出笔数与盈利卖出笔数。"""
    sell_count = 0
    win_count = 0
    for t in trade_log:
        if t.get("action") != "sell":
            continue
        sell_count += 1
        if float(t.get("pnl", 0.0) or 0.0) > 0:
            win_count += 1
    return win_count, sell_count


def _calc_realized_pnl(trade_log: list[dict]) -> float:
    """统计一轮回测的已实现盈亏（仅卖出 pnl）。"""
    total = 0.0
    for t in trade_log:
        if t.get("action") == "sell":
            total += float(t.get("pnl", 0.0) or 0.0)
    return total


def _sample_windows(
    available_trade_dates: list[str],
    trials_per_symbol: int,
    trial_days: int,
    rng: random.Random,
) -> list[tuple[str, str]]:
    """生成独立实验窗口；窗口不足时退化为全区间重复实验。"""
    if not available_trade_dates:
        return []

    if len(available_trade_dates) <= trial_days:
        return [(available_trade_dates[0], available_trade_dates[-1]) for _ in range(trials_per_symbol)]

    max_start_idx = len(available_trade_dates) - trial_days
    windows: list[tuple[str, str]] = []
    for _ in range(trials_per_symbol):
        idx = rng.randint(0, max_start_idx)
        s = available_trade_dates[idx]
        e = available_trade_dates[idx + trial_days - 1]
        windows.append((s, e))
    return windows


def _resolve_symbol_trade_dates(
    symbol: str,
    start_date: str,
    end_date: str,
    trade_dates: list[str],
    asset_meta: dict[str, dict],
) -> list[str]:
    """解析单代码可用交易区间（从首个有效收盘价开始）。"""
    cleaned = fetch_asset_history(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        trade_dates=trade_dates,
        asset_meta=asset_meta,
    )
    if cleaned is None or cleaned.empty or "close" not in cleaned.columns:
        return []

    first_valid = cleaned["close"].first_valid_index()
    if first_valid is None:
        return []

    first_valid_str = first_valid.strftime("%Y-%m-%d")
    return [d for d in trade_dates if d >= first_valid_str]


def _run_symbol_trials(
    symbol: str,
    windows: list[tuple[str, str]],
    capital: float,
    risk_free_rate: float,
    target_trials: int,
) -> dict | None:
    """对单个代码执行多次独立实验并聚合结果。"""
    if not windows:
        return None

    total_return_sum = 0.0
    sharpe_sum = 0.0
    realized_pnl_sum = 0.0
    total_wins = 0
    total_sells = 0
    success_count = 0

    unique_windows = list(dict.fromkeys(windows))
    window_result_cache: dict[tuple[str, str], dict | None] = {}

    attempt_idx = 0
    max_attempts = max(target_trials * 5, len(unique_windows))
    while success_count < target_trials and attempt_idx < max_attempts:
        s, ed = unique_windows[attempt_idx % len(unique_windows)]
        attempt_idx += 1
        try:
            cache_key = (s, ed)
            if cache_key not in window_result_cache:
                window_result_cache[cache_key] = run_backtest(
                    symbols=[symbol],
                    capital=capital,
                    start_date=s,
                    end_date=ed,
                    risk_free_rate=risk_free_rate,
                )
            result = window_result_cache[cache_key]
            if not result:
                continue
            metrics = result.get("metrics", {})
            trade_log = result.get("trade_log", [])
            total_return_sum += float(metrics.get("total_return", 0.0))
            sharpe_sum += float(metrics.get("sharpe_ratio", 0.0))
            realized_pnl_sum += _calc_realized_pnl(trade_log)
            wins, sells = _calc_sell_win_stat(trade_log)
            total_wins += wins
            total_sells += sells
            success_count += 1
        except Exception as exc:
            logger.warning("独立实验失败：%s %s~%s | %s", symbol, s, ed, exc)

    if success_count == 0:
        return None

    avg_total_return = total_return_sum / success_count
    avg_sharpe = sharpe_sum / success_count
    avg_realized_pnl = realized_pnl_sum / success_count
    if total_sells == 0:
        win_rate = None
    else:
        win_rate = total_wins / total_sells

    # 排序评分仅使用已实现盈亏，避免高浮盈掩盖真实兑现能力
    score = avg_realized_pnl

    return {
        "symbol": symbol,
        "total_return": avg_total_return,
        "sharpe": avg_sharpe,
        "realized_pnl": avg_realized_pnl,
        "win_stat": (total_wins, total_sells, win_rate),
        "score": score,
        "trials": success_count,
        "target_trials": target_trials,
    }


def _resolve_worker_count(requested_workers: int, total_jobs: int) -> int:
    """解析回测报告实际并发进程数。"""
    if total_jobs <= 1:
        return 1
    if requested_workers > 0:
        return max(1, min(requested_workers, total_jobs))
    cpu_count = mp.cpu_count() or 1
    return max(1, min(cpu_count - 1 if cpu_count > 1 else 1, total_jobs))


def _run_symbol_trials_worker(task: tuple[str, list[tuple[str, str]], float, float, int]) -> dict | None:
    """多进程 worker：执行单代码多轮实验。"""
    symbol, windows, capital, risk_free_rate, trials = task
    return _run_symbol_trials(
        symbol=symbol,
        windows=windows,
        capital=capital,
        risk_free_rate=risk_free_rate,
        target_trials=trials,
    )


def _print_symbol_breakdown(rows: list[dict], trials_per_symbol: int, trial_days: int) -> None:
    """输出分代码表现表（按综合得分降序）。"""
    if not rows:
        print("分代码表现: 无可用数据")
        return

    rows.sort(key=lambda x: x["score"], reverse=True)

    _ = trials_per_symbol
    _ = trial_days
    print("代码      收益率      夏普    胜率(赢/平仓)      已实现盈亏(元)")
    for r in rows:
        win_count, sell_count, win_rate = r["win_stat"]
        if win_rate is None:
            win_text = "N/A"
        else:
            win_text = f"{win_rate:.2%}({win_count}/{sell_count})"
        print(
            f"{r['symbol']:<8} {r['total_return']:>8.2%}  {r['sharpe']:>7.4f}  "
            f"{win_text:>14}  {r['realized_pnl']:>14.2f}"
        )


def main() -> None:
    logging.disable(logging.CRITICAL)

    parser = argparse.ArgumentParser(description="分代码独立回测报告")
    parser.add_argument("--trials-per-symbol", type=int, default=50, help="每个代码独立实验次数（默认50）")
    parser.add_argument("--trial-days", type=int, default=120, help="单次独立实验窗口长度（交易日，固定不超过120）")
    parser.add_argument("--workers", type=int, default=0, help="并发进程数，0 表示自动")
    parser.add_argument("--seed", type=int, default=42, help="随机种子，保证实验可复现")
    parser.add_argument("--show-meta", action="store_true", help="显示回测区间、标的池和参数信息")
    parser.add_argument("--show-progress", action="store_true", help="显示代码级运行进度")
    args = parser.parse_args()

    if args.trials_per_symbol <= 0:
        raise ValueError("--trials-per-symbol 必须大于 0")
    if args.trial_days < 60:
        raise ValueError("--trial-days 建议不小于 60（因子计算需要足够窗口）")
    if args.trial_days > _MAX_BACKTEST_TRADE_DAYS:
        args.trial_days = _MAX_BACKTEST_TRADE_DAYS

    data_cfg = _load_yaml(_DATA_CONFIG_PATH)
    strategy_cfg = _load_yaml(_STRATEGY_CONFIG_PATH)

    backtest_cfg = data_cfg.get("backtest", {})
    evaluation_cfg = strategy_cfg.get("evaluation", {})
    capital_cfg = strategy_cfg.get("capital", {})

    symbols = resolve_symbol_pool()
    if not symbols:
        raise ValueError("标的池为空，请先在 strategy_config.yaml 中配置 holdings/watchlist/current_positions")

    start_date = backtest_cfg.get("start_date", "2015-01-01")
    end_date = get_latest_trade_date()
    capital = float(capital_cfg.get("total", 100000))
    risk_free_rate = float(evaluation_cfg.get("risk_free_rate", 0.02))

    trade_dates = fetch_trade_calendar(start_date, end_date)
    if not trade_dates:
        raise ValueError(f"区间 {start_date} ~ {end_date} 无可用交易日")
    if len(trade_dates) > _MAX_BACKTEST_TRADE_DAYS:
        trade_dates = trade_dates[-_MAX_BACKTEST_TRADE_DAYS:]

    # 历史回测固定在最近 120 个交易日
    start_date = trade_dates[0]
    end_date = trade_dates[-1]

    asset_meta = build_asset_metadata()
    rng = random.Random(args.seed)

    if args.show_meta:
        print("=" * 60)
        print("分代码独立回测报告")
        print("=" * 60)
        print(f"回测区间: {start_date} ~ {end_date}（结束日自动取最近交易日）")
        print(f"标的池: {', '.join(symbols)}")
        print(f"每代码独立实验次数: {args.trials_per_symbol}")
        print(f"单次实验窗口: {args.trial_days} 交易日")
        print("-" * 60)

    symbol_tasks: list[tuple[str, list[tuple[str, str]], float, float, int]] = []
    for idx, sym in enumerate(symbols, 1):
        if args.show_progress:
            print(f"进度: [{idx}/{len(symbols)}] {sym}")
        symbol_trade_dates = _resolve_symbol_trade_dates(
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            asset_meta=asset_meta,
        )
        if not symbol_trade_dates:
            continue
        windows = _sample_windows(
            available_trade_dates=symbol_trade_dates,
            trials_per_symbol=args.trials_per_symbol,
            trial_days=args.trial_days,
            rng=rng,
        )
        if windows:
            symbol_tasks.append((sym, windows, capital, risk_free_rate, args.trials_per_symbol))

    rows: list[dict] = []
    workers = _resolve_worker_count(args.workers, len(symbol_tasks))
    if args.show_meta:
        print(f"并发进程数: {workers}")

    if workers == 1:
        for task in symbol_tasks:
            row = _run_symbol_trials_worker(task)
            if row is not None:
                rows.append(row)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_run_symbol_trials_worker, task) for task in symbol_tasks]
            done = 0
            total = len(futures)
            for fut in concurrent.futures.as_completed(futures):
                done += 1
                if args.show_progress:
                    print(f"并行进度: [{done}/{total}]")
                try:
                    row = fut.result()
                except Exception as exc:
                    logger.warning("并行回测任务失败: %s", exc)
                    continue
                if row is not None:
                    rows.append(row)

    if args.show_meta:
        print("-" * 60)
    _print_symbol_breakdown(rows, trials_per_symbol=args.trials_per_symbol, trial_days=args.trial_days)
    if args.show_meta:
        print("=" * 60)


if __name__ == "__main__":
    main()
