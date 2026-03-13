"""
参数自动调优：基于历史回测网格搜索 Livermore 参数。

目标：在给定标的池和回测区间内，对 m/c/h/k/z/y 进行网格搜索，
输出评分最高的一组参数，帮助从历史数据中校准策略。
"""

import argparse
import itertools
import logging
import multiprocessing as mp
import os
import sys
import time

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.backtest.engine import run_backtest
from scripts.processed.fetch_data import get_latest_trade_date
from scripts.strategy.signal_generator import resolve_symbol_pool

_DATA_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "data_config.yaml")
_STRATEGY_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _score(metrics: dict) -> float:
    """评分函数：仅优化收益率、夏普比率、胜率三项（越大越好）。"""
    total_return = float(metrics.get("total_return", 0.0))
    sharpe = float(metrics.get("sharpe_ratio", 0.0))
    win_rate = float(metrics.get("win_rate", 0.0))
    return total_return + sharpe + win_rate


def _parse_grid(raw: str) -> list[float]:
    """解析逗号分隔参数列表。"""
    values = [x.strip() for x in raw.split(",") if x.strip()]
    if not values:
        raise ValueError("参数列表不能为空")
    return [float(v) for v in values]


def _format_eta(seconds: float) -> str:
    """格式化剩余时间显示。"""
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes > 0:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _build_param_cases(
    m_grid: list[float],
    c_grid: list[float],
    h_grid: list[float],
    k_grid: list[float],
    z_grid: list[float],
    y_grid: list[float],
    max_cases: int,
) -> list[dict]:
    """构造待评估参数组合列表。"""
    cases: list[dict] = []
    for idx, (m, c, h, k, z_threshold, y_threshold) in enumerate(
        itertools.product(m_grid, c_grid, h_grid, k_grid, z_grid, y_grid),
        start=1,
    ):
        if max_cases > 0 and idx > max_cases:
            break
        cases.append(
            {
                "case_idx": idx,
                "params": {
                    "m": m,
                    "c": c,
                    "h": h,
                    "k": k,
                    "z_threshold": z_threshold,
                    "y_threshold": y_threshold,
                },
            }
        )
    return cases


def _evaluate_case(task: dict) -> dict:
    """评估单组参数，供串行与并行两种模式复用。"""
    logging.disable(logging.CRITICAL)
    params = task["params"]
    try:
        result = run_backtest(
            symbols=task["symbols"],
            capital=task["capital"],
            start_date=task["start_date"],
            end_date=task["end_date"],
            risk_free_rate=task["risk_free_rate"],
            strategy_params=params,
        )
        metrics = result.get("metrics", {})
        return {
            "case_idx": task["case_idx"],
            "params": params,
            "metrics": metrics,
            "score": _score(metrics),
            "error": None,
        }
    except Exception as exc:
        return {
            "case_idx": task["case_idx"],
            "params": params,
            "metrics": {},
            "score": None,
            "error": str(exc),
        }


def _resolve_worker_count(requested_workers: int, total_cases: int) -> int:
    """解析实际并发进程数。"""
    if total_cases <= 1:
        return 1
    if requested_workers > 0:
        return max(1, min(requested_workers, total_cases))
    cpu_count = mp.cpu_count() or 1
    return max(1, min(cpu_count - 1 if cpu_count > 1 else 1, total_cases))


def _apply_best_to_config(best_params: dict) -> None:
    """将最优参数写回 strategy_config.yaml。"""
    cfg = _load_yaml(_STRATEGY_CONFIG_PATH)
    cfg.setdefault("livermore", {})
    cfg.setdefault("signal", {})

    cfg["livermore"]["m"] = float(best_params["m"])
    cfg["livermore"]["c"] = float(best_params["c"])
    cfg["livermore"]["h"] = float(best_params["h"])
    cfg["livermore"]["k"] = float(best_params["k"])
    cfg["livermore"]["y_threshold"] = float(best_params["y_threshold"])
    cfg["signal"]["confidence_threshold"] = float(best_params["z_threshold"])

    with open(_STRATEGY_CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)


def main() -> None:
    logging.disable(logging.CRITICAL)
    mp.freeze_support()

    parser = argparse.ArgumentParser(description="Livermore 参数自动调优")
    parser.add_argument("--m-grid", default="0.05,0.08,0.1")
    parser.add_argument("--c-grid", default="0.05,0.07,0.09")
    parser.add_argument("--h-grid", default="0.08,0.10,0.12")
    parser.add_argument("--k-grid", default="0.3,0.5,0.7")
    parser.add_argument("--z-grid", default="1.0,1.3,1.5")
    parser.add_argument("--y-grid", default="0.50,0.55,0.60")
    parser.add_argument("--max-cases", type=int, default=0, help="最多运行多少组参数，0 表示不限制")
    parser.add_argument("--workers", type=int, default=0, help="并发进程数，0 表示自动选择")
    parser.add_argument("--apply", action="store_true", help="将最优参数写回 strategy_config.yaml")
    args = parser.parse_args()

    data_cfg = _load_yaml(_DATA_CONFIG_PATH)
    strategy_cfg = _load_yaml(_STRATEGY_CONFIG_PATH)

    symbols = resolve_symbol_pool()
    if not symbols:
        raise ValueError("标的池为空，请先配置 holdings/watchlist/current_positions")

    backtest_cfg = data_cfg.get("backtest", {})
    evaluation_cfg = strategy_cfg.get("evaluation", {})
    capital_cfg = strategy_cfg.get("capital", {})

    start_date = backtest_cfg.get("start_date", "2015-01-01")
    end_date = get_latest_trade_date()
    capital = float(capital_cfg.get("total", 100000))
    risk_free_rate = float(evaluation_cfg.get("risk_free_rate", 0.02))

    m_grid = _parse_grid(args.m_grid)
    c_grid = _parse_grid(args.c_grid)
    h_grid = _parse_grid(args.h_grid)
    k_grid = _parse_grid(args.k_grid)
    z_grid = _parse_grid(args.z_grid)
    y_grid = _parse_grid(args.y_grid)

    param_cases = _build_param_cases(
        m_grid=m_grid,
        c_grid=c_grid,
        h_grid=h_grid,
        k_grid=k_grid,
        z_grid=z_grid,
        y_grid=y_grid,
        max_cases=args.max_cases,
    )
    total_cases = len(param_cases)
    workers = _resolve_worker_count(args.workers, total_cases)
    print(f"开始调优，共 {total_cases} 组参数，并发进程数 {workers}...")

    best = None
    started_at = time.time()
    tasks = [
        {
            **case,
            "symbols": symbols,
            "capital": capital,
            "start_date": start_date,
            "end_date": end_date,
            "risk_free_rate": risk_free_rate,
        }
        for case in param_cases
    ]

    if workers == 1:
        results_iter = map(_evaluate_case, tasks)
    else:
        ctx = mp.get_context("spawn")
        pool = ctx.Pool(processes=workers)
        results_iter = pool.imap_unordered(_evaluate_case, tasks)

    try:
        completed = 0
        for outcome in results_iter:
            completed += 1
            elapsed = time.time() - started_at
            avg_per_case = elapsed / completed
            remaining = avg_per_case * (total_cases - completed)
            eta_str = _format_eta(remaining)

            if outcome["error"]:
                print(
                    f"[{completed}/{total_cases}] ETA {eta_str}  失败: "
                    f"params={outcome['params']}, error={outcome['error']}"
                )
                continue

            if best is None or outcome["score"] > best["score"]:
                best = {
                    "params": outcome["params"],
                    "score": outcome["score"],
                    "metrics": outcome["metrics"],
                    "case_idx": outcome["case_idx"],
                }

            metrics = outcome["metrics"]
            print(
                f"[{completed}/{total_cases}] ETA {eta_str}  score={outcome['score']:.4f} "
                f"ret={metrics.get('total_return', 0.0):.2%} "
                f"sharpe={metrics.get('sharpe_ratio', 0.0):.3f} "
                f"win={metrics.get('win_rate', 0.0):.2%} "
                f"params={outcome['params']}"
            )
    finally:
        if workers > 1:
            pool.close()
            pool.join()

    if not best:
        raise RuntimeError("调优失败：没有可用结果")

    elapsed = time.time() - started_at
    print(f"耗时: {elapsed:.1f} 秒")

    print("=" * 60)
    print("最优参数")
    print(best["params"])
    print(f"评分: {best['score']:.4f}")
    print("对应指标:")
    for k, v in best["metrics"].items():
        if isinstance(v, float):
            if "rate" in k or "return" in k or "drawdown" in k or k == "annual_vol" or k == "win_rate":
                print(f"  {k}: {v:.2%}")
            else:
                print(f"  {k}: {v:.4f}")
        else:
            print(f"  {k}: {v}")

    if args.apply:
        _apply_best_to_config(best["params"])
        print("最优参数已写回 configs/strategy_config.yaml")


if __name__ == "__main__":
    main()
