"""
独立荐股脚本：仅输出 1 个推荐代码，不输出持仓买卖建议。

用途：
    按利弗莫尔策略在候选池中的历史表现（收益率/夏普/胜率）
    选出 1 个最优代码，供用户人工进一步判断。
"""

import argparse
import logging
import multiprocessing as mp
import os
import re
import sys
from datetime import datetime

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.strategy.signal_generator import resolve_symbol_pool
from scripts.utils.market_scanner import load_candidate_pool_file, recommend_best_candidate

logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(ROOT_DIR, "configs", "strategy_config.yaml")
_RECOMMEND_DIR = os.path.join(ROOT_DIR, "datas", "recommend")
_RECOMMEND_FILE = os.path.join(_RECOMMEND_DIR, "荐股.txt")


def _load_strategy_config() -> dict:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _recommend_worker(
    conn,
    exclude_symbols: set[str],
    etf_top_n: int,
    stock_top_n: int,
    fund_top_n: int,
    eval_days: int,
    strategy_params: dict,
    risk_free_rate: float,
    disable_proxy: bool,
) -> None:
    """子进程执行荐股逻辑，通过 Pipe 返回结果。"""
    logging.disable(logging.CRITICAL)
    try:
        result = recommend_best_candidate(
            exclude_symbols=exclude_symbols,
            etf_top_n=etf_top_n,
            stock_top_n=stock_top_n,
            fund_top_n=fund_top_n,
            eval_days=eval_days,
            strategy_params=strategy_params,
            risk_free_rate=risk_free_rate,
            disable_proxy=disable_proxy,
        )
        conn.send(result)
    except Exception:
        conn.send(None)
    finally:
        conn.close()


def _append_recommend_record(symbol: str) -> None:
    """将有效荐股结果按天聚合写入 datas/recommend/荐股.txt。"""
    os.makedirs(_RECOMMEND_DIR, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")

    by_day: dict[str, list[str]] = {}
    if os.path.exists(_RECOMMEND_FILE):
        with open(_RECOMMEND_FILE, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                date_key = ""
                codes: list[str] = []

                # 新格式：2026-03-13：022364、022365
                if "：" in line:
                    left, right = line.split("：", 1)
                    left = left.strip()
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", left):
                        date_key = left
                        codes = [x.strip() for x in re.split(r"[、,，]", right) if x.strip()]
                # 兼容旧格式：2026-03-13 12:41:15\t022364
                elif "\t" in line:
                    ts, code = line.split("\t", 1)
                    date_key = ts.strip()[:10]
                    code = code.strip()
                    if code:
                        codes = [code]

                if not date_key:
                    continue
                bucket = by_day.setdefault(date_key, [])
                for code in codes:
                    if code not in bucket:
                        bucket.append(code)

    day_codes = by_day.setdefault(today, [])
    if symbol not in day_codes:
        day_codes.append(symbol)

    with open(_RECOMMEND_FILE, "w", encoding="utf-8") as f:
        for date_key in sorted(by_day.keys()):
            codes_text = "、".join(by_day[date_key])
            f.write(f"{date_key}：{codes_text}\n")


def _expand_exclude_symbols(exclude_symbols: set[str]) -> set[str]:
    """扩展排除列表：若排除场外基金某代码，同时排除同前5位代码（A/C类）。"""
    pool = load_candidate_pool_file()
    if not pool or not exclude_symbols:
        return exclude_symbols

    fund_groups: dict[str, set[str]] = {}
    for item in pool:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol.isdigit() or len(symbol) != 6:
            continue
        if str(item.get("asset_type", "")) != "fund_open":
            continue
        fund_groups.setdefault(symbol[:5], set()).add(symbol)

    expanded = set(exclude_symbols)
    for symbol in list(exclude_symbols):
        if symbol.isdigit() and len(symbol) == 6 and symbol[:5] in fund_groups:
            expanded.update(fund_groups[symbol[:5]])
    return expanded


def main() -> None:
    # 独立荐股模式下仅输出最终代码，不输出中间日志
    logging.disable(logging.CRITICAL)

    parser = argparse.ArgumentParser(description="独立荐股：仅输出 1 个代码")
    parser.add_argument("--disable-proxy", action="store_true", default=True, help="执行荐股时临时禁用代理")
    parser.add_argument("--etf-top-n", type=int, default=None, help="ETF 候选数量上限")
    parser.add_argument("--stock-top-n", type=int, default=None, help="个股候选数量上限")
    parser.add_argument("--fund-top-n", type=int, default=None, help="场外基金候选数量上限")
    parser.add_argument("--exclude-symbols", default="", help="额外排除代码，逗号分隔，如 022364,159001")
    parser.add_argument("--eval-days", type=int, default=None, help="候选回测窗口（天）")
    parser.add_argument("--timeout", type=int, default=90, help="荐股总超时时间（秒），超时输出 NONE")
    args = parser.parse_args()

    cfg = _load_strategy_config()
    lv_cfg = cfg.get("livermore", {})
    signal_cfg = cfg.get("signal", {})

    lv_asset_params = (lv_cfg.get("asset_params") or {})
    exchange_lv = (lv_asset_params.get("exchange") or {})
    fund_lv = (lv_asset_params.get("fund_open") or {})

    strategy_params = {
        "m": float(exchange_lv.get("m", lv_cfg.get("m", 0.1))),
        "c": float(exchange_lv.get("c", lv_cfg.get("c", 0.07))),
        "h": float(exchange_lv.get("h", lv_cfg.get("h", 0.10))),
        "k": float(exchange_lv.get("k", lv_cfg.get("k", 0.5))),
        "asset_params": {
            "exchange": {
                "m": float(exchange_lv.get("m", lv_cfg.get("m", 0.1))),
                "c": float(exchange_lv.get("c", lv_cfg.get("c", 0.07))),
                "h": float(exchange_lv.get("h", lv_cfg.get("h", 0.10))),
                "k": float(exchange_lv.get("k", lv_cfg.get("k", 0.5))),
            },
            "fund_open": {
                "m": float(fund_lv.get("m", exchange_lv.get("m", lv_cfg.get("m", 0.1)))),
                "c": float(fund_lv.get("c", exchange_lv.get("c", lv_cfg.get("c", 0.07)))),
                "h": float(fund_lv.get("h", exchange_lv.get("h", lv_cfg.get("h", 0.10)))),
                "k": float(fund_lv.get("k", exchange_lv.get("k", lv_cfg.get("k", 0.5)))),
            },
        },
    }

    exclude_symbols = set(resolve_symbol_pool())
    config_excludes = signal_cfg.get("recommend_exclude_symbols", [])
    if isinstance(config_excludes, list):
        exclude_symbols.update(str(x).strip() for x in config_excludes if str(x).strip())
    arg_excludes = [x.strip() for x in str(args.exclude_symbols or "").split(",") if x.strip()]
    exclude_symbols.update(arg_excludes)
    etf_top_n = args.etf_top_n if args.etf_top_n is not None else int(signal_cfg.get("scan_etf_top_n", 8))
    stock_top_n = args.stock_top_n if args.stock_top_n is not None else int(signal_cfg.get("scan_stock_top_n", 8))
    fund_top_n = args.fund_top_n if args.fund_top_n is not None else int(signal_cfg.get("scan_fund_top_n", etf_top_n))
    eval_days = args.eval_days if args.eval_days is not None else int(signal_cfg.get("scan_eval_days", 365))
    risk_free_rate = float(cfg.get("evaluation", {}).get("risk_free_rate", 0.02))
    exclude_symbols = _expand_exclude_symbols(exclude_symbols)

    best: dict | None = None
    timeout_seconds = max(1, int(args.timeout))
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    process = ctx.Process(
        target=_recommend_worker,
        args=(
            child_conn,
            exclude_symbols,
            etf_top_n,
            stock_top_n,
            fund_top_n,
            eval_days,
            strategy_params,
            risk_free_rate,
            bool(args.disable_proxy),
        ),
        daemon=True,
    )
    process.start()
    child_conn.close()

    if parent_conn.poll(timeout_seconds):
        try:
            best = parent_conn.recv()
        except Exception:
            best = None
    else:
        best = None

    if process.is_alive():
        process.terminate()
    process.join(timeout=1)
    parent_conn.close()

    # 仅在有有效推荐代码时写入文件；无结果仅输出 NONE，不落盘
    if best and best.get("symbol"):
        symbol = str(best["symbol"])
        _append_recommend_record(symbol)
        print(symbol)
    else:
        print("NONE")


if __name__ == "__main__":
    main()
