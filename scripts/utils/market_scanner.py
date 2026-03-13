"""
市场候选扫描与优选推荐。

功能：
    1) 扫描候选：从活跃 ETF 与沪深 300 成分股中筛选候选
    2) 优选推荐：对候选逐一做利弗莫尔单标的回测，输出 1 个最优代码
"""

import logging
import os
import sys
import json
from datetime import date, timedelta
from contextlib import contextmanager

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import akshare as ak
from scripts.backtest.engine import run_backtest

logger = logging.getLogger(__name__)

# ETF 最低成交额过滤阈值（元），过滤掉流动性极差的标的
_MIN_ETF_AMOUNT = 5_000_000  # 500 万元
_DEFAULT_EVAL_CAPITAL = 100000
_CANDIDATE_POOL_DIR = os.path.join(ROOT_DIR, "datas", "recommend")
_CANDIDATE_POOL_PATH = os.path.join(_CANDIDATE_POOL_DIR, "candidate_pool.json")
_CLUSTER_FEATURES = [
    "total_return",
    "sharpe_ratio",
    "max_drawdown",
    "win_rate",
]
_DEFAULT_CLUSTER_KEEP_PER_CLUSTER = 1
_DEFAULT_MAX_CLUSTER_COUNT = 6
_DEFAULT_PARETO_SCORE_EPSILON = 0.05
_DEFAULT_FUND_TOP_N = 16


def _get_active_proxy_env() -> dict[str, str]:
    """获取当前进程中的代理环境变量。"""
    keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
    ]
    return {k: os.environ.get(k, "") for k in keys if os.environ.get(k, "")}


@contextmanager
def _temporary_disable_proxy(disable_proxy: bool):
    """临时禁用代理环境变量，退出上下文后恢复。"""
    if not disable_proxy:
        yield
        return

    keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
    ]
    backup = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            if k in os.environ:
                del os.environ[k]
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        yield
    finally:
        for k in keys:
            if backup.get(k) is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = backup[k]


def _classify_fund_asset_type(symbol: str, fund_name: str, fund_type: str) -> str:
    """根据基金代码与名称推断资产类型。"""
    upper_name = str(fund_name or "").upper()
    upper_type = str(fund_type or "").upper()
    merged = f"{upper_name} {upper_type}"

    # 场外联接基金优先判定，避免误走 ETF 场内行情接口
    if "联接" in merged:
        return "fund_open"

    if "LOF" in merged:
        return "lof"

    # 仅当明确属于场内语义时判定为 ETF
    if "ETF" in merged and ("场内" in merged or symbol.startswith(("15", "16", "50", "51", "52", "56", "58"))):
        return "etf"

    # 其余基金默认按场外基金处理
    return "fund_open"


def _safe_float(value, default: float = 0.0) -> float:
    """安全转换浮点数。"""
    try:
        converted = pd.to_numeric(value, errors="coerce")
        if pd.isna(converted):
            return default
        return float(converted)
    except Exception:
        return default


def load_candidate_pool_file() -> list[dict]:
    """从本地 JSON 文件读取离线候选池。"""
    if not os.path.exists(_CANDIDATE_POOL_PATH):
        return []
    with open(_CANDIDATE_POOL_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return []


def save_candidate_pool_file(candidates: list[dict]) -> str:
    """将离线候选池写入本地 JSON 文件。"""
    os.makedirs(_CANDIDATE_POOL_DIR, exist_ok=True)
    with open(_CANDIDATE_POOL_PATH, "w", encoding="utf-8") as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)
    return _CANDIDATE_POOL_PATH


def build_offline_candidate_pool(disable_proxy: bool = True) -> list[dict]:
    """构建离线候选池，覆盖股票、ETF/LOF 与场外基金。"""
    candidates: list[dict] = []

    with _temporary_disable_proxy(disable_proxy=disable_proxy):
        stock_spot_map: dict[str, dict] = {}
        try:
            stock_spot_df = ak.stock_zh_a_spot_em()
            if not stock_spot_df.empty and "代码" in stock_spot_df.columns:
                if "成交额" in stock_spot_df.columns:
                    stock_spot_df["成交额"] = pd.to_numeric(stock_spot_df["成交额"], errors="coerce").fillna(0.0)
                    stock_spot_df = stock_spot_df.sort_values("成交额", ascending=False).reset_index(drop=True)
                for idx, row in stock_spot_df.iterrows():
                    symbol = str(row.get("代码", "")).strip()
                    if not symbol:
                        continue
                    stock_spot_map[symbol] = {
                        "name": str(row.get("名称", symbol)).strip() or symbol,
                        "latest_price": _safe_float(row.get("最新价")),
                        "turnover": _safe_float(row.get("成交额")),
                        "pool_rank": idx + 1,
                    }
        except Exception as exc:
            logger.warning("获取 A 股实时行情失败，股票候选将回退到代码表顺序：%s", exc)

        stock_df = ak.stock_info_a_code_name()
        for idx, row in stock_df.iterrows():
            symbol = str(row.get("code", "")).strip()
            if not symbol:
                continue
            spot_meta = stock_spot_map.get(symbol, {})
            candidates.append(
                {
                    "symbol": symbol,
                    "name": str(spot_meta.get("name") or row.get("name", symbol)),
                    "asset_type": "stock",
                    "latest_price": float(spot_meta.get("latest_price", 0.0) or 0.0),
                    "turnover": float(spot_meta.get("turnover", 0.0) or 0.0),
                    "pool_rank": int(spot_meta.get("pool_rank", idx + 1) or idx + 1),
                    "source": "stock_zh_a_spot_em" if spot_meta else "stock_info_a_code_name",
                }
            )

        fund_df = ak.fund_name_em()
        fund_rank_df = ak.fund_open_fund_rank_em(symbol="全部")
        fund_rank_map: dict[str, dict] = {}
        for _, row in fund_rank_df.iterrows():
            symbol = str(row.get("基金代码", "")).strip()
            if not symbol:
                continue
            fund_rank_map[symbol] = {
                "pool_rank": int(_safe_float(row.get("序号"), 999999)),
                "latest_price": _safe_float(row.get("单位净值")),
                "rank_score": _safe_float(row.get("自定义")),
            }

        for idx, row in fund_df.iterrows():
            symbol = str(row.get("基金代码", "")).strip()
            if not symbol:
                continue
            name = str(row.get("基金简称", symbol))
            fund_type = str(row.get("基金类型", ""))
            asset_type = _classify_fund_asset_type(symbol, name, fund_type)
            rank_meta = fund_rank_map.get(symbol, {})
            candidates.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "asset_type": asset_type,
                    "latest_price": float(rank_meta.get("latest_price", 0.0) or 0.0),
                    "turnover": 0.0,
                    "pool_rank": int(rank_meta.get("pool_rank", idx + 1)),
                    "rank_score": float(rank_meta.get("rank_score", 0.0) or 0.0),
                    "source": "fund_name_em",
                }
            )

    dedup: dict[str, dict] = {}
    for item in candidates:
        dedup[item["symbol"]] = item
    result = list(dedup.values())
    logger.info("离线候选池构建完成：共 %d 只候选", len(result))
    return result


def refresh_candidate_pool(disable_proxy: bool = True) -> str:
    """构建并保存离线候选池。"""
    candidates = build_offline_candidate_pool(disable_proxy=disable_proxy)
    path = save_candidate_pool_file(candidates)
    logger.info("离线候选池已写入：%s", path)
    return path


def load_market_candidates_from_pool(
    etf_top_n: int = 30,
    stock_top_n: int = 20,
    fund_top_n: int = _DEFAULT_FUND_TOP_N,
    exclude_symbols: set[str] | None = None,
) -> dict[str, dict]:
    """从本地候选池中加载候选并按类型配额筛选。"""
    exclude = exclude_symbols or set()
    pool = load_candidate_pool_file()
    if not pool:
        logger.warning("离线候选池不存在或为空：%s", _CANDIDATE_POOL_PATH)
        return {}

    bucketed: dict[str, list[dict]] = {"stock": [], "etf": [], "lof": [], "fund_open": []}
    for item in pool:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in exclude:
            continue
        asset_type = str(item.get("asset_type", "stock"))
        bucketed.setdefault(asset_type, []).append(item)

    # 个股候选实时补齐行情，避免离线池中 turnover=0 导致按代码顺序退化。
    if stock_top_n > 0 and bucketed.get("stock"):
        stock_spot_map: dict[str, dict] = {}
        try:
            stock_spot_df = ak.stock_zh_a_spot_em()
            if not stock_spot_df.empty and "代码" in stock_spot_df.columns:
                if "成交额" in stock_spot_df.columns:
                    stock_spot_df["成交额"] = pd.to_numeric(stock_spot_df["成交额"], errors="coerce").fillna(0.0)
                    stock_spot_df = stock_spot_df.sort_values("成交额", ascending=False).reset_index(drop=True)

                for idx, row in stock_spot_df.iterrows():
                    symbol = str(row.get("代码", "")).strip()
                    if not symbol:
                        continue
                    stock_spot_map[symbol] = {
                        "name": str(row.get("名称", symbol)).strip() or symbol,
                        "latest_price": _safe_float(row.get("最新价")),
                        "turnover": _safe_float(row.get("成交额")),
                        "pool_rank": idx + 1,
                    }

            if stock_spot_map:
                for item in bucketed["stock"]:
                    meta = stock_spot_map.get(str(item.get("symbol", "")))
                    if not meta:
                        continue
                    item["name"] = meta["name"]
                    item["latest_price"] = float(meta["latest_price"])
                    item["turnover"] = float(meta["turnover"])
                    item["pool_rank"] = int(meta["pool_rank"])
                    item["source"] = "stock_zh_a_spot_em"
            else:
                logger.warning("未获取到 A 股实时行情，个股候选将沿用离线池排序")
        except Exception as exc:
            logger.warning("补齐个股实时行情失败，个股候选将沿用离线池排序：%s", exc)

        # 兜底：若个股成交额全部缺失/为0，按市场前缀轮转混排，避免 000xxx 顺序偏置。
        stock_items = bucketed.get("stock", [])
        if stock_items:
            max_turnover = max(_safe_float(item.get("turnover"), 0.0) for item in stock_items)
            if max_turnover <= 0:
                by_prefix: dict[str, list[dict]] = {}
                for item in stock_items:
                    symbol = str(item.get("symbol", "")).strip()
                    prefix = symbol[:1] if symbol else ""
                    by_prefix.setdefault(prefix, []).append(item)

                for prefix_items in by_prefix.values():
                    prefix_items.sort(
                        key=lambda x: (
                            int(x.get("pool_rank", 999999) or 999999),
                            str(x.get("symbol", "")),
                        )
                    )

                # 优先覆盖主板常见前缀，其他前缀随后补齐
                prefix_order = ["6", "0", "3"]
                prefix_order.extend(sorted(p for p in by_prefix.keys() if p not in prefix_order))

                remixed: list[dict] = []
                progressed = True
                while progressed:
                    progressed = False
                    for p in prefix_order:
                        queue = by_prefix.get(p) or []
                        if not queue:
                            continue
                        remixed.append(queue.pop(0))
                        progressed = True

                if remixed:
                    # 赋予新的排序位次，后续统一排序时可直接使用。
                    for idx, item in enumerate(remixed, start=1):
                        item["pool_rank"] = idx
                    bucketed["stock"] = remixed
                    logger.warning("个股候选缺少成交额，已启用按前缀轮转混排兜底策略")

    def _sort_key(item: dict) -> tuple:
        turnover = _safe_float(item.get("turnover"), 0.0)
        rank_score = _safe_float(item.get("rank_score"), 0.0)
        pool_rank = int(item.get("pool_rank", 999999) or 999999)
        return (-turnover, -rank_score, pool_rank, str(item.get("symbol", "")))

    result: dict[str, dict] = {}
    for asset_type in bucketed:
        bucketed[asset_type].sort(key=_sort_key)

    for item in bucketed.get("etf", [])[:etf_top_n]:
        result[item["symbol"]] = item
    for item in bucketed.get("lof", [])[:etf_top_n]:
        result[item["symbol"]] = item
    for item in bucketed.get("stock", [])[:stock_top_n]:
        result[item["symbol"]] = item
    for item in bucketed.get("fund_open", [])[:fund_top_n]:
        result[item["symbol"]] = item

    logger.info(
        "离线候选池加载完成：ETF/LOF %d 只 + 个股 %d 只 + 场外基金 %d 只 = 合计 %d 只",
        min(len(bucketed.get("etf", [])) + len(bucketed.get("lof", [])), etf_top_n * 2),
        min(len(bucketed.get("stock", [])), stock_top_n),
        min(len(bucketed.get("fund_open", [])), fund_top_n),
        len(result),
    )
    return result


def scan_etf_candidates(
    top_n: int = 30,
    exclude_symbols: set[str] | None = None,
) -> dict[str, dict]:
    """
    从场内 ETF 列表中，按当日成交额降序筛选活跃 ETF，返回代码列表。

    参数：
        top_n: 最多返回的候选标的数量
        exclude_symbols: 已持有或已在监控的标的代码集合，跳过这些标的

    返回：
        ETF 代码列表（6 位字符串）
    """
    exclude = exclude_symbols or set()
    try:
        df = ak.fund_etf_spot_em()
        if df.empty or "代码" not in df.columns:
            logger.warning("fund_etf_spot_em 返回数据为空或字段缺失，跳过 ETF 扫描")
            return []

        # 按成交额降序排列，过滤低流动性标的
        if "成交额" in df.columns:
            df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce").fillna(0)
            df = df[df["成交额"] >= _MIN_ETF_AMOUNT]
            df = df.sort_values("成交额", ascending=False)

        result: dict[str, dict] = {}
        name_col = "名称" if "名称" in df.columns else None
        price_col = "最新价" if "最新价" in df.columns else None
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code or code in exclude:
                continue
            result[code] = {
                "name": str(row.get(name_col, code)) if name_col else code,
                "asset_type": "etf",
                "latest_price": float(pd.to_numeric(row.get(price_col, 0.0), errors="coerce") or 0.0),
            }
            if len(result) >= top_n:
                break
        logger.info("ETF 候选扫描：筛出 %d 只（原始 %d 只，排除 %d 只）", len(result), len(df), len(exclude))
        return result

    except Exception as e:
        logger.warning("ETF 候选扫描失败：%s", e)
        return []


def scan_stock_candidates(
    top_n: int = 20,
    exclude_symbols: set[str] | None = None,
) -> dict[str, dict]:
    """
    从沪深 300 成分股中，按当日成交额筛选活跃个股，返回代码列表。

    使用沪深 300 成分股作为候选域，在兼顾流动性的同时限制扫描规模。

    参数：
        top_n: 最多返回的候选标的数量
        exclude_symbols: 已持有或已在监控的标的代码集合

    返回：
        股票代码列表（6 位字符串）
    """
    exclude = exclude_symbols or set()
    try:
        # 获取沪深 300 成分股代码
        df_index = ak.index_stock_cons(symbol="000300")
        if df_index.empty:
            logger.warning("沪深 300 成分股列表为空，跳过股票扫描")
            return []

        hs300_codes = set(df_index.iloc[:, 0].astype(str).tolist())

        # 获取 A 股实时行情，过滤出沪深 300 成分股
        df_spot = ak.stock_zh_a_spot_em()
        if df_spot.empty or "代码" not in df_spot.columns:
            logger.warning("stock_zh_a_spot_em 返回数据为空或字段缺失，跳过股票扫描")
            return []

        df_spot = df_spot[df_spot["代码"].isin(hs300_codes)].copy()

        if "成交额" in df_spot.columns:
            df_spot["成交额"] = pd.to_numeric(df_spot["成交额"], errors="coerce").fillna(0)
            df_spot = df_spot.sort_values("成交额", ascending=False)

        result: dict[str, dict] = {}
        name_col = "名称" if "名称" in df_spot.columns else None
        price_col = "最新价" if "最新价" in df_spot.columns else None
        for _, row in df_spot.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code or code in exclude:
                continue
            result[code] = {
                "name": str(row.get(name_col, code)) if name_col else code,
                "asset_type": "stock",
                "latest_price": float(pd.to_numeric(row.get(price_col, 0.0), errors="coerce") or 0.0),
            }
            if len(result) >= top_n:
                break
        logger.info("个股候选扫描：筛出 %d 只（沪深 300 共 %d 只）", len(result), len(hs300_codes))
        return result

    except Exception as e:
        logger.warning("个股候选扫描失败：%s", e)
        return []


def get_market_candidates(
    etf_top_n: int = 30,
    stock_top_n: int = 20,
    fund_top_n: int = _DEFAULT_FUND_TOP_N,
    exclude_symbols: set[str] | None = None,
) -> dict[str, dict]:
    """
    综合扫描市场候选标的（ETF + 沪深 300 个股）。

    返回的字典格式与 build_asset_metadata() 保持一致，
    可直接作为 extra_meta 传入 build_asset_metadata() 以覆盖资产类型。

    参数：
        etf_top_n: ETF 候选数量上限
        stock_top_n: 个股候选数量上限
        exclude_symbols: 需要排除的标的集合

    返回：
        {symbol: {"name": str, "asset_type": str}} 字典
    """
    return load_market_candidates_from_pool(
        etf_top_n=etf_top_n,
        stock_top_n=stock_top_n,
        fund_top_n=fund_top_n,
        exclude_symbols=exclude_symbols,
    )


def _calc_realized_pnl(trade_log: list[dict]) -> float:
    """统计已实现盈亏（仅卖出成交 pnl）。"""
    total = 0.0
    for trade in trade_log:
        if trade.get("action") == "sell":
            total += float(trade.get("pnl", 0.0) or 0.0)
    return total


def _score_backtest(metrics: dict) -> float:
    """
    将回测结果压缩为单一评分：仅优化已实现盈亏。

    分数越高越优：
        score = realized_pnl
    """
    return float(metrics.get("realized_pnl", 0.0) or 0.0)


def _candidate_feature_vector(candidate: dict) -> list[float]:
    """提取聚类使用的高维特征向量。"""
    metrics = candidate.get("metrics", {})
    values: list[float] = []
    for key in _CLUSTER_FEATURES:
        raw = metrics.get(key, 0.0)
        values.append(float(raw) if raw is not None else 0.0)
    return values


def _cluster_candidates(candidates: list[dict]) -> list[dict]:
    """对候选做高维聚类，每个簇仅保留评分最高的代表。"""
    if len(candidates) <= 2:
        return sorted(candidates, key=lambda item: item["score"], reverse=True)

    cluster_count = max(1, min(_DEFAULT_MAX_CLUSTER_COUNT, int(np.sqrt(len(candidates)))))
    if cluster_count <= 1:
        return sorted(candidates, key=lambda item: item["score"], reverse=True)

    feature_matrix = np.array([_candidate_feature_vector(candidate) for candidate in candidates], dtype=float)
    scaler = StandardScaler()
    normalized = scaler.fit_transform(feature_matrix)

    model = KMeans(n_clusters=cluster_count, n_init=10, random_state=42)
    labels = model.fit_predict(normalized)

    best_by_cluster: dict[int, list[dict]] = {}
    for label, candidate in zip(labels.tolist(), candidates):
        best_by_cluster.setdefault(label, []).append(candidate)

    clustered: list[dict] = []
    for label_candidates in best_by_cluster.values():
        label_candidates.sort(key=lambda item: item["score"], reverse=True)
        clustered.extend(label_candidates[:_DEFAULT_CLUSTER_KEEP_PER_CLUSTER])

    clustered.sort(key=lambda item: item["score"], reverse=True)
    logger.info("高维聚类完成：原始 %d 只 -> 聚类代表 %d 只", len(candidates), len(clustered))
    return clustered


def _is_stock_pareto_dominated(candidate: dict, peers: list[dict]) -> bool:
    """判断个股是否被更低价格且得分不差的同类候选支配。"""
    if candidate.get("asset_type") != "stock":
        return False

    candidate_price = float(candidate.get("latest_price", 0.0) or 0.0)
    candidate_score = float(candidate.get("score", 0.0) or 0.0)
    if candidate_price <= 0:
        return False

    for peer in peers:
        if peer["symbol"] == candidate["symbol"] or peer.get("asset_type") != "stock":
            continue

        peer_price = float(peer.get("latest_price", 0.0) or 0.0)
        peer_score = float(peer.get("score", 0.0) or 0.0)
        if peer_price <= 0:
            continue

        score_not_worse = peer_score >= candidate_score - _DEFAULT_PARETO_SCORE_EPSILON
        price_not_higher = peer_price <= candidate_price
        strictly_better = peer_price < candidate_price or peer_score > candidate_score
        if score_not_worse and price_not_higher and strictly_better:
            return True

    return False


def _pareto_filter_stocks(candidates: list[dict]) -> list[dict]:
    """优先移除被帕累托支配的高价个股候选。"""
    filtered = [candidate for candidate in candidates if not _is_stock_pareto_dominated(candidate, candidates)]
    if filtered:
        logger.info("帕累托过滤完成：输入 %d 只 -> 输出 %d 只", len(candidates), len(filtered))
        return sorted(filtered, key=lambda item: item["score"], reverse=True)
    return sorted(candidates, key=lambda item: item["score"], reverse=True)


def _dedupe_fund_share_classes(candidates: list[dict]) -> list[dict]:
    """场外基金同前5位代码视为同组，仅保留评分最高者。"""
    best_by_group: dict[str, dict] = {}
    remained: list[dict] = []

    for candidate in candidates:
        symbol = str(candidate.get("symbol", ""))
        if candidate.get("asset_type") != "fund_open" or not symbol.isdigit() or len(symbol) != 6:
            remained.append(candidate)
            continue

        group_key = symbol[:5]
        current = best_by_group.get(group_key)
        if current is None or float(candidate.get("score", 0.0)) > float(current.get("score", 0.0)):
            best_by_group[group_key] = candidate

    deduped = remained + list(best_by_group.values())
    deduped.sort(key=lambda item: item["score"], reverse=True)
    logger.info("基金同类去重完成：输入 %d 只 -> 输出 %d 只", len(candidates), len(deduped))
    return deduped


def recommend_best_candidate(
    exclude_symbols: set[str] | None = None,
    etf_top_n: int = 8,
    stock_top_n: int = 8,
    fund_top_n: int = _DEFAULT_FUND_TOP_N,
    eval_days: int = 365,
    strategy_params: dict | None = None,
    risk_free_rate: float = 0.02,
    disable_proxy: bool = False,
) -> dict | None:
    """
    推荐 1 个在利弗莫尔策略下历史表现较优的候选代码。

    步骤：
        1. 扫描候选（ETF + 沪深300）
        2. 对每个候选做单标的回测
        3. 按评分函数排序，返回第 1 名

    返回：
        {
            "symbol": "xxxxxx",
            "asset_type": "stock|etf",
            "score": float,
            "metrics": {...}
        }
        若无有效候选则返回 None
    """
    exclude = exclude_symbols or set()
    with _temporary_disable_proxy(disable_proxy=disable_proxy):
        active_proxy = _get_active_proxy_env()
        if active_proxy:
            logger.warning("市场优选当前检测到代理环境变量：%s", ", ".join(sorted(active_proxy.keys())))
        elif disable_proxy:
            logger.info("市场优选已临时禁用代理环境变量")

        universe_meta = get_market_candidates(
            etf_top_n=etf_top_n,
            stock_top_n=stock_top_n,
            fund_top_n=fund_top_n,
            exclude_symbols=exclude,
        )
        if not universe_meta:
            logger.warning("市场优选：候选集合为空")
            return None

        end_date = date.today().strftime("%Y-%m-%d")
        start_date = (date.today() - timedelta(days=eval_days)).strftime("%Y-%m-%d")

        evaluated_candidates: list[dict] = []
        for symbol, meta in universe_meta.items():
            try:
                result = run_backtest(
                    symbols=[symbol],
                    capital=_DEFAULT_EVAL_CAPITAL,
                    start_date=start_date,
                    end_date=end_date,
                    risk_free_rate=risk_free_rate,
                    strategy_params=strategy_params,
                    asset_meta_override={symbol: meta},
                )
                metrics = result.get("metrics", {})
                if not metrics:
                    continue

                metrics = {
                    **metrics,
                    "realized_pnl": _calc_realized_pnl(result.get("trade_log", [])),
                }

                score = _score_backtest(metrics)
                candidate = {
                    "symbol": symbol,
                    "asset_type": meta.get("asset_type", "stock"),
                    "name": meta.get("name", symbol),
                    "latest_price": float(meta.get("latest_price", 0.0) or 0.0),
                    "score": score,
                    "metrics": metrics,
                }
                evaluated_candidates.append(candidate)

            except Exception as e:
                logger.warning("市场优选回测失败：%s - %s", symbol, e)
                continue

        if not evaluated_candidates:
            best = None
        else:
            deduped_candidates = _dedupe_fund_share_classes(evaluated_candidates)
            clustered_candidates = _cluster_candidates(deduped_candidates)
            filtered_candidates = _pareto_filter_stocks(clustered_candidates)
            best = filtered_candidates[0] if filtered_candidates else None

    if best:
        logger.info(
            "市场优选推荐：%s（score=%.2f, pnl=%.2f, return=%.2f%%, sharpe=%.2f, mdd=%.2f%%）",
            best["symbol"],
            best["score"],
            float(best["metrics"].get("realized_pnl", 0.0)),
            100 * float(best["metrics"].get("total_return", 0.0)),
            float(best["metrics"].get("sharpe_ratio", 0.0)),
            100 * float(best["metrics"].get("max_drawdown", 0.0)),
        )
    else:
        logger.warning("市场优选：未找到可用推荐")

    return best
