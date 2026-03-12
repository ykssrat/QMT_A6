"""
审计日志模块：以追加方式记录信号事件、交易事件和系统异常。

日志文件格式：JSON Lines（每行一条 JSON），存放于 datas/logs/audit_YYYY-MM-DD.jsonl。
只追加写入，不删除、不覆盖，支持事后全量回溯。
"""

import json
import logging
import os
import traceback
from datetime import datetime

logger = logging.getLogger(__name__)

# 日志目录：datas/logs/（相对项目根目录）
_LOG_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
    "datas", "logs",
)


def _ensure_log_dir() -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)


def _log_path(date_str: str | None = None) -> str:
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(_LOG_DIR, f"audit_{date_str}.jsonl")


def _write(record: dict) -> None:
    """将一条审计记录追加写入当日日志文件。"""
    _ensure_log_dir()
    path = _log_path()
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error("审计日志写入失败：%s", e)


# ─── 公开接口 ──────────────────────────────────────────────────────────────────

def write_signal_event(
    signal: dict,
    factor_values: dict | None = None,
    signal_date: str | None = None,
) -> None:
    """
    记录一条信号事件（建仓 / 止损 / 加仓）。

    参数：
        signal:        LivermoreStrategy.generate_signals 返回的单条信号字典
        factor_values: {因子名: 值}，记录触发信号时的因子快照（可选）
        signal_date:   信号日期，默认取当日
    """
    record = {
        "event_type":   "signal",
        "timestamp":    datetime.now().isoformat(),
        "signal_date":  signal_date or datetime.now().strftime("%Y-%m-%d"),
        "symbol":       signal.get("symbol"),
        "action":       signal.get("action"),
        "amount":       signal.get("amount"),
        "reason":       signal.get("reason"),
        "factors":      factor_values or {},
    }
    _write(record)


def write_trade_event(trade: dict, trade_date: str | None = None) -> None:
    """
    记录一条实际成交事件（来自 execute_signals 的 trade_log 条目）。

    参数：
        trade:      trade_log 中的单条成交字典，需含 symbol/action/shares/price/amount
        trade_date: 成交日期，默认取当日
    """
    record = {
        "event_type":  "trade",
        "timestamp":   datetime.now().isoformat(),
        "trade_date":  trade_date or datetime.now().strftime("%Y-%m-%d"),
        "symbol":      trade.get("symbol"),
        "action":      trade.get("action"),
        "shares":      trade.get("shares"),
        "price":       trade.get("price"),
        "amount":      trade.get("amount"),
        "pnl":         trade.get("pnl"),          # 卖出时的平仓盈亏（元），买入为 None
        "reason":      trade.get("reason"),
    }
    _write(record)


def write_daily_summary(
    trade_date: str,
    equity: float,
    daily_pnl: float,
    signal_count: int,
    trade_count: int,
) -> None:
    """
    记录每日收盘后的资产快照与损益汇总。

    参数：
        trade_date:   日期字符串 "YYYY-MM-DD"
        equity:       当日收盘总资产（元）
        daily_pnl:    当日损益（元，可正可负）
        signal_count: 当日信号条数
        trade_count:  当日实际成交笔数
    """
    record = {
        "event_type":    "daily_summary",
        "timestamp":     datetime.now().isoformat(),
        "trade_date":    trade_date,
        "equity":        round(equity, 2),
        "daily_pnl":     round(daily_pnl, 2),
        "signal_count":  signal_count,
        "trade_count":   trade_count,
    }
    _write(record)


def write_error_event(message: str, exc: Exception | None = None) -> None:
    """
    记录脚本异常事件，便于事后追溯。

    参数：
        message: 异常描述
        exc:     Exception 对象（可选），会附加完整 traceback
    """
    record = {
        "event_type": "error",
        "timestamp":  datetime.now().isoformat(),
        "message":    message,
        "traceback":  traceback.format_exc() if exc else None,
    }
    _write(record)
    logger.error("审计异常：%s", message, exc_info=exc)


def read_audit_log(date_str: str) -> list[dict]:
    """
    读取指定日期的全量审计日志。

    参数：
        date_str: "YYYY-MM-DD"

    返回：
        该日所有审计记录列表（解析后的 dict 列表，按写入顺序排列）
    """
    path = _log_path(date_str)
    if not os.path.exists(path):
        return []
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("审计日志行解析失败，已跳过：%s", line[:80])
    return records
