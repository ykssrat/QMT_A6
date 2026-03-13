"""
离线候选池构建脚本：预先抓取全市场候选元信息并写入本地文件。

用途：
    1. 将股票、ETF/LOF、场外基金候选写入本地 JSON 文件
    2. 供 recommend_one.py 在离线模式下直接读取，不再运行时临时联网扫候选
"""

import argparse
import logging
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.utils.market_scanner import refresh_candidate_pool


def main() -> None:
    logging.disable(logging.CRITICAL)

    parser = argparse.ArgumentParser(description="构建离线候选池")
    parser.add_argument("--disable-proxy", action="store_true", default=True, help="构建候选池时临时禁用代理")
    args = parser.parse_args()

    path = refresh_candidate_pool(disable_proxy=bool(args.disable_proxy))
    print(path)


if __name__ == "__main__":
    main()