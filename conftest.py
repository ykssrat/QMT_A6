"""
pytest 全局配置：将项目根目录加入模块搜索路径，确保 `scripts.*` 包可以在测试中正常 import。
"""
import os
import sys

# 将项目根目录（本文件所在目录）插入 sys.path 最前面
sys.path.insert(0, os.path.dirname(__file__))
