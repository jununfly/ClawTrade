# -*- coding: utf-8 -*-
"""
数据模块
"""

from .fetcher import DataFetcher
from .storage import DataStorage

__all__ = [
    "DataFetcher",
    "DataStorage",
]