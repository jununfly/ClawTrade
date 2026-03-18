# -*- coding: utf-8 -*-
"""
ClawTrade - 股票交易系统
"""

__version__ = "1.0.0"
__author__ = "Zj"

from .selector.base import BaseSelector
from .selector.b1_selector import B1Selector
from .selector.brick_selector import BrickChartSelector

__all__ = [
    "BaseSelector",
    "B1Selector",
    "BrickChartSelector",
]