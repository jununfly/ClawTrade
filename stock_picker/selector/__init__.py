# -*- coding: utf-8 -*-
"""
选股模块
"""

from .base import BaseSelector
from .b1_selector import B1Selector
from .brick_selector import BrickChartSelector

__all__ = [
    "BaseSelector",
    "B1Selector",
    "BrickChartSelector",
]