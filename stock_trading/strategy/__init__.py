"""
策略模块
"""

from .base import BaseStrategy
from .macd import MACDStrategy
from .mean_reversion import MeanReversionStrategy

__all__ = ["BaseStrategy", "MACDStrategy", "MeanReversionStrategy"]