# -*- coding: utf-8 -*-
"""
技术指标计算
"""
import pandas as pd
import numpy as np


class TechnicalIndicators:
    """技术指标计算工具类"""

    @staticmethod
    def sma(series: pd.Series, period: int) -> pd.Series:
        """简单移动平均"""
        return series.rolling(window=period, min_periods=1).mean()

    @staticmethod
    def ema(series: pd.Series, period: int) -> pd.Series:
        """指数移动平均"""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def macd(
        series: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> tuple:
        """MACD指标"""
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        macd = (dif - dea) * 2
        return dif, dea, macd

    @staticmethod
    def kdj(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        n: int = 9,
    ) -> tuple:
        """KDJ指标"""
        low_n = low.rolling(window=n, min_periods=1).min()
        high_n = high.rolling(window=n, min_periods=1).max()
        
        rsv = (close - low_n) / (high_n - low_n + 1e-9) * 100
        
        K = rsv.ewm(alpha=1/3, adjust=False).mean()
        D = K.ewm(alpha=1/3, adjust=False).mean()
        J = 3 * K - 2 * D
        
        return K, D, J

    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """RSI指标"""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / (loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def boll(series: pd.Series, period: int = 20, std_dev: int = 2) -> tuple:
        """布林带"""
        sma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        
        upper = sma + std * std_dev
        lower = sma - std * std_dev
        
        return sma, upper, lower

    @staticmethod
    def atr(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """ATR平均真实波幅"""
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        return atr

    @staticmethod
    def volume_ratio(series: pd.Series, period: int = 5) -> pd.Series:
        """量比"""
        return series.rolling(window=period).mean() / series.rolling(window=period * 2).mean()

    @staticmethod
    def adx(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """ADX平均趋向指数"""
        plus_dm = high.diff()
        minus_dm = -low.diff()
        
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = TechnicalIndicators.atr(high, low, close, period)
        
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / tr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / tr)
        
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9)
        adx = dx.rolling(window=period).mean()
        
        return adx

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """OBV能量潮"""
        sign = close.diff().apply(lambda x: 1 if x > 0 else -1)
        obv = (sign * volume).cumsum()
        return obv