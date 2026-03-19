# -*- coding: utf-8 -*-
"""
技术指标计算
"""
import pandas as pd
import numpy as np
from numba import njit


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

    @staticmethod
    def cci(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        """
        CCI商品通道指数
        
        计算公式：
        TP = (high + low + close) / 3
        MAD = mean(|TP - mean(TP)|)
        CCI = (TP - MA(TP)) / (0.015 * MAD)
        """
        tp = (high + low + close) / 3
        ma_tp = tp.rolling(window=n, min_periods=1).mean()
        mad = (tp - ma_tp).abs().rolling(window=n, min_periods=1).mean()
        cci = (tp - ma_tp) / (0.015 * mad + 1e-9)
        return cci

    @staticmethod
    def bbi(close: pd.Series) -> pd.Series:
        """
        BBI多空指标
        
        计算公式：
        BBI = (MA3 + MA6 + MA12 + MA24) / 4
        """
        ma3 = close.rolling(window=3, min_periods=1).mean()
        ma6 = close.rolling(window=6, min_periods=1).mean()
        ma12 = close.rolling(window=12, min_periods=1).mean()
        ma24 = close.rolling(window=24, min_periods=1).mean()
        bbi = (ma3 + ma6 + ma12 + ma24) / 4
        return bbi

    @staticmethod
    def ma20_trend(close: pd.Series) -> pd.Series:
        """
        MA20趋势判断
        
        返回：1=向上, 0=持平, -1=向下
        """
        ma20 = close.rolling(window=20, min_periods=20).mean()
        # 比较今天和昨天的MA20
        trend = (ma20 > ma20.shift(1)).astype(int)
        trend = trend.where(ma20.notna() & ma20.shift(1).notna(), 0)
        return trend

    @staticmethod
    def volume_ma5(close: pd.Series, volume: pd.Series) -> pd.Series:
        """成交量5日均线"""
        return volume.rolling(window=5, min_periods=1).mean()

    @staticmethod
    def bottom_signal(
        close: pd.Series,
        volume: pd.Series,
        open_: pd.Series,
        high: pd.Series,
        low: pd.Series,
    ) -> pd.Series:
        """
        底部放量大阳线信号识别
        
        条件：
        1. 成交量放大1.8倍
        2. 涨幅 > 4.5%
        3. 上影线 < 35%
        4. 阳线（close > open）
        """
        vol_ma5 = volume.rolling(window=5, min_periods=1).mean()
        
        # 成交量放大1.8倍
        vk_vol = (volume > vol_ma5 * 1.8) & (volume > volume.shift(1) * 1.8)
        
        # 涨幅 > 4.5%
        vk_gain = ((close - close.shift(1)) / (close.shift(1) + 1e-9) * 100) > 4.5
        
        # 上影线 < 35%
        vk_shadow = ((high - np.maximum(open_, close)) / (high - low + 1e-3)) < 0.35
        
        # 阳线
        vk_bull = close > open_
        
        # 综合信号
        vk_signal = vk_vol & vk_gain & vk_shadow & vk_bull
        
        # 30天内有信号
        vk_recent = vk_signal.rolling(window=30, min_periods=1).sum() >= 1
        return vk_recent.astype(int)


@njit(cache=True)
def _calc_kdj_numba(
    close: np.ndarray, high: np.ndarray, low: np.ndarray, n: int = 9
) -> tuple:
    """Numba加速的KDJ计算"""
    length = len(close)
    K = np.empty(length, dtype=np.float64)
    D = np.empty(length, dtype=np.float64)
    J = np.empty(length, dtype=np.float64)
    
    K[0] = D[0] = 50.0
    
    for i in range(1, length):
        start = max(0, i - n + 1)
        low_n = low[start:i + 1].min()
        high_n = high[start:i + 1].max()
        
        rng = high_n - low_n
        if rng < 1e-9:
            rng = 1e-9
        
        rsv = (close[i] - low_n) / rng * 100
        
        K[i] = 2.0 / 3.0 * K[i - 1] + 1.0 / 3.0 * rsv
        D[i] = 2.0 / 3.0 * D[i - 1] + 1.0 / 3.0 * K[i]
        J[i] = 3.0 * K[i] - 2.0 * D[i]
    
    return K, D, J


def compute_all_indicators(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    计算所有技术指标（用于评分引擎）
    
    指标列表：
    - KDJ (K, D, J)
    - BBI 多空指标
    - CCI 商品通道指数
    - MA20 趋势
    - 底部放量大阳线信号
    
    Args:
        df: 包含 high, low, close, open, volume 列的DataFrame
        config: 配置字典
        
    Returns:
        添加了技术指标列的DataFrame
    """
    if config is None:
        config = {}
    
    df = df.copy()
    
    ti = TechnicalIndicators
    
    # KDJ
    K, D, J = ti.kdj(df["high"], df["low"], df["close"])
    df["kdj_K"] = K
    df["kdj_D"] = D
    df["kdj_J"] = J
    
    # BBI
    df["BBI"] = ti.bbi(df["close"])
    
    # CCI
    df["CCI"] = ti.cci(df["high"], df["low"], df["close"])
    
    # MA20趋势
    df["MA20_TREND"] = ti.ma20_trend(df["close"])
    
    # 底部放量大阳线信号
    df["VK_RECENT"] = ti.bottom_signal(
        df["close"], df["volume"], df["open"], df["high"], df["low"]
    )
    
    return df


def check_buy_signal(row: pd.Series, config: dict = None) -> dict:
    """
    检查是否满足买入信号
    
    硬条件检查：
    1. KDJ J值 <= 16
    2. 总评分 >= 70
    3. VK_RECENT == 1（有底部放量信号）
    4. 无风险标记
    
    Args:
        row: 包含技术指标的Series
        config: 配置字典
        
    Returns:
        {'is_buy': bool, 'reasons': list}
    """
    if config is None:
        config = {
            'kdj_j_max': 16,
            'min_score': 70,
            'require_vk_recent': True,
        }
    
    reasons = []
    is_buy = True
    
    # 检查KDJ J值
    kdj_j = row.get('kdj_J', 0)
    if pd.notna(kdj_j) and kdj_j > config.get('kdj_j_max', 16):
        is_buy = False
        reasons.append(f"KDJ J值({kdj_j:.1f})超买")
    
    # 检查评分
    total_score = row.get('TOTAL_SCORE', 0)
    if pd.notna(total_score) and total_score < config.get('min_score', 70):
        is_buy = False
        reasons.append(f"评分不足({total_score:.0f} < 70)")
    
    # 检查底部信号
    vk_recent = row.get('VK_RECENT', 0)
    if config.get('require_vk_recent', True) and vk_recent != 1:
        is_buy = False
        reasons.append("无底部放量信号")
    
    # 检查风险标记
    total_risk = row.get('TOTAL_RISK', 0)
    if pd.notna(total_risk) and total_risk > 0:
        is_buy = False
        reasons.append(f"有风险标记({total_risk})")
    
    return {'is_buy': is_buy, 'reasons': reasons}