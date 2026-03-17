"""
MACD策略
"""

import pandas as pd
import numpy as np
from .base import BaseStrategy


class MACDStrategy(BaseStrategy):
    """MACD策略
    
    策略逻辑:
    - MACD金叉(快线穿过慢线) -> 买入信号
    - MACD死叉(快线下穿慢线) -> 卖出信号
    """
    
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9, **kwargs):
        super().__init__(name="MACD", fast=fast, slow=slow, signal=signal, **kwargs)
        self.fast = fast
        self.slow = slow
        self.signal = signal
    
    def calculate_ema(self, series: pd.Series, period: int) -> pd.Series:
        """计算指数移动平均"""
        return series.ewm(span=period, adjust=False).mean()
    
    def calculate_macd(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算MACD指标"""
        df = data.copy()
        
        # 计算EMA
        df['ema_fast'] = self.calculate_ema(df['close'], self.fast)
        df['ema_slow'] = self.calculate_ema(df['close'], self.slow)
        
        # 计算DIF和DEA
        df['dif'] = df['ema_fast'] - df['ema_slow']
        df['dea'] = self.calculate_ema(df['dif'], self.signal)
        df['macd'] = (df['dif'] - df['dea']) * 2
        
        return df
    
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """生成MACD交易信号"""
        if data.empty or 'close' not in data.columns:
            return pd.DataFrame()
        
        df = self.calculate_macd(data)
        
        # 初始化信号
        df['signal'] = 'HOLD'
        
        # 计算DIF和DEA的交叉
        df['dif_prev'] = df['dif'].shift(1)
        df['dea_prev'] = df['dea'].shift(1)
        
        # 金叉: DIF从下往上穿过DEA
        golden_cross = (df['dif'] > df['dea']) & (df['dif_prev'] <= df['dea_prev'])
        # 死叉: DIF从上往下穿过DEA
        death_cross = (df['dif'] < df['dea']) & (df['dif_prev'] >= df['dea_prev'])
        
        df.loc[golden_cross, 'signal'] = 'BUY'
        df.loc[death_cross, 'signal'] = 'SELL'
        
        # 只保留需要的列
        result = df[['date', 'close', 'dif', 'dea', 'macd', 'signal']].copy()
        
        return result
    
    def get_latest_signal(self, data: pd.DataFrame) -> str:
        """获取最新信号"""
        df = self.generate_signals(data)
        if df.empty:
            return 'HOLD'
        return df.iloc[-1]['signal']