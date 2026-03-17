"""
均值回归策略
"""

import pandas as pd
import numpy as np
from .base import BaseStrategy


class MeanReversionStrategy(BaseStrategy):
    """均值回归策略
    
    策略逻辑:
    - 价格偏离均线太多时买入/卖出
    - 价格低于均线一定比例 -> 买入
    - 价格高于均线一定比例 -> 卖出
    """
    
    def __init__(self, period: int = 20, threshold: float = 0.02, **kwargs):
        super().__init__(
            name="MeanReversion", 
            period=period, 
            threshold=threshold,
            **kwargs
        )
        self.period = period
        self.threshold = threshold
    
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """生成均值回归交易信号"""
        if data.empty or 'close' not in data.columns:
            return pd.DataFrame()
        
        df = data.copy()
        
        # 计算均线
        df['ma'] = df['close'].rolling(window=self.period).mean()
        
        # 计算偏离度
        df['deviation'] = (df['close'] - df['ma']) / df['ma']
        
        # 初始化信号
        df['signal'] = 'HOLD'
        
        # 买入信号: 价格低于均线threshold
        buy_condition = df['deviation'] < -self.threshold
        # 卖出信号: 价格高于均线threshold
        sell_condition = df['deviation'] > self.threshold
        
        df.loc[buy_condition, 'signal'] = 'BUY'
        df.loc[sell_condition, 'signal'] = 'SELL'
        
        result = df[['date', 'close', 'ma', 'deviation', 'signal']].copy()
        return result
    
    def get_latest_signal(self, data: pd.DataFrame) -> str:
        """获取最新信号"""
        df = self.generate_signals(data)
        if df.empty:
            return 'HOLD'
        return df.iloc[-1]['signal']