"""
基础策略类
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd


class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str = "BaseStrategy", **kwargs):
        self.name = name
        self.params = kwargs
        self.signals = []
    
    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        生成交易信号
        
        Args:
            data: K线数据
        
        Returns:
            DataFrame with signal column
        """
        pass
    
    def get_signal(self, data: pd.DataFrame) -> Optional[str]:
        """
        获取当前信号
        
        Args:
            data: K线数据
        
        Returns:
            "BUY", "SELL", "HOLD" or None
        """
        df = self.generate_signals(data)
        if df.empty or 'signal' not in df.columns:
            return "HOLD"
        
        return df.iloc[-1].get('signal', 'HOLD')
    
    def calculate_returns(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算收益率"""
        if 'close' not in data.columns:
            return data
        
        df = data.copy()
        df['returns'] = df['close'].pct_change()
        df['cum_returns'] = (1 + df['returns']).cumprod() - 1
        return df
    
    def get_params(self) -> Dict:
        """获取策略参数"""
        return self.params
    
    def set_params(self, **kwargs):
        """设置策略参数"""
        self.params.update(kwargs)