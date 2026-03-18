# -*- coding: utf-8 -*-
"""
选股基类
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd


@dataclass
class Candidate:
    """候选股票"""
    code: str           # 股票代码
    date: str           # 选股日期
    strategy: str       # 策略名称
    close: float        # 收盘价
    turnover_n: float  # 成交额
    brick_growth: Optional[float] = None  # 砖型图增长倍数


class BaseSelector(ABC):
    """选股器基类"""

    def __init__(
        self,
        name: str = "BaseSelector",
        min_bars: int = 60,
        date_col: str = "date",
    ):
        self.name = name
        self.min_bars = min_bars
        self.date_col = date_col

    @abstractmethod
    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预计算所有中间列，返回含 _vec_pick 的 DataFrame
        
        Args:
            df: 原始日线数据
            
        Returns:
            预处理后的DataFrame，包含 _vec_pick 列
        """
        pass

    @abstractmethod
    def passes_df_on_date(self, df: pd.DataFrame, date: pd.Timestamp) -> bool:
        """
        单日判断
        
        Args:
            df: 预处理后的DataFrame
            date: 判断日期
            
        Returns:
            是否通过选股条件
        """
        pass

    def vec_picks_from_prepared(
        self,
        df: pd.DataFrame,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> List[pd.Timestamp]:
        """
        从预计算列批量获取通过日期
        
        Args:
            df: 预处理后的DataFrame
            start: 开始日期
            end: 结束日期
            
        Returns:
            通过选股条件的日期列表
        """
        if "_vec_pick" not in df.columns:
            return []
        
        mask = df["_vec_pick"].astype(bool)
        if start is not None:
            mask = mask & (df.index >= start)
        if end is not None:
            mask = mask & (df.index <= end)
        
        return list(df.index[mask])

    def _get_hist(self, df: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
        """获取截至date的历史数据"""
        if self.date_col in df.columns:
            return df[df[self.date_col] <= date]
        if isinstance(df.index, pd.DatetimeIndex):
            return df.loc[:date]
        raise KeyError(f"DataFrame must have '{self.date_col}' column or DatetimeIndex")

    def select(
        self,
        data: Dict[str, pd.DataFrame],
        date: pd.Timestamp,
    ) -> List[str]:
        """
        从股票池中选出符合条件的股票
        
        Args:
            data: 股票代码到DataFrame的映射
            date: 选股日期
            
        Returns:
            符合条件的股票代码列表
        """
        results = []
        for code, df in data.items():
            if self.passes_df_on_date(df, date):
                results.append(code)
        return results