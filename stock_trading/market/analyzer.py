"""
市场分析模块
"""

from typing import Dict, Optional
import pandas as pd

from ..data.fetcher import StockFetcher
from ..config import CONFIG


class MarketAnalyzer:
    """市场分析器"""
    
    def __init__(self):
        self.fetcher = StockFetcher()
    
    def get_realtime_price(self, stock_code: str) -> Dict:
        """获取实时价格"""
        return self.fetcher.get_realtime(stock_code)
    
    def get_fund_flow(self, stock_code: str) -> Dict:
        """获取资金流向"""
        return self.fetcher.get_fund_flow(stock_code)
    
    def get_board_industry(self) -> pd.DataFrame:
        """获取行业板块"""
        return self.fetcher.get_board_industry()
    
    def get_board_concept(self) -> pd.DataFrame:
        """获取概念板块"""
        return self.fetcher.get_board_concept()
    
    def get_market_overview(self) -> Dict:
        """获取市场概览"""
        df = self.fetcher.get_realtime_all()
        if df.empty:
            return {}
        
        return {
            "total_stocks": len(df),
            "up_count": len(df[df['涨跌幅'] > 0]),
            "down_count": len(df[df['涨跌幅'] < 0]),
            "flat_count": len(df[df['涨跌幅'] == 0]),
            "total_amount": df['成交额'].sum(),
            "avg_change": df['涨跌幅'].mean(),
        }
    
    def get_stock_pe(self, stock_code: str) -> Optional[float]:
        """获取市盈率(简单估算)"""
        data = self.fetcher.get_realtime(stock_code)
        if not data:
            return None
        # 注意: 这只是简单估算，实际需要财务数据
        return data.get('pe', None)
    
    def get_stock_pb(self, stock_code: str) -> Optional[float]:
        """获取市净率(简单估算)"""
        data = self.fetcher.get_realtime(stock_code)
        if not data:
            return None
        return data.get('pb', None)