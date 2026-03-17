"""
数据获取模块
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    print("Warning: akshare not installed. Data fetching will not work.")

from ..config import CONFIG


class StockFetcher:
    """股票数据获取器"""
    
    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or CONFIG["cache_dir"]
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def get_kline(self, stock_code: str, period: str = "daily", 
                  start_date: str = None, end_date: str = None,
                  adjust: str = "qfq") -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            stock_code: 股票代码，如 "600519"
            period: 周期 "daily", "weekly", "monthly"
            start_date: 开始日期，如 "20230101"
            end_date: 结束日期，如 "20231231"
            adjust: 复权类型 "qfq" 前复权, "hfq" 后复权, ""
        
        Returns:
            DataFrame with OHLC data
        """
        if not AKSHARE_AVAILABLE:
            raise RuntimeError("akshare not available")
        
        # 转换股票代码格式
        if stock_code.startswith("6"):
            symbol = f"sh{stock_code}"
        elif stock_code.startswith(("0", "3")):
            symbol = f"sz{stock_code}"
        else:
            symbol = stock_code
        
        try:
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            return df
        except Exception as e:
            print(f"Error fetching kline: {e}")
            return pd.DataFrame()
    
    def get_realtime(self, stock_code: str) -> dict:
        """
        获取实时行情
        
        Args:
            stock_code: 股票代码
        
        Returns:
            dict with price info
        """
        if not AKSHARE_AVAILABLE:
            return {}
        
        try:
            df = ak.stock_zh_a_spot_em()
            row = df[df['代码'] == stock_code]
            if not row.empty:
                return {
                    "code": stock_code,
                    "name": row.iloc[0]['名称'],
                    "price": row.iloc[0]['最新价'],
                    "change": row.iloc[0]['涨跌幅'],
                    "volume": row.iloc[0]['成交量'],
                    "amount": row.iloc[0]['成交额'],
                    "high": row.iloc[0]['最高'],
                    "low": row.iloc[0]['最低'],
                    "open": row.iloc[0]['今开'],
                    "close": row.iloc[0]['昨收'],
                }
        except Exception as e:
            print(f"Error fetching realtime: {e}")
        return {}
    
    def get_realtime_all(self) -> pd.DataFrame:
        """获取所有A股实时行情"""
        if not AKSHARE_AVAILABLE:
            return pd.DataFrame()
        
        try:
            return ak.stock_zh_a_spot_em()
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def get_fund_flow(self, stock_code: str) -> dict:
        """
        获取资金流向
        
        Args:
            stock_code: 股票代码
        
        Returns:
            dict with fund flow data
        """
        if not AKSHARE_AVAILABLE:
            return {}
        
        # 确定市场
        if stock_code.startswith("6"):
            market = "sh"
        else:
            market = "sz"
        
        try:
            df = ak.stock_individual_fund_flow(stock=stock_code, market=market)
            if not df.empty:
                return {
                    "code": stock_code,
                    "main_inflow": df.iloc[0]['主力净流入-净额'] if '主力净流入-净额' in df.columns else 0,
                    "main_inflow_pct": df.iloc[0]['主力净流入-净占比'] if '主力净流入-净占比' in df.columns else 0,
                }
        except Exception as e:
            print(f"Error: {e}")
        return {}
    
    def get_board_industry(self) -> pd.DataFrame:
        """获取行业板块行情"""
        if not AKSHARE_AVAILABLE:
            return pd.DataFrame()
        
        try:
            return ak.stock_board_industry_name_em()
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def get_board_concept(self) -> pd.DataFrame:
        """获取概念板块行情"""
        if not AKSHARE_AVAILABLE:
            return pd.DataFrame()
        
        try:
            return ak.stock_board_concept_name_em()
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def cache_data(self, name: str, data: pd.DataFrame):
        """缓存数据到本地"""
        if data.empty:
            return
        path = os.path.join(self.cache_dir, f"{name}.parquet")
        data.to_parquet(path)
    
    def load_cache(self, name: str) -> Optional[pd.DataFrame]:
        """从缓存加载数据"""
        path = os.path.join(self.cache_dir, f"{name}.parquet")
        if os.path.exists(path):
            return pd.read_parquet(path)
        return None