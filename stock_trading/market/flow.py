"""
资金流向模块
"""

from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta

from ..data.fetcher import StockFetcher
from ..config import CONFIG


class FlowAnalyzer:
    """资金流向分析器"""
    
    def __init__(self):
        self.fetcher = StockFetcher()
    
    def get_stock_flow(self, stock_code: str) -> Dict:
        """
        获取个股资金流向
        
        Args:
            stock_code: 股票代码
        
        Returns:
            资金流向数据
        """
        # 确定市场
        if stock_code.startswith("6"):
            market = "sh"
        else:
            market = "sz"
        
        try:
            df = self.fetcher.get_fund_flow(stock_code)
            return df
        except Exception as e:
            print(f"Error fetching fund flow: {e}")
            return {}
    
    def get_stock_flow_detail(self, stock_code: str, period: str = "5") -> Dict:
        """
        获取资金流向明细
        
        Args:
            stock_code: 股票代码
            period: 时间周期 ("5"=5日, "10"=10日, "20"=20日)
        
        Returns:
            资金流向详情
        """
        if not stock_code.startswith("6") and not stock_code.startswith(("0", "3")):
            return {}
        
        market = "sh" if stock_code.startswith("6") else "sz"
        
        try:
            import akshare as ak
            df = ak.stock_individual_fund_flow(stock=stock_code, market=market)
            
            if df.empty:
                return {}
            
            # 解析数据
            result = {
                "code": stock_code,
                "date": datetime.now().strftime("%Y-%m-%d"),
            }
            
            # 列名映射
            columns_map = {
                '股票代码': 'code',
                '股票简称': 'name',
                '主力净流入-净额': 'main_inflow',
                '主力净流入-净占比': 'main_inflow_pct',
                '超大单净流入-净额': 'huge_inflow',
                '超大单净流入-净占比': 'huge_inflow_pct',
                '大单净流入-净额': 'big_inflow',
                '大单净流入-净占比': 'big_inflow_pct',
                '中单净流入-净额': 'mid_inflow',
                '中单净流入-净占比': 'mid_inflow_pct',
                '小单净流入-净额': 'small_inflow',
                '小单净流入-净占比': 'small_inflow_pct',
            }
            
            for cn, en in columns_map.items():
                if cn in df.columns:
                    result[en] = df.iloc[0][cn]
            
            return result
            
        except Exception as e:
            print(f"Error: {e}")
            return {}
    
    def get_sector_flow(self, sector_name: str = None) -> pd.DataFrame:
        """
        获取板块资金流向
        
        Args:
            sector_name: 板块名称（可选）
        
        Returns:
            板块资金流向 DataFrame
        """
        try:
            import akshare as ak
            
            if sector_name:
                df = ak.stock_sector_fund_flow_rank(symbol=sector_name)
            else:
                # 默认获取行业板块
                df = ak.stock_board_industry_name_em()
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def get_market_flow(self) -> Dict:
        """
        获取市场整体资金流向
        
        Returns:
            市场资金流向
        """
        try:
            import akshare as ak
            
            # 尝试获取市场资金流向
            df = ak.stock_market_fund_flow()
            
            if df.empty:
                return {}
            
            result = {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "inflow_main": df.iloc[0]['主力净流入'] if '主力净流入' in df.columns else 0,
                "inflow_huge": df.iloc[0]['超大单净流入'] if '超大单净流入' in df.columns else 0,
                "inflow_big": df.iloc[0]['大单净流入'] if '大单净流入' in df.columns else 0,
                "inflow_mid": df.iloc[0]['中单净流入'] if '中单净流入' in df.columns else 0,
                "inflow_small": df.iloc[0]['小单净流入'] if '小单净流入' in df.columns else 0,
            }
            
            return result
            
        except Exception as e:
            # 如果获取失败，返回空
            return {}
    
    def get_main_inflow_stocks(self, limit: int = 10) -> pd.DataFrame:
        """
        获取主力资金净流入最多的股票
        
        Args:
            limit: 返回数量
        
        Returns:
            股票列表
        """
        try:
            import akshare as ak
            
            df = ak.stock_market_fund_flow()
            
            if '主力净流入' in df.columns:
                df = df.sort_values('主力净流入', ascending=False).head(limit)
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def get_main_outflow_stocks(self, limit: int = 10) -> pd.DataFrame:
        """
        获取主力资金净流出最多的股票
        
        Args:
            limit: 返回数量
        
        Returns:
            股票列表
        """
        try:
            import akshare as ak
            
            df = ak.stock_market_fund_flow()
            
            if '主力净流入' in df.columns:
                df = df.sort_values('主力净流入', ascending=True).head(limit)
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def analyze_flow_trend(self, stock_code: str, days: int = 5) -> Dict:
        """
        分析资金流向趋势
        
        Args:
            stock_code: 股票代码
            days: 分析天数
        
        Returns:
            趋势分析结果
        """
        flows = []
        
        for i in range(days):
            date = (datetime.now() - timedelta(days=i+1)).strftime("%Y%m%d")
            # 简化处理：获取当日资金流向
            flow = self.get_stock_flow(stock_code)
            if flow:
                flow['date'] = date
                flows.append(flow)
        
        if not flows:
            return {"trend": "unknown", "message": "No data"}
        
        # 计算趋势
        main_inflows = [f.get('main_inflow', 0) for f in flows]
        
        if not main_inflows:
            return {"trend": "unknown"}
        
        # 判断趋势
        if main_inflows[0] > 0 and all(main_inflows[i] >= main_inflows[i-1] for i in range(1, len(main_inflows))):
            trend = "increasing"
            message = "主力资金持续净流入"
        elif main_inflows[0] < 0 and all(main_inflows[i] <= main_inflows[i-1] for i in range(1, len(main_inflows))):
            trend = "decreasing"
            message = "主力资金持续净流出"
        elif sum(main_inflows) > 0:
            trend = "net_inflow"
            message = "主力资金总体净流入"
        elif sum(main_inflows) < 0:
            trend = "net_outflow"
            message = "主力资金总体净流出"
        else:
            trend = "neutral"
            message = "主力资金进出平衡"
        
        return {
            "trend": trend,
            "message": message,
            "flows": flows,
            "avg_inflow": sum(main_inflows) / len(main_inflows) if main_inflows else 0,
        }
    
    def get_flow_indicator(self, stock_code: str) -> str:
        """
        获取资金流向信号
        
        Args:
            stock_code: 股票代码
        
        Returns:
            信号: "BUY", "SELL", "HOLD"
        """
        flow = self.get_stock_flow_detail(stock_code)
        
        if not flow:
            return "HOLD"
        
        main_inflow = flow.get('main_inflow', 0)
        main_inflow_pct = flow.get('main_inflow_pct', 0)
        
        # 简单判断逻辑
        if main_inflow > 0 and main_inflow_pct > 5:
            return "BUY"
        elif main_inflow < 0 and main_inflow_pct < -5:
            return "SELL"
        else:
            return "HOLD"


class MoneyFlowTracker:
    """资金流向追踪器"""
    
    def __init__(self):
        self.flow_analyzer = FlowAnalyzer()
        self.tracked_stocks: List[str] = []
    
    def add_stock(self, stock_code: str):
        """添加追踪股票"""
        if stock_code not in self.tracked_stocks:
            self.tracked_stocks.append(stock_code)
    
    def remove_stock(self, stock_code: str):
        """移除追踪股票"""
        if stock_code in self.tracked_stocks:
            self.tracked_stocks.remove(stock_code)
    
    def get_tracked_flows(self) -> List[Dict]:
        """获取追踪股票的资金流向"""
        results = []
        
        for code in self.tracked_stocks:
            flow = self.flow_analyzer.get_stock_flow_detail(code)
            if flow:
                flow['signal'] = self.flow_analyzer.get_flow_indicator(code)
                results.append(flow)
        
        return results
    
    def get_buying_signals(self) -> List[Dict]:
        """获取买入信号股票"""
        flows = self.get_tracked_flows()
        return [f for f in flows if f.get('signal') == 'BUY']
    
    def get_selling_signals(self) -> List[Dict]:
        """获取卖出信号股票"""
        flows = self.get_tracked_flows()
        return [f for f in flows if f.get('signal') == 'SELL']