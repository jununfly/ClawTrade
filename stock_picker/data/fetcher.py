# -*- coding: utf-8 -*-
"""
数据获取模块
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import tushare as ts
from tqdm import tqdm

logger = logging.getLogger(__name__)


class DataFetcher:
    """数据获取器"""

    def __init__(self, data_source: str = "akshare", data_dir: Optional[Path] = None):
        """
        初始化数据获取器
        
        Args:
            data_source: 数据源 ("akshare" 或 "tushare")
            data_dir: 数据存储目录
        """
        self.data_source = data_source
        self.data_dir = data_dir or Path("data/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化Tushare
        if data_source == "tushare":
            token = os.environ.get("TUSHARE_TOKEN", "")
            if token:
                ts.set_token(token)
                self.pro = ts.pro_api()
            else:
                logger.warning("未设置TUSHARE_TOKEN环境变量")

    def fetch_kline(
        self,
        code: str,
        start: str = "20190101",
        end: Optional[str] = None,
        adj: str = "qfq",
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            code: 股票代码 (6位数字)
            start: 开始日期
            end: 结束日期，默认今天
            adj: 复权类型 ("qfq" 前复权, "hfq" 后复权, "None" 不复权)
            
        Returns:
            DataFrame: 日线数据
        """
        if end is None:
            end = datetime.now().strftime("%Y%m%d")
        
        if self.data_source == "tushare":
            return self._fetch_from_tushare(code, start, end, adj)
        else:
            return self._fetch_from_akshare(code, start, end, adj)

    def _fetch_from_tushare(
        self,
        code: str,
        start: str,
        end: str,
        adj: str,
    ) -> pd.DataFrame:
        """从Tushare获取数据"""
        ts_code = self._to_ts_code(code)
        
        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                adj=adj,
                start_date=start,
                end_date=end,
                freq="D",
                api=self.pro if hasattr(self, "pro") else None,
            )
        except Exception as e:
            logger.error(f"获取{code}数据失败: {e}")
            return pd.DataFrame()
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        df = df.rename(columns={"trade_date": "date", "vol": "volume"})[
            ["date", "open", "close", "high", "low", "volume"]
        ].copy()
        
        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df.sort_values("date").reset_index(drop=True)

    def _fetch_from_akshare(
        self,
        code: str,
        start: str,
        end: str,
        adj: str,
    ) -> pd.DataFrame:
        """从AkShare获取数据"""
        try:
            import akshare as ak
        except ImportError:
            logger.error("请安装akshare: pip install akshare")
            return pd.DataFrame()
        
        try:
            # 尝试获取A股数据
            if code.startswith(("60", "68")):
                df = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust=adj)
            else:
                df = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust=adj)
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 统一列名
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            })[["date", "open", "close", "high", "low", "volume"]].copy()
            
            df["date"] = pd.to_datetime(df["date"])
            for col in ["open", "close", "high", "low", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            return df.sort_values("date").reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"获取{code}数据失败: {e}")
            return pd.DataFrame()

    def _to_ts_code(self, code: str) -> str:
        """转换为tushare格式"""
        code = str(code).zfill(6)
        if code.startswith(("60", "68", "9")):
            return f"{code}.SH"
        elif code.startswith(("4", "8")):
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"

    def fetch_stock_list(self, exclude_boards: Optional[set] = None) -> List[str]:
        """
        获取股票列表
        
        Args:
            exclude_boards: 排除的板块 ("gem", "star", "bj")
            
        Returns:
            股票代码列表
        """
        exclude_boards = exclude_boards or set()
        
        if self.data_source == "tushare" and hasattr(self, "pro"):
            return self._fetch_stock_list_tushare(exclude_boards)
        else:
            return self._fetch_stock_list_akshare(exclude_boards)

    def _fetch_stock_list_tushare(self, exclude_boards: set) -> List[str]:
        """从Tushare获取股票列表"""
        try:
            df = self.pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol")
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        
        codes = df["symbol"].tolist()
        
        # 排除板块
        if "gem" in exclude_boards:
            codes = [c for c in codes if not c.startswith(("300", "301"))]
        if "star" in exclude_boards:
            codes = [c for c in codes if not c.startswith("688")]
        if "bj" in exclude_boards:
            codes = [c for c in codes if not c.startswith(("4", "8"))]
        
        return codes

    def _fetch_stock_list_akshare(self, exclude_boards: set) -> List[str]:
        """从AkShare获取股票列表"""
        try:
            import akshare as ak
            df = ak.stock_info_a_code_name()
            codes = df["code"].tolist()
            
            # 排除板块
            if "gem" in exclude_boards:
                codes = [c for c in codes if not c.startswith(("300", "301"))]
            if "star" in exclude_boards:
                codes = [c for c in codes if not c.startswith("688")]
            if "bj" in exclude_boards:
                codes = [c for c in codes if not c.startswith(("4", "8"))]
            
            return codes
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []

    def fetch_all(
        self,
        codes: Optional[List[str]] = None,
        start: str = "20190101",
        end: Optional[str] = None,
        workers: int = 4,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票数据
        
        Args:
            codes: 股票代码列表，None时获取全部
            start: 开始日期
            end: 结束日期
            workers: 并发数
            
        Returns:
            股票代码到DataFrame的映射
        """
        if codes is None:
            codes = self.fetch_stock_list()
        
        if end is None:
            end = datetime.now().strftime("%Y%m%d")
        
        results: Dict[str, pd.DataFrame] = {}
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self.fetch_kline, code, start, end): code
                for code in codes
            }
            
            for fut in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
                code = futures[fut]
                try:
                    df = fut.result()
                    if not df.empty:
                        # 保存到文件
                        csv_path = self.data_dir / f"{code}.csv"
                        df.to_csv(csv_path, index=False)
                        results[code] = df
                except Exception as e:
                    logger.error(f"{code}下载失败: {e}")
        
        return results

    def load_csv(self, code: str) -> pd.DataFrame:
        """从CSV文件加载数据"""
        csv_path = self.data_dir / f"{code}.csv"
        if not csv_path.exists():
            return pd.DataFrame()
        
        df = pd.read_csv(csv_path)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def load_all_csv(self) -> Dict[str, pd.DataFrame]:
        """加载目录下所有CSV文件"""
        results = {}
        for csv_path in self.data_dir.glob("*.csv"):
            code = csv_path.stem
            df = self.load_csv(code)
            if not df.empty:
                results[code] = df
        return results