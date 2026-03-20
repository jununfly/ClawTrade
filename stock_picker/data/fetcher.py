# -*- coding: utf-8 -*-
"""
数据获取模块
"""
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import tushare as ts
from tqdm import tqdm

logger = logging.getLogger(__name__)

# 默认配置
DEFAULT_TIMEOUT = 10  # 请求超时时间（秒）
DEFAULT_REQUEST_DELAY = 1.0  # 请求间隔（秒）
DEFAULT_BATCH_SIZE = 100  # 每批数量
DEFAULT_BATCH_DELAY = 60  # 每批之间等待秒数
DEFAULT_MAX_RETRIES = 3  # 最大重试次数
DEFAULT_BASE_DELAY = 2.0  # 基础退避时间（秒）


class FetchStats:
    """获取统计信息"""
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.skipped = 0
        self.retries = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    @property
    def elapsed_time(self) -> float:
        if self.start_time is None:
            return 0
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0
        return self.success / self.total * 100
    
    def __str__(self) -> str:
        return (f"进度: {self.success}/{self.total} ({self.success_rate:.1f}%), "
                f"失败: {self.failed}, 跳过: {self.skipped}, "
                f"重试: {self.retries}, 耗时: {self.elapsed_time:.1f}s")


class DataFetcher:
    """数据获取器 - 支持分批获取、自动重试、频率限制"""

    def __init__(
        self,
        data_source: str = "akshare",
        data_dir: Optional[Path] = None,
        timeout: int = DEFAULT_TIMEOUT,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_delay: int = DEFAULT_BATCH_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
    ):
        """
        初始化数据获取器
        
        Args:
            data_source: 数据源 ("akshare" 或 "tushare")
            data_dir: 数据存储目录
            timeout: 请求超时时间（秒）
            request_delay: 请求间隔（秒）
            batch_size: 每批获取的股票数量
            batch_delay: 每批之间的等待秒数
            max_retries: 最大重试次数
            base_delay: 基础退避时间（秒），用于指数退避
        """
        self.data_source = data_source
        self.data_dir = data_dir or Path("data/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.request_delay = request_delay
        self.batch_size = batch_size
        self.batch_delay = batch_delay
        self.max_retries = max_retries
        self.base_delay = base_delay
        
        # 初始化Tushare
        if data_source == "tushare":
            token = os.environ.get("TUSHARE_TOKEN", "")
            if token:
                ts.set_token(token)
                self.pro = ts.pro_api()
                logger.info("Tushare 初始化成功")
            else:
                logger.warning("未设置TUSHARE_TOKEN环境变量")

    def _wait_with_jitter(self, attempt: int) -> None:
        """指数退避 + 随机抖动"""
        delay = self.base_delay * (2 ** attempt) + random.uniform(0, 1)
        logger.debug(f"等待 {delay:.1f}s 后重试...")
        time.sleep(delay)

    def fetch_kline(
        self,
        code: str,
        start: str = "20190101",
        end: Optional[str] = None,
        adj: str = "qfq",
        max_retries: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        获取K线数据（带重试机制）
        
        Args:
            code: 股票代码 (6位数字)
            start: 开始日期
            end: 结束日期，默认今天
            adj: 复权类型 ("qfq" 前复权, "hfq" 后复权, "None" 不复权)
            max_retries: 最大重试次数（默认使用实例配置）
            
        Returns:
            DataFrame: 日线数据
        """
        if end is None:
            end = datetime.now().strftime("%Y%m%d")
        
        max_retries = max_retries if max_retries is not None else self.max_retries
        
        # 带重试的获取
        for attempt in range(max_retries + 1):
            try:
                if self.data_source == "tushare":
                    df = self._fetch_from_tushare(code, start, end, adj)
                else:
                    df = self._fetch_from_akshare(code, start, end, adj)
                
                if df is not None and not df.empty:
                    return df
                
                # 空结果，重试
                if attempt < max_retries:
                    logger.warning(
                        f"⚠️ [{self.data_source}] 返回空数据 | code={code} | "
                        f"attempt={attempt + 1}/{max_retries} | params: start={start} end={end} adj={adj}"
                    )
                    self._wait_with_jitter(attempt)
                    
            except Exception as e:
                error_type = self._classify_error(e)
                if attempt < max_retries:
                    logger.warning(
                        f"❌ [{self.data_source}] 获取失败 | code={code} | "
                        f"error_type={error_type} attempt={attempt + 1}/{max_retries} | "
                        f"exception={type(e).__name__}: {e}"
                    )
                    self._wait_with_jitter(attempt)
                else:
                    logger.error(
                        f"❌ [{self.data_source}] 获取失败，已达最大重试次数 | code={code} | "
                        f"error_type={error_type} | exception={type(e).__name__}: {e}"
                    )
        
        return pd.DataFrame()

    def _classify_error(self, error: Exception) -> str:
        """分类错误类型，便于排查"""
        error_msg = str(error).lower()
        
        if "timeout" in error_msg or "timed out" in error_msg:
            return "TIMEOUT"
        elif "connect" in error_msg and "refused" in error_msg:
            return "CONNECTION_REFUSED"
        elif "connect" in error_msg:
            return "CONNECTION_ERROR"
        elif "unauthorized" in error_msg or "401" in error_msg or "403" in error_msg:
            return "AUTH_ERROR"
        elif "quota" in error_msg or "limit" in error_msg or "频率" in error_msg:
            return "RATE_LIMIT"
        elif "token" in error_msg:
            return "TOKEN_ERROR"
        elif "permission" in error_msg:
            return "PERMISSION_DENIED"
        elif "empty" in error_msg or "无数据" in error_msg:
            return "NO_DATA"
        else:
            return "UNKNOWN"

    def _fetch_from_tushare(
        self,
        code: str,
        start: str,
        end: str,
        adj: str,
    ) -> pd.DataFrame:
        """从Tushare获取数据"""
        ts_code = self._to_ts_code(code)
        
        # 检查是否已初始化pro
        if not hasattr(self, "pro") or self.pro is None:
            logger.error(
                f"❌ [{self.data_source}] Tushare未初始化，请设置TUSHARE_TOKEN环境变量 | "
                f"code={code} ts_code={ts_code} start={start} end={end}"
            )
            return pd.DataFrame()
        
        try:
            df = self.pro.pro_bar(
                ts_code=ts_code,
                adj=adj,
                start_date=start,
                end_date=end,
                freq="D",
            )
        except Exception as e:
            error_type = self._classify_error(e)
            logger.error(
                f"❌ [{self.data_source}] Tushare API调用失败 | "
                f"error_type={error_type} code={code} ts_code={ts_code} | "
                f"start={start} end={end} adj={adj} | exception={type(e).__name__}: {e}"
            )
            raise  # 重新抛出，让上层处理重试
        
        if df is None or df.empty:
            logger.warning(
                f"⚠️ [{self.data_source}] 返回空数据 | code={code} ts_code={ts_code} | "
                f"start={start} end={end}"
            )
            return pd.DataFrame()
        
        # 列名兼容处理
        df = df.rename(columns={
            "trade_date": "date",
            "vol": "volume",
        })[["date", "open", "close", "high", "low", "volume"]].copy()
        
        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        logger.debug(f"✅ [{self.data_source}] 成功获取 | code={code} | rows={len(df)}")
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
            logger.error(
                f"❌ [{self.data_source}] akshare未安装，请运行: pip install akshare"
            )
            return pd.DataFrame()
        
        try:
            # 获取A股数据（akshare内部会根据code自动区分沪/深/北交所）
            logger.debug(f"📡 [{self.data_source}] 请求数据 | code={code} start={start} end={end} adj={adj}")
            df = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust=adj)
            
            if df is None or df.empty:
                logger.warning(
                    f"⚠️ [{self.data_source}] 返回空数据 | code={code} | "
                    f"start={start} end={end} adj={adj}"
                )
                return pd.DataFrame()
            
            # 列名映射（新版本akshare可能返回不同列名）
            column_mapping = {
                "日期": "date",
                "日期 ": "date",  # 带空格的情况
                "开盘": "open",
                "开盘价": "open",
                "收盘": "close",
                "收盘价": "close",
                "最高": "high",
                "最高价": "high",
                "最低": "low",
                "最低价": "low",
                "成交量": "volume",
                "成交额": "amount",
            }
            
            df = df.rename(columns=column_mapping)
            
            # 选取需要的列（如果amount不存在则忽略）
            needed_cols = ["date", "open", "close", "high", "low", "volume"]
            available_cols = [c for c in needed_cols if c in df.columns]
            missing_cols = set(needed_cols) - set(available_cols)
            
            if missing_cols:
                logger.warning(
                    f"⚠️ [{self.data_source}] 列名缺失 | code={code} | "
                    f"缺失列: {missing_cols} | 可用列: {df.columns.tolist()}"
                )
            
            df = df[available_cols].copy()
            
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            for col in ["open", "close", "high", "low", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            logger.debug(f"✅ [{self.data_source}] 成功获取 | code={code} | rows={len(df)}")
            return df.sort_values("date").reset_index(drop=True)
            
        except Exception as e:
            error_type = self._classify_error(e)
            logger.error(
                f"❌ [{self.data_source}] AkShare API调用失败 | "
                f"error_type={error_type} code={code} | "
                f"start={start} end={end} adj={adj} | "
                f"exception={type(e).__name__}: {e}"
            )
            raise  # 重新抛出，让上层处理重试

    def _to_ts_code(self, code: str) -> str:
        """转换为tushare格式
        
        股票代码规则：
        - 沪市主板: 600, 601, 603, 605
        - 科创板: 688
        - 深市主板: 000, 001
        - 中小板: 002
        - 创业板: 300, 301
        - 北交所: 4, 8, 9 开头
        """
        code = str(code).zfill(6)
        if code.startswith(("60", "68")):
            return f"{code}.SH"
        elif code.startswith(("4", "8", "9")):
            return f"{code}.BJ"  # 北交所
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
            logger.info(f"📡 [{self.data_source}] 获取股票列表...")
            df = self.pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol")
        except Exception as e:
            error_type = self._classify_error(e)
            logger.error(
                f"❌ [{self.data_source}] 获取股票列表失败 | "
                f"error_type={error_type} exception={type(e).__name__}: {e}"
            )
            return []
        
        # 列名兼容
        if "symbol" in df.columns:
            codes = df["symbol"].tolist()
        elif "ts_code" in df.columns:
            # 从ts_code提取数字代码
            codes = [str(x).split(".")[0] for x in df["ts_code"].tolist()]
        else:
            logger.error(
                f"❌ [{self.data_source}] 无法识别股票代码列 | "
                f"可用列: {df.columns.tolist()}"
            )
            return []
        
        # 排除板块
        if "gem" in exclude_boards:
            codes = [c for c in codes if not c.startswith(("300", "301"))]
        if "star" in exclude_boards:
            codes = [c for c in codes if not c.startswith("688")]
        if "bj" in exclude_boards:
            codes = [c for c in codes if not c.startswith(("4", "8"))]
        
        logger.info(f"✅ [{self.data_source}] 获取股票列表成功 | 总数: {len(codes)}")
        return codes

    def _fetch_stock_list_akshare(self, exclude_boards: set) -> List[str]:
        """从AkShare获取股票列表"""
        try:
            import akshare as ak
            logger.info(f"📡 [{self.data_source}] 获取股票列表...")
            df = ak.stock_info_a_code_name()
            
            # 列名兼容处理
            if "code" in df.columns:
                codes = df["code"].tolist()
            elif "股票代码" in df.columns:
                codes = df["股票代码"].tolist()
            else:
                logger.error(
                    f"❌ [{self.data_source}] 无法识别股票代码列 | "
                    f"可用列: {df.columns.tolist()}"
                )
                return []
            
            # 排除板块
            if "gem" in exclude_boards:
                codes = [c for c in codes if not c.startswith(("300", "301"))]
            if "star" in exclude_boards:
                codes = [c for c in codes if not c.startswith("688")]
            if "bj" in exclude_boards:
                codes = [c for c in codes if not c.startswith(("4", "8"))]
            
            logger.info(f"✅ [{self.data_source}] 获取股票列表成功 | 总数: {len(codes)}")
            return codes
        except Exception as e:
            error_type = self._classify_error(e)
            logger.error(
                f"❌ [{self.data_source}] 获取股票列表失败 | "
                f"error_type={error_type} exception={type(e).__name__}: {e}"
            )
            return []

    def fetch_all(
        self,
        codes: Optional[List[str]] = None,
        start: str = "20190101",
        end: Optional[str] = None,
        skip_existing: bool = True,
        progress_callback=None,
    ) -> Tuple[Dict[str, pd.DataFrame], FetchStats]:
        """
        批量获取多只股票数据（分批获取 + 频率限制）
        
        Args:
            codes: 股票代码列表，None时获取全部
            start: 开始日期
            end: 结束日期
            skip_existing: 是否跳过已存在的CSV文件
            progress_callback: 进度回调函数 (stats: FetchStats) -> None
            
        Returns:
            (股票代码到DataFrame的映射, 统计信息)
        """
        if codes is None:
            codes = self.fetch_stock_list()
        
        if end is None:
            end = datetime.now().strftime("%Y%m%d")
        
        # 统计信息
        stats = FetchStats()
        stats.total = len(codes)
        stats.start_time = time.time()
        
        results: Dict[str, pd.DataFrame] = {}
        
        # 分批处理
        total_batches = (len(codes) + self.batch_size - 1) // self.batch_size
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * self.batch_size
            batch_end = min(batch_start + self.batch_size, len(codes))
            batch_codes = codes[batch_start:batch_end]
            
            logger.info(f"处理第 {batch_idx + 1}/{total_batches} 批 ({batch_start + 1}-{batch_end}/{len(codes)})")
            
            # 逐个获取
            for code in tqdm(batch_codes, desc=f"批次{batch_idx + 1}"):
                # 检查是否已存在
                csv_path = self.data_dir / f"{code}.csv"
                if skip_existing and csv_path.exists():
                    logger.debug(f"{code} 已存在，跳过")
                    stats.skipped += 1
                    # 加载已存在的文件
                    df = self.load_csv(code)
                    if not df.empty:
                        results[code] = df
                    continue
                
                # 获取数据
                try:
                    df = self.fetch_kline(code, start, end)
                    if not df.empty:
                        # 保存到文件
                        df.to_csv(csv_path, index=False)
                        results[code] = df
                        stats.success += 1
                    else:
                        stats.failed += 1
                except Exception as e:
                    logger.error(f"{code}下载失败: {e}")
                    stats.failed += 1
                
                # 请求间隔延迟
                if self.request_delay > 0:
                    time.sleep(self.request_delay)
                
                # 更新进度
                if progress_callback:
                    progress_callback(stats)
            
            # 批间延迟（除了最后一批）
            if batch_idx < total_batches - 1 and self.batch_delay > 0:
                logger.info(f"批次间隔等待 {self.batch_delay}s...")
                time.sleep(self.batch_delay)
        
        stats.end_time = time.time()
        logger.info(f"完成: {stats}")
        
        return results, stats

    def fetch_incremental(
        self,
        codes: Optional[List[str]] = None,
        days: int = 30,
    ) -> Tuple[Dict[str, pd.DataFrame], FetchStats]:
        """
        增量获取数据（仅获取最近N天，用于更新）
        
        Args:
            codes: 股票代码列表，None时取data_dir下已有的
            days: 获取最近N天数据
            
        Returns:
            (股票代码到DataFrame的映射, 统计信息)
        """
        if codes is None:
            # 读取已有文件
            codes = [f.stem for f in self.data_dir.glob("*.csv")]
            if not codes:
                logger.warning("没有已有数据，请先运行 fetch_all")
                return {}, FetchStats()
        
        # 计算开始日期
        from datetime import timedelta
        start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        results = {}
        stats = FetchStats()
        stats.total = len(codes)
        stats.start_time = time.time()
        
        for code in tqdm(codes, desc="增量更新"):
            csv_path = self.data_dir / f"{code}.csv"
            
            # 获取新数据
            try:
                df_new = self.fetch_kline(code, start)
                if df_new.empty:
                    stats.failed += 1
                    continue
                
                if csv_path.exists():
                    # 合并数据
                    df_old = self.load_csv(code)
                    if not df_old.empty:
                        # 合并并去重
                        df = pd.concat([df_old, df_new]).drop_duplicates(subset=["date"])
                        df = df.sort_values("date").reset_index(drop=True)
                    else:
                        df = df_new
                else:
                    df = df_new
                
                df.to_csv(csv_path, index=False)
                results[code] = df
                stats.success += 1
                
            except Exception as e:
                logger.error(f"{code}更新失败: {e}")
                stats.failed += 1
            
            if self.request_delay > 0:
                time.sleep(self.request_delay)
            
            if stats.total % 100 == 0 and self.batch_delay > 0:
                # 每100条后稍作休息
                time.sleep(5)
        
        stats.end_time = time.time()
        return results, stats

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