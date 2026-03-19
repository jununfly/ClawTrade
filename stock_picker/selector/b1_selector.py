# -*- coding: utf-8 -*-
"""
B1策略选股器

B1策略由以下四个Filter组成：
1. KDJQuantileFilter - J值低位（<-5或历史10%分位）
2. ZXConditionFilter - 知行线条件（close>zxdkx 且 zxdq>zxdkx）
3. WeeklyMABullFilter - 周线多头排列
4. MaxVolNotBearishFilter - 近20日成交量最大日非阴线
"""
from dataclasses import dataclass
try:
    from typing import Protocol, Tuple
except ImportError:
    from typing_extensions import Protocol
    Tuple = tuple
from typing import Optional, Sequence
import numpy as np
import pandas as pd
from numba import njit

from .base import BaseSelector


# =============================================================================
# Numba 加速核心函数
# =============================================================================

@njit(cache=True)
def _kdj_core(rsv: np.ndarray) -> tuple:
    """KDJ递推计算"""
    n = len(rsv)
    K = np.empty(n, dtype=np.float64)
    D = np.empty(n, dtype=np.float64)
    K[0] = D[0] = 50.0
    for i in range(1, n):
        K[i] = 2.0 / 3.0 * K[i - 1] + 1.0 / 3.0 * rsv[i]
        D[i] = 2.0 / 3.0 * D[i - 1] + 1.0 / 3.0 * K[i]
    J = 3.0 * K - 2.0 * D
    return K, D, J


# =============================================================================
# 指标计算辅助函数
# =============================================================================

def compute_kdj(df: pd.DataFrame, n: int = 9) -> pd.DataFrame:
    """计算KDJ指标"""
    if df.empty:
        return df.assign(K=np.nan, D=np.nan, J=np.nan)
    
    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_n = df["high"].rolling(window=n, min_periods=1).max()
    rsv = ((df["close"] - low_n) / (high_n - low_n + 1e-9) * 100).to_numpy(dtype=np.float64)
    
    K, D, J = _kdj_core(rsv)
    return df.assign(K=K, D=D, J=J)


def compute_zx_lines(
    df: pd.DataFrame,
    m1: int = 14, m2: int = 28, m3: int = 57, m4: int = 114,
    zxdq_span: int = 10,
):
    """计算知行线（zxdq和zxdkx）"""
    close = df["close"].astype(float)
    zxdq = close.ewm(span=zxdq_span, adjust=False).mean().ewm(span=zxdq_span, adjust=False).mean()
    zxdkx = (
        close.rolling(m1, min_periods=m1).mean()
        + close.rolling(m2, min_periods=m2).mean()
        + close.rolling(m3, min_periods=m3).mean()
        + close.rolling(m4, min_periods=m4).mean()
    ) / 4.0
    return zxdq, zxdkx


def compute_weekly_close(df: pd.DataFrame) -> pd.Series:
    """日线转周线收盘价"""
    close = df["close"].astype(float) if isinstance(df.index, pd.DatetimeIndex) else df.set_index("date")["close"].astype(float)
    idx = close.index
    year_week = idx.isocalendar().year.astype(str) + "-" + idx.isocalendar().week.astype(str).str.zfill(2)
    weekly = close.groupby(year_week).last()
    last_date_per_week = close.groupby(year_week).apply(lambda s: s.index[-1])
    weekly.index = pd.DatetimeIndex(last_date_per_week.values)
    return weekly.dropna()


def compute_weekly_ma_bull(
    df: pd.DataFrame,
    ma_periods=(20, 60, 120),
):
    """计算周线均线多头排列"""
    weekly_close = compute_weekly_close(df)
    s, m, l = ma_periods
    ma_s = weekly_close.rolling(s, min_periods=s).mean()
    ma_m = weekly_close.rolling(m, min_periods=m).mean()
    ma_l = weekly_close.rolling(l, min_periods=l).mean()
    bull = (ma_s > ma_m) & (ma_m > ma_l)
    
    daily_index = df.index if isinstance(df.index, pd.DatetimeIndex) else pd.DatetimeIndex(df["date"])
    bull_daily = bull.astype(float).reindex(daily_index).ffill().fillna(0.0).astype(bool)
    return bull_daily


# =============================================================================
# Filter 定义
# =============================================================================

class StockFilter(Protocol):
    """单股票过滤器"""
    def __call__(self, hist: pd.DataFrame) -> bool: ...


@dataclass(frozen=True)
class KDJQuantileFilter:
    """KDJ分位过滤"""
    j_threshold: float = -5.0
    j_q_threshold: float = 0.10
    kdj_n: int = 9

    def _j_series(self, hist: pd.DataFrame) -> pd.Series:
        if "J" in hist.columns:
            return hist["J"].astype(float)
        return compute_kdj(hist, n=self.kdj_n)["J"].astype(float)

    def __call__(self, hist: pd.DataFrame) -> bool:
        j = self._j_series(hist).dropna()
        if j.empty:
            return False
        j_today = float(j.iloc[-1])
        j_q = float(j.quantile(self.j_q_threshold))
        return (j_today < self.j_threshold) or (j_today <= j_q)

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        J = self._j_series(df)
        j_vals = J.to_numpy(dtype=float)
        j_q_exp = J.expanding(min_periods=1).quantile(self.j_q_threshold).to_numpy(dtype=float)
        return (j_vals < self.j_threshold) | (j_vals <= j_q_exp)


@dataclass(frozen=True)
class ZXConditionFilter:
    """知行线条件过滤"""
    zx_m1: int = 14
    zx_m2: int = 28
    zx_m3: int = 57
    zx_m4: int = 114
    zxdq_span: int = 10
    require_close_gt_long: bool = True
    require_short_gt_long: bool = True

    def __call__(self, hist: pd.DataFrame) -> bool:
        if hist.empty:
            return False
        c = float(hist["close"].iloc[-1])
        if "zxdq" in hist.columns and "zxdkx" in hist.columns:
            s = float(hist["zxdq"].iloc[-1])
            lv = hist["zxdkx"].iloc[-1]
            l = float(lv) if pd.notna(lv) else float("nan")
        else:
            zxdq, zxdkx = compute_zx_lines(hist, self.zx_m1, self.zx_m2, self.zx_m3, self.zx_m4, zxdq_span=self.zxdq_span)
            s = float(zxdq.iloc[-1])
            l = float(zxdkx.iloc[-1]) if pd.notna(zxdkx.iloc[-1]) else float("nan")
        
        if not (np.isfinite(s) and np.isfinite(l)):
            return False
        if self.require_close_gt_long and not (c > l):
            return False
        if self.require_short_gt_long and not (s > l):
            return False
        return True

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        if "zxdq" in df.columns and "zxdkx" in df.columns:
            zxdq_v = df["zxdq"].to_numpy(dtype=float)
            zxdkx_v = df["zxdkx"].to_numpy(dtype=float)
        else:
            zs, zk = compute_zx_lines(df, self.zx_m1, self.zx_m2, self.zx_m3, self.zx_m4, zxdq_span=self.zxdq_span)
            zxdq_v = zs.to_numpy(dtype=float)
            zxdkx_v = zk.to_numpy(dtype=float)
        close_v = df["close"].to_numpy(dtype=float)
        mask = np.isfinite(zxdq_v) & np.isfinite(zxdkx_v)
        if self.require_close_gt_long:
            mask &= close_v > zxdkx_v
        if self.require_short_gt_long:
            mask &= zxdq_v > zxdkx_v
        return mask


@dataclass(frozen=True)
class WeeklyMABullFilter:
    """周线均线多头排列过滤"""
    wma_short: int = 20
    wma_mid: int = 60
    wma_long: int = 120

    def __call__(self, hist: pd.DataFrame) -> bool:
        if "wma_bull" in hist.columns:
            return bool(hist["wma_bull"].iloc[-1])
        wc = compute_weekly_close(hist)
        if len(wc) < self.wma_long:
            return False
        ma_s = wc.rolling(self.wma_short, min_periods=self.wma_short).mean()
        ma_m = wc.rolling(self.wma_mid, min_periods=self.wma_mid).mean()
        ma_l = wc.rolling(self.wma_long, min_periods=self.wma_long).mean()
        sv, mv, lv = float(ma_s.iloc[-1]), float(ma_m.iloc[-1]), float(ma_l.iloc[-1])
        return bool(np.isfinite(sv) and np.isfinite(mv) and np.isfinite(lv) and sv > mv > lv)

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        if "wma_bull" in df.columns:
            return df["wma_bull"].to_numpy(dtype=bool)
        return compute_weekly_ma_bull(df, ma_periods=(self.wma_short, self.wma_mid, self.wma_long)).to_numpy(dtype=bool)


@dataclass(frozen=True)
class MaxVolNotBearishFilter:
    """成交量最大日非阴线过滤 - 近20日成交量最大日非阴线"""
    n: int = 20

    def __call__(self, hist: pd.DataFrame) -> bool:
        window = hist.tail(self.n)
        if window.empty or "volume" not in window.columns:
            return False
        idx_max_vol = window["volume"].idxmax()
        row = window.loc[idx_max_vol]
        return float(row["close"]) >= float(row["open"])

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        """向量化版本：计算每日的成交量最大日是否非阴线"""
        n = self.n
        length = len(df)
        if length < n:
            return np.zeros(length, dtype=bool)
        
        vol = df["volume"].to_numpy(dtype=float)
        close = df["close"].to_numpy(dtype=float)
        open_ = df["open"].to_numpy(dtype=float)
        
        result = np.zeros(length, dtype=bool)
        for i in range(n - 1, length):
            start = i - n + 1
            window_vol = vol[start:i + 1]
            idx_max = np.argmax(window_vol)
            actual_idx = start + idx_max
            
            if close[actual_idx] >= open_[actual_idx]:
                result[i] = True
        
        return result


def _apply_vec_filters(df: pd.DataFrame, filters: list) -> np.ndarray:
    """对所有Filter取交集"""
    mask = np.ones(len(df), dtype=bool)
    for f in filters:
        mask &= f.vec_mask(df)
    return mask


# =============================================================================
# B1Selector 实现
# =============================================================================

class B1Selector(BaseSelector):
    """B1策略选股器"""

    def __init__(
        self,
        j_threshold: float = 15.0,
        j_q_threshold: float = 0.10,
        kdj_n: int = 9,
        zx_m1: int = 14,
        zx_m2: int = 28,
        zx_m3: int = 57,
        zx_m4: int = 114,
        zxdq_span: int = 10,
        require_close_gt_long: bool = True,
        require_short_gt_long: bool = True,
        wma_short: int = 10,
        wma_mid: int = 20,
        wma_long: int = 30,
        max_vol_lookback: int = 20,
        **kwargs,
    ):
        super().__init__(name="B1Selector", **kwargs)
        
        self._kdj_filter = KDJQuantileFilter(
            j_threshold=j_threshold,
            j_q_threshold=j_q_threshold,
            kdj_n=kdj_n,
        )
        self._zx_filter = ZXConditionFilter(
            zx_m1=zx_m1,
            zx_m2=zx_m2,
            zx_m3=zx_m3,
            zx_m4=zx_m4,
            zxdq_span=zxdq_span,
            require_close_gt_long=require_close_gt_long,
            require_short_gt_long=require_short_gt_long,
        )
        self._wma_filter = WeeklyMABullFilter(
            wma_short=wma_short,
            wma_mid=wma_mid,
            wma_long=wma_long,
        )
        self._maxvol_filter = MaxVolNotBearishFilter(n=max_vol_lookback)
        
        # 保存参数
        self.kdj_n = kdj_n
        self.zx_m1, self.zx_m2, self.zx_m3, self.zx_m4 = zx_m1, zx_m2, zx_m3, zx_m4
        self.zxdq_span = zxdq_span
        self.wma_short, self.wma_mid, self.wma_long = wma_short, wma_mid, wma_long
        self.max_vol_lookback = max_vol_lookback

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """预计算知行线、KDJ、周线多头排列、向量化pick mask"""
        df = df.copy()
        
        # 知行线
        zs, zk = compute_zx_lines(
            df, self.zx_m1, self.zx_m2, self.zx_m3, self.zx_m4,
            zxdq_span=self.zxdq_span,
        )
        df["zxdq"] = zs
        df["zxdkx"] = zk
        
        # KDJ
        kdj = compute_kdj(df, n=self.kdj_n)
        df["K"] = kdj["K"]
        df["D"] = kdj["D"]
        df["J"] = kdj["J"]
        
        # 周线多头排列
        df["wma_bull"] = compute_weekly_ma_bull(
            df, ma_periods=(self.wma_short, self.wma_mid, self.wma_long)
        ).to_numpy()
        
        # 向量化pick - 包含4个Filter
        _b1_vec_filters = [
            self._kdj_filter,
            self._zx_filter,
            self._wma_filter,
            self._maxvol_filter,
        ]
        df["_vec_pick"] = _apply_vec_filters(df, _b1_vec_filters)
        
        return df

    def passes_df_on_date(self, df: pd.DataFrame, date: pd.Timestamp) -> bool:
        """单日判断"""
        hist = self._get_hist(df, date)
        if hist is None or hist.empty:
            return False
        if len(hist) < self.min_bars:
            return False
        
        # 逐个检查filter（4个核心Filter）
        if not self._kdj_filter(hist):
            return False
        if not self._zx_filter(hist):
            return False
        if not self._wma_filter(hist):
            return False
        if not self._maxvol_filter(hist):
            return False
        
        return True