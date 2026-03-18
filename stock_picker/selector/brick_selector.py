# -*- coding: utf-8 -*-
"""
砖型图策略选股器

砖型图选股器由以下模块组成：
1. BrickPatternFilter - 砖型图形态（红柱/绿柱 + 涨幅 + 连续绿柱数）
2. ZXDQRatioFilter - close < zxdq × ratio
3. ZXConditionFilter - zxdq > zxdkx
4. WeeklyMABullFilter - 周线多头排列
"""
from dataclasses import dataclass, field
from typing import Optional
import numpy as np
import pandas as pd
from numba import njit

from .base import BaseSelector
from .b1_selector import compute_zx_lines, compute_weekly_ma_bull, compute_weekly_close, _apply_vec_filters


# =============================================================================
# Numba 加速核心函数
# =============================================================================

@njit(cache=True)
def _green_run(brick_vals: np.ndarray) -> np.ndarray:
    """连续绿柱计数"""
    n = len(brick_vals)
    out = np.zeros(n, dtype=np.int32)
    for i in range(1, n):
        if brick_vals[i - 1] < 0.0:
            out[i] = out[i - 1] + 1
        else:
            out[i] = 0
    return out


@njit(cache=True)
def _compute_brick_numba(
    high: np.ndarray, low: np.ndarray, close: np.ndarray,
    n: int, m1: int, m2: int, m3: int,
    t: float, shift1: float, shift2: float,
    sma_w1: int, sma_w2: int, sma_w3: int,
) -> np.ndarray:
    """砖型图核心计算"""
    length = len(close)
    hhv = np.empty(length, dtype=np.float64)
    llv = np.empty(length, dtype=np.float64)
    
    for i in range(length):
        start = max(0, i - n + 1)
        h_max = high[start]
        l_min = low[start]
        for j in range(start + 1, i + 1):
            if high[j] > h_max:
                h_max = high[j]
            if low[j] < l_min:
                l_min = low[j]
        hhv[i] = h_max
        llv[i] = l_min

    a1 = sma_w1 / m1
    b1 = 1.0 - a1
    var2a = np.empty(length, dtype=np.float64)
    for i in range(length):
        rng = hhv[i] - llv[i]
        if rng == 0.0:
            rng = 0.01
        v1 = (hhv[i] - close[i]) / rng * 100.0 - shift1
        var2a[i] = (v1 + shift2) if i == 0 else (a1 * v1 + b1 * (var2a[i - 1] - shift2) + shift2)

    a2 = sma_w2 / m2
    b2 = 1.0 - a2
    a3 = sma_w3 / m3
    b3 = 1.0 - a3
    var4a = np.empty(length, dtype=np.float64)
    var5a = np.empty(length, dtype=np.float64)
    for i in range(length):
        rng = hhv[i] - llv[i]
        if rng == 0.0:
            rng = 0.01
        v3 = (close[i] - llv[i]) / rng * 100.0
        if i == 0:
            var4a[i] = v3
            var5a[i] = v3 + shift2
        else:
            var4a[i] = a2 * v3 + b2 * var4a[i - 1]
            var5a[i] = a3 * var4a[i] + b3 * (var5a[i - 1] - shift2) + shift2

    raw = np.empty(length, dtype=np.float64)
    for i in range(length):
        diff = var5a[i] - var2a[i]
        raw[i] = diff - t if diff > t else 0.0

    brick = np.empty(length, dtype=np.float64)
    brick[0] = 0.0
    for i in range(1, length):
        brick[i] = raw[i] - raw[i - 1]
    return brick


# =============================================================================
# 砖型图计算
# =============================================================================

def compute_brick_chart(
    df: pd.DataFrame,
    n: int = 4, m1: int = 4, m2: int = 6, m3: int = 6,
    t: float = 4.0, shift1: float = 90.0, shift2: float = 100.0,
    sma_w1: int = 1, sma_w2: int = 1, sma_w3: int = 1,
) -> pd.Series:
    """计算砖型图"""
    arr = _compute_brick_numba(
        df["high"].to_numpy(dtype=np.float64),
        df["low"].to_numpy(dtype=np.float64),
        df["close"].to_numpy(dtype=np.float64),
        n, m1, m2, m3, float(t), float(shift1), float(shift2),
        sma_w1, sma_w2, sma_w3,
    )
    return pd.Series(arr, index=df.index, name="brick")


# =============================================================================
# Filter 定义
# =============================================================================

@dataclass(frozen=True)
class BrickComputeParams:
    """砖型图计算参数"""
    n: int = 4
    m1: int = 4
    m2: int = 6
    m3: int = 6
    t: float = 4.0
    shift1: float = 90.0
    shift2: float = 100.0
    sma_w1: int = 1
    sma_w2: int = 1
    sma_w3: int = 1

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return compute_brick_chart(
            df, n=self.n, m1=self.m1, m2=self.m2, m3=self.m3,
            t=self.t, shift1=self.shift1, shift2=self.shift2,
            sma_w1=self.sma_w1, sma_w2=self.sma_w2, sma_w3=self.sma_w3,
        )

    def compute_arr(self, df: pd.DataFrame) -> np.ndarray:
        return _compute_brick_numba(
            df["high"].to_numpy(dtype=np.float64),
            df["low"].to_numpy(dtype=np.float64),
            df["close"].to_numpy(dtype=np.float64),
            self.n, self.m1, self.m2, self.m3,
            float(self.t), float(self.shift1), float(self.shift2),
            self.sma_w1, self.sma_w2, self.sma_w3,
        )


@dataclass(frozen=True)
class BrickPatternFilter:
    """砖型图形态过滤"""
    daily_return_threshold: float = 0.05
    brick_growth_ratio: float = 1.0
    min_prior_green_bars: int = 1
    brick_params: BrickComputeParams = field(default_factory=BrickComputeParams)

    def _brick_arr(self, hist: pd.DataFrame) -> np.ndarray:
        if "brick" in hist.columns:
            return hist["brick"].to_numpy(dtype=float)
        return self.brick_params.compute_arr(hist)

    def __call__(self, hist: pd.DataFrame) -> bool:
        min_len = max(3, 1 + self.min_prior_green_bars + 1)
        if len(hist) < min_len:
            return False
        
        close = hist["close"].to_numpy(dtype=float)
        c0, c1 = close[-1], close[-2]
        if c1 <= 0 or (c0 / c1 - 1.0) >= self.daily_return_threshold:
            return False
        
        vals = self._brick_arr(hist)
        b0, b1 = vals[-1], vals[-2]
        if not (b0 > 0 and b1 < 0):
            return False
        if b0 < self.brick_growth_ratio * abs(b1):
            return False
        
        # 连续绿柱
        green_count = 1
        i = len(vals) - 3
        while green_count < self.min_prior_green_bars and i > 0:
            if vals[i] < 0:
                green_count += 1
                i -= 1
            else:
                break
        return green_count >= self.min_prior_green_bars

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        bv = self._brick_arr(df)
        cv = df["close"].to_numpy(dtype=float)
        
        bp = np.empty_like(bv)
        bp[0] = np.nan
        bp[1:] = bv[:-1]
        abp = np.abs(bp)
        
        cond_ret = (cv / np.roll(cv, 1) - 1.0)[1:] < self.daily_return_threshold
        cond_ret = np.concatenate([[False], cond_ret])
        
        cond_red = bv > 0
        cond_green = bp < 0
        cond_growth = bv >= self.brick_growth_ratio * abp
        
        if self.min_prior_green_bars <= 1:
            cond_gc = cond_green
        else:
            gr = _green_run(bv)
            cond_gc = cond_green & (gr >= self.min_prior_green_bars)
        
        return cond_ret & cond_red & cond_gc & cond_growth

    def brick_growth_arr(self, df: pd.DataFrame) -> np.ndarray:
        bv = self._brick_arr(df)
        bp = np.empty_like(bv)
        bp[0] = np.nan
        bp[1:] = bv[:-1]
        abp = np.abs(bp)
        safe = np.where(abp > 0, abp, 1.0)
        return np.where(abp > 0, bv / safe, bv)


@dataclass(frozen=True)
class ZXDQRatioFilter:
    """砖型图选股条件6：close < zxdq × zxdq_ratio"""
    zxdq_ratio: float = 1.0
    zxdq_span: int = 10
    zxdkx_m1: int = 14
    zxdkx_m2: int = 28
    zxdkx_m3: int = 57
    zxdkx_m4: int = 114

    def _zxdq_arr(self, df: pd.DataFrame) -> np.ndarray:
        if "zxdq" in df.columns:
            return df["zxdq"].to_numpy(dtype=float)
        zs, _ = compute_zx_lines(
            df, self.zxdkx_m1, self.zxdkx_m2, self.zxdkx_m3, self.zxdkx_m4,
            zxdq_span=self.zxdq_span,
        )
        return zs.to_numpy(dtype=float)

    def __call__(self, hist: pd.DataFrame) -> bool:
        zxdq_arr = self._zxdq_arr(hist)
        zv = float(zxdq_arr[-1])
        if not np.isfinite(zv) or zv <= 0:
            return False
        return float(hist["close"].iloc[-1]) < zv * self.zxdq_ratio

    def vec_mask(self, df: pd.DataFrame) -> np.ndarray:
        zxdq_v = self._zxdq_arr(df)
        close_v = df["close"].to_numpy(dtype=float)
        return (
            np.isfinite(zxdq_v)
            & (zxdq_v > 0)
            & (close_v < zxdq_v * self.zxdq_ratio)
        )


# =============================================================================
# BrickChartSelector 实现
# =============================================================================

class BrickChartSelector(BaseSelector):
    """砖型图策略选股器"""

    def __init__(
        self,
        # 砖型图形态参数
        daily_return_threshold: float = 0.05,
        brick_growth_ratio: float = 1.0,
        min_prior_green_bars: int = 1,
        # 砖型图计算参数
        n: int = 4, m1: int = 4, m2: int = 6, m3: int = 6,
        t: float = 4.0, shift1: float = 90.0, shift2: float = 100.0,
        sma_w1: int = 1, sma_w2: int = 1, sma_w3: int = 1,
        # 知行线参数
        zxdq_span: int = 10,
        zxdkx_m1: int = 14, zxdkx_m2: int = 28,
        zxdkx_m3: int = 57, zxdkx_m4: int = 114,
        # 条件开关
        zxdq_ratio: Optional[float] = 1.0,
        require_zxdq_gt_zxdkx: bool = True,
        require_weekly_ma_bull: bool = True,
        # 周线参数
        wma_short: int = 20, wma_mid: int = 60, wma_long: int = 120,
        **kwargs,
    ):
        super().__init__(name="BrickChartSelector", **kwargs)
        
        self._bp = BrickComputeParams(
            n=n, m1=m1, m2=m2, m3=m3,
            t=t, shift1=shift1, shift2=shift2,
            sma_w1=sma_w1, sma_w2=sma_w2, sma_w3=sma_w3,
        )
        self._pattern_filter = BrickPatternFilter(
            daily_return_threshold=daily_return_threshold,
            brick_growth_ratio=brick_growth_ratio,
            min_prior_green_bars=min_prior_green_bars,
            brick_params=self._bp,
        )
        
        self._zxdq_ratio_filter = None
        if zxdq_ratio is not None:
            self._zxdq_ratio_filter = ZXDQRatioFilter(
                zxdq_ratio=zxdq_ratio,
                zxdq_span=zxdq_span,
                zxdkx_m1=zxdkx_m1,
                zxdkx_m2=zxdkx_m2,
                zxdkx_m3=zxdkx_m3,
                zxdkx_m4=zxdkx_m4,
            )
        
        self._zxdq_gt_filter = None
        if require_zxdq_gt_zxdkx:
            from .b1_selector import ZXConditionFilter
            self._zxdq_gt_filter = ZXConditionFilter(
                zx_m1=zxdkx_m1, zx_m2=zxdkx_m2,
                zx_m3=zxdkx_m3, zx_m4=zxdkx_m4,
                zxdq_span=zxdq_span,
                require_close_gt_long=False,
                require_short_gt_long=True,
            )
        
        self._wma_filter = None
        if require_weekly_ma_bull:
            from .b1_selector import WeeklyMABullFilter
            self._wma_filter = WeeklyMABullFilter(
                wma_short=wma_short,
                wma_mid=wma_mid,
                wma_long=wma_long,
            )
        
        # 保存参数
        self.zxdq_span = zxdq_span
        self.zxdkx_m1, self.zxdkx_m2 = zxdkx_m1, zxdkx_m2
        self.zxdkx_m3, self.zxdkx_m4 = zxdkx_m3, zxdkx_m4
        self.wma_short, self.wma_mid, self.wma_long = wma_short, wma_mid, wma_long

    def _precompute_zx_wma(self, df: pd.DataFrame) -> None:
        """预计算知行线和周线"""
        zs, zk = compute_zx_lines(
            df, self.zxdkx_m1, self.zxdkx_m2, self.zxdkx_m3, self.zxdkx_m4,
            zxdq_span=self.zxdq_span,
        )
        df["zxdq"] = zs
        df["zxdkx"] = zk
        
        if self._wma_filter is not None:
            df["wma_bull"] = compute_weekly_ma_bull(
                df, ma_periods=(self.wma_short, self.wma_mid, self.wma_long)
            ).to_numpy()

    def _precompute_brick(self, df: pd.DataFrame) -> None:
        """预计算砖型图"""
        bv = self._bp.compute_arr(df)
        bp_ = np.empty_like(bv)
        bp_[0] = np.nan
        bp_[1:] = bv[:-1]
        abp = np.abs(bp_)
        safe = np.where(abp > 0, abp, 1.0)
        
        df["brick"] = bv
        df["brick_growth"] = np.where(abp > 0, bv / safe, bv)

    def _compute_vec_pick(self, df: pd.DataFrame) -> np.ndarray:
        fs = [self._pattern_filter]
        if self._zxdq_ratio_filter is not None:
            fs.append(self._zxdq_ratio_filter)
        if self._zxdq_gt_filter is not None:
            fs.append(self._zxdq_gt_filter)
        if self._wma_filter is not None:
            fs.append(self._wma_filter)
        return _apply_vec_filters(df, fs)

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """完整预计算"""
        df = df.copy()
        self._precompute_zx_wma(df)
        self._precompute_brick(df)
        df["_vec_pick"] = self._compute_vec_pick(df)
        return df

    def passes_df_on_date(self, df: pd.DataFrame, date: pd.Timestamp) -> bool:
        """单日判断"""
        hist = self._get_hist(df, date)
        if hist is None or hist.empty:
            return False
        if len(hist) < self.min_bars:
            return False
        
        if not self._pattern_filter(hist):
            return False
        if self._zxdq_ratio_filter is not None and not self._zxdq_ratio_filter(hist):
            return False
        if self._zxdq_gt_filter is not None and not self._zxdq_gt_filter(hist):
            return False
        if self._wma_filter is not None and not self._wma_filter(hist):
            return False
        
        return True

    def brick_growth_on_date(self, df: pd.DataFrame, date: pd.Timestamp) -> float:
        """获取指定日期的砖型图增长倍数"""
        hist = self._get_hist(df, date)
        if len(hist) < 3:
            return -np.inf
        if "brick_growth" in hist.columns:
            val = float(hist["brick_growth"].iloc[-1])
            return val if np.isfinite(val) else -np.inf
        return float(self._pattern_filter.brick_growth_arr(hist)[-1])