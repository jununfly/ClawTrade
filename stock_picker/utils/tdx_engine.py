# -*- coding: utf-8 -*-
"""
通达信版统一评分引擎

评分体系（总分110分）：
- 趋势分 45分：QXYQ(35分) + MA20趋势向上(10分)
- 超卖分 25分：KDJ J值≤16(15分) + CCI超卖(10分)
- 成交量分 20分：缩量信号
- 结构分 20分：N型结构(8分) + 假突破(6分) + 活跃信号(6分)
- 额外分 10分：CCI底背离
"""
from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd

from .indicators import TechnicalIndicators


# =============================================================================
# 默认配置
# =============================================================================

DEFAULT_CONFIG = {
    # KDJ参数
    'kdj_n': 9,
    'kdj_j_max': 16,           # J值 <= 16 得15分
    
    # CCI参数
    'cci_oversold': -100,       # CCI超卖阈值
    'cci_divergence_span': 20,  # 底背离检查窗口
    
    # MA20趋势
    'ma20_period': 20,
    
    # 成交量参数
    'vol_ma5_period': 5,
    'vol_shrink_ratio': 0.5,   # 缩量阈值
    
    # 评分阈值
    'min_score': 70,           # 最小评分
    'min_market_cap': 100,     # 最小市值（亿）
    
    # 风险控制
    'require_vk_recent': True,  # 要求有底部放量信号
    'require_ma20_up': True,  # 要求MA20向上
    'exclude_st': True,         # 排除ST股
    'exclude_etf': True,        # 排除ETF
}


# =============================================================================
# 评分计算函数
# =============================================================================

def calc_trend_score(df: pd.DataFrame, config: dict = None) -> pd.Series:
    """
    趋势分（45分）
    - QXYQ趋势（35分）
    - MA20向上（10分）
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    close = df['close']
    ma20 = close.rolling(window=config.get('ma20_period', 20), min_periods=1).mean()
    
    # QXYQ趋势评分：连续上涨则加分
    qxyq = (close > close.shift(1)).astype(int)
    qxyq_score = qxyq.rolling(window=5, min_periods=1).sum() / 5.0 * 35.0
    
    # MA20趋势评分
    ma20_up = (ma20 > ma20.shift(1)).astype(float)
    ma20_score = ma20_up * 10.0
    
    return qxyq_score + ma20_score


def calc_oversold_score(df: pd.DataFrame, config: dict = None) -> pd.Series:
    """
    超卖分（25分）
    - KDJ J值 <= 16（15分）
    - CCI超卖（10分）
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    ti = TechnicalIndicators
    
    # KDJ J值评分
    kdj_j = df.get('kdj_J')
    if kdj_j is None:
        K, D, J = ti.kdj(df['high'], df['low'], df['close'])
        kdj_j = J
    
    kdj_score = (kdj_j <= config.get('kdj_j_max', 16)).astype(float) * 15.0
    
    # CCI超卖评分
    cci = df.get('CCI')
    if cci is None:
        cci = ti.cci(df['high'], df['low'], df['close'])
    
    cci_score = (cci <= config.get('cci_oversold', -100)).astype(float) * 10.0
    
    return kdj_score + cci_score


def calc_volume_score(df: pd.DataFrame, config: dict = None) -> pd.Series:
    """
    成交量分（20分）
    - 缩量信号：今日成交量 < 5日均量 * 0.5
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    volume = df['volume']
    vol_ma5 = volume.rolling(window=config.get('vol_ma5_period', 5), min_periods=1).mean()
    
    shrink_signal = volume < vol_ma5 * config.get('vol_shrink_ratio', 0.5)
    return shrink_signal.astype(float) * 20.0


def calc_structure_score(df: pd.DataFrame, config: dict = None) -> pd.Series:
    """
    结构分（20分）
    - N型结构（8分）
    - 假突破（6分）
    - 活跃信号（6分）
    """
    close = df['close']
    high = df['high']
    low = df['low']
    
    # N型结构：走势呈现N型形态
    # 条件：低点抬高的同时高点也在抬高
    low_5 = low.rolling(5).min()
    low_10 = low.rolling(10).min()
    high_5 = high.rolling(5).max()
    high_10 = high.rolling(10).max()
    
    n_pattern = (low_5 > low_10) & (high_5 > high_10)
    n_score = n_pattern.astype(float) * 8.0
    
    # 假突破：创近期新高后回落
    high_20 = high.rolling(20).max()
    recent_high = high > high_20.shift(1) * 1.02  # 突破2%
    pullback = close < high_20 * 0.98  # 回落2%
    fake_break = (recent_high.shift(5) & pullback).astype(float)
    fake_score = fake_break * 6.0
    
    # 活跃信号：振幅较大
    amplitude = (high - low) / close * 100
    active = amplitude > amplitude.rolling(20).mean() * 1.2
    active_score = active.astype(float) * 6.0
    
    return n_score + fake_score + active_score


def calc_extra_score(df: pd.DataFrame, config: dict = None) -> pd.Series:
    """
    额外分（10分）
    - CCI底背离
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    cci = df.get('CCI')
    if cci is None:
        ti = TechnicalIndicators()
        cci = ti.cci(df['high'], df['low'], df['close'])
    
    # CCI底背离：价格创新低但CCI未创新低
    span = config.get('cci_divergence_span', 20)
    
    cci_low = cci.rolling(span).min()
    price_low = df['low'].rolling(span).min()
    
    cci_divergence = (cci <= cci_low) & (df['low'] <= price_low * 1.02)
    
    return cci_divergence.astype(float) * 10.0


def evaluate_df(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    评估整张表，返回带评分列的DataFrame
    
    评分列：
    - TREND_SCORE: 趋势分（45分）
    - OVERSOLD_SCORE: 超卖分（25分）
    - VOLUME_SCORE: 成交量分（20分）
    - STRUCTURE_SCORE: 结构分（20分）
    - EXTRA_SCORE: 额外分（10分）
    - TOTAL_SCORE: 总分（110分）
    
    Args:
        df: 包含OHLCV数据的DataFrame
        config: 配置字典
        
    Returns:
        添加了评分列的DataFrame
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    df = df.copy()
    
    # 确保技术指标已计算
    ti = TechnicalIndicators
    if 'kdj_J' not in df.columns:
        K, D, J = ti.kdj(df['high'], df['low'], df['close'])
        df['kdj_K'] = K
        df['kdj_D'] = D
        df['kdj_J'] = J
    
    if 'CCI' not in df.columns:
        df['CCI'] = ti.cci(df['high'], df['low'], df['close'])
    
    if 'BBI' not in df.columns:
        df['BBI'] = ti.bbi(df['close'])
    
    # 计算各项评分
    df['TREND_SCORE'] = calc_trend_score(df, config)
    df['OVERSOLD_SCORE'] = calc_oversold_score(df, config)
    df['VOLUME_SCORE'] = calc_volume_score(df, config)
    df['STRUCTURE_SCORE'] = calc_structure_score(df, config)
    df['EXTRA_SCORE'] = calc_extra_score(df, config)
    
    # 总分
    df['TOTAL_SCORE'] = (
        df['TREND_SCORE'].fillna(0) +
        df['OVERSOLD_SCORE'].fillna(0) +
        df['VOLUME_SCORE'].fillna(0) +
        df['STRUCTURE_SCORE'].fillna(0) +
        df['EXTRA_SCORE'].fillna(0)
    )
    
    # 将最后一行的买入信号存储到DataFrame属性
    last_row = df.iloc[-1] if len(df) > 0 else None
    if last_row is not None:
        signal = check_buy_signal(last_row, config)
        df.attrs['last_buy_signal'] = signal
    
    return df


def compute_all_indicators(df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    计算所有技术指标（兼容旧接口）
    """
    return evaluate_df(df, config)


# =============================================================================
# 风险标记
# =============================================================================

def mark_risks(df: pd.DataFrame) -> pd.DataFrame:
    """
    标记风险类型
    
    风险类型：
    - BASE_RISK: 放量下跌（高位放量阴线）
    - HAS_HIGH_RISK: 30天内创历史新高后的大跌
    - SECOND_HIGH_R: 二次高点风险
    - DOWN_PERIOD_R: 下跌周期风险
    - LJBL_FX: 主力出货风险
    - BK_RECENT: 30天内放量大跌
    """
    df = df.copy()
    close = df['close']
    open_ = df['open']
    high = df['high']
    low = df['low']
    volume = df['volume']
    
    # BASE_RISK: 放量下跌（高位放量阴线 + 跌幅>3%）
    vol_ma20 = volume.rolling(20).mean()
    is_high_vol = volume > vol_ma20 * 1.5
    is_bearish = close < open_
    drop_pct = (close - close.shift(1)) / close.shift(1) * 100
    is_big_drop = drop_pct < -3
    
    df['BASE_RISK'] = (is_high_vol & is_bearish & is_big_drop).astype(int)
    
    # HAS_HIGH_RISK: 创历史新高后大跌
    high_250 = high.rolling(250).max()  # 近一年新高
    new_high = high >= high_250
    after_drop = drop_pct < -5
    df['HAS_HIGH_RISK'] = (new_high.shift(5) & after_drop).astype(int)
    
    # SECOND_HIGH_R: 二次高点
    high_20 = high.rolling(20).max()
    is_high_20 = high >= high_20 * 0.98
    second_high = is_high_20.shift(10) & is_high_20 & after_drop
    df['SECOND_HIGH_R'] = second_high.astype(int)
    
    # DOWN_PERIOD_R: 下跌周期（连续5日下跌）
    daily_down = (close < close.shift(1)).astype(int)
    down_period = daily_down.rolling(5).sum() >= 4
    df['DOWN_PERIOD_R'] = down_period.astype(int)
    
    # LJBL_FX: 主力出货（大阳线后连续下跌）
    big_up = (close - open_) / open_ * 100 > 5
    after_big_up = big_up.shift(1)
    consecutive_down = daily_down.rolling(3).sum() >= 2
    df['LJBL_FX'] = (after_big_up & consecutive_down).astype(int)
    
    # BK_RECENT: 30天内放量大跌
    big_drop = drop_pct < -7
    high_vol = volume > vol_ma20 * 2
    df['BK_RECENT'] = (big_drop & high_vol).rolling(30).max().astype(int)
    
    # 总风险标记
    risk_cols = ['BASE_RISK', 'HAS_HIGH_RISK', 'SECOND_HIGH_R', 'DOWN_PERIOD_R', 'LJBL_FX', 'BK_RECENT']
    df['TOTAL_RISK'] = df[risk_cols].sum(axis=1)
    
    return df


# =============================================================================
# 买入信号检查
# =============================================================================

def check_buy_signal(row: pd.Series, config: dict = None) -> dict:
    """
    检查是否满足B1买入条件
    
    硬条件检查：
    1. KDJ J值 <= 16
    2. 总评分 >= 70
    3. VK_RECENT == 1（有底部放量信号）
    4. TOTAL_RISK == 0（无风险标记）
    5. STRONG_EXCLUDE == 0（不在强势排除列表）
    6. LIQUIDITY_OK == 1（流动性满足要求）
    
    Args:
        row: 包含技术指标的Series
        config: 配置字典
        
    Returns:
        {'is_buy': bool, 'reasons': list}
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    reasons = []
    is_buy = True
    
    # 1. 检查KDJ J值
    kdj_j = row.get('kdj_J')
    if pd.notna(kdj_j) and kdj_j > config.get('kdj_j_max', 16):
        is_buy = False
        reasons.append(f"KDJ J值({kdj_j:.1f})超买")
    
    # 2. 检查总评分
    total_score = row.get('TOTAL_SCORE', 0)
    if pd.notna(total_score) and total_score < config.get('min_score', 70):
        is_buy = False
        reasons.append(f"评分不足({total_score:.0f} < 70)")
    
    # 3. 检查底部放量信号
    vk_recent = row.get('VK_RECENT', 0)
    if config.get('require_vk_recent', True) and vk_recent != 1:
        is_buy = False
        reasons.append("无底部放量信号")
    
    # 4. 检查风险标记
    total_risk = row.get('TOTAL_RISK', 0)
    if pd.notna(total_risk) and total_risk > 0:
        is_buy = False
        reasons.append(f"有风险标记({int(total_risk)})")
    
    # 5. 检查强势排除
    strong_exclude = row.get('STRONG_EXCLUDE', 0)
    if pd.notna(strong_exclude) and strong_exclude == 1:
        is_buy = False
        reasons.append("在强势排除列表")
    
    # 6. 检查流动性
    liquidity_ok = row.get('LIQUIDITY_OK', 1)
    if pd.notna(liquidity_ok) and liquidity_ok != 1:
        is_buy = False
        reasons.append("流动性不足")
    
    return {'is_buy': is_buy, 'reasons': reasons}