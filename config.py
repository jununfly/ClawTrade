# -*- coding: utf-8 -*-
"""
配置文件
"""

import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent

# 数据目录
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CANDIDATES_DIR = DATA_DIR / "candidates"
KLINE_DIR = DATA_DIR / "kline"
REVIEW_DIR = DATA_DIR / "review"
CACHE_DIR = DATA_DIR / "cache"
RESULTS_DIR = DATA_DIR / "results"

# 日志目录
LOGS_DIR = PROJECT_ROOT / "logs"

# 数据库
DATABASE_PATH = DATA_DIR / "stocks.db"

# 数据源配置
DATA_SOURCE = "akshare"  # akshare 或 tushare

# Tushare Token
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")

# Gemini API Key
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# 回测参数
DEFAULT_COMMISSION = 0.0003  # 手续费
DEFAULT_INITIAL_CAPITAL = 1000000  # 初始资金

# 选股参数
MIN_VOLUME = 100000000  # 最小成交额
MIN_TURNOVER = 1.0  # 最小换手率

# 流动性池配置
TOP_M = 5000  # 取成交额最高的股票数量
N_TURNOVER_DAYS = 43  # 滚动成交额窗口天数

# B1策略参数
B1_CONFIG = {
    "enabled": True,
    "zx_m1": 14,
    "zx_m2": 28,
    "zx_m3": 57,
    "zx_m4": 114,
    "j_threshold": 15.0,
    "j_q_threshold": 0.10,
    "kdj_n": 9,
    "zxdq_span": 10,
}

# 砖型图策略参数
BRICK_CONFIG = {
    "enabled": False,
    "n": 8,
    "m1": 3,
    "m2": 12,
    "m3": 12,
    "t": 8.0,
    "shift1": 92.0,
    "shift2": 114.0,
    "sma_w1": 1,
    "sma_w2": 1,
    "sma_w3": 1,
    "daily_return_threshold": 0.2,
    "brick_growth_ratio": 0.5,
    "min_prior_green_bars": 1,
    "zxdq_ratio": 1.47,
    "zxdq_span": 10,
    "require_zxdq_gt_zxdkx": True,
    "require_weekly_ma_bull": True,
    "wma_short": 5,
    "wma_mid": 10,
    "wma_long": 20,
}

# Gemini 复评配置
GEMINI_CONFIG = {
    "model": "gemini-3.1-pro-preview",
    "request_delay": 5,
    "skip_existing": False,
    "suggest_min_score": 4.0,
}

# 策略参数
MOMENTUM_PERIOD = 20  # 动量周期
BREAKOUT_THRESHOLD = 0.05  # 突破阈值

# 目录初始化
def init_directories():
    """初始化所有数据目录"""
    dirs = [
        DATA_DIR,
        RAW_DATA_DIR,
        CANDIDATES_DIR,
        KLINE_DIR,
        REVIEW_DIR,
        CACHE_DIR,
        RESULTS_DIR,
        LOGS_DIR,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


# 初始化目录
init_directories()

CONFIG = {
    # 数据
    "database": str(DATABASE_PATH),
    "cache_dir": str(CACHE_DIR),
    # 数据源
    "data_source": DATA_SOURCE,
    # 回测参数
    "default_commission": DEFAULT_COMMISSION,
    "default_initial_capital": DEFAULT_INITIAL_CAPITAL,
    # 选股参数
    "min_volume": MIN_VOLUME,
    "min_turnover": MIN_TURNOVER,
    # 策略参数
    "momentum_period": MOMENTUM_PERIOD,
    "breakout_threshold": BREAKOUT_THRESHOLD,
}