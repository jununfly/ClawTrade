"""
配置模块
"""

import os

# 项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 配置
CONFIG = {
    # 数据库
    "database": os.path.join(BASE_DIR, "data", "stocks.db"),
    "cache_dir": os.path.join(BASE_DIR, "data", "cache"),
    
    # 数据源
    "data_source": "akshare",  # 或 "tushare"
    
    # 回测参数
    "default_commission": 0.0003,
    "default_slippage": 0.001,
    "default_initial_capital": 1000000,
    
    # 策略参数
    "default_period": 20,
    "default_threshold": 0.05,
    
    # 告警
    "price_alert_threshold": 0.02,
    
    # 日志
    "log_dir": os.path.join(BASE_DIR, "logs"),
    "log_level": "INFO",
}

# 确保目录存在
for key in ["cache_dir", "log_dir"]:
    os.makedirs(CONFIG[key], exist_ok=True)