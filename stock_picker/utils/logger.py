# -*- coding: utf-8 -*-
"""
日志工具
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(
    name: str = "ClawTrade",
    level: int = logging.INFO,
    log_dir: str = "logs",
    console: bool = True,
) -> logging.Logger:
    """
    设置日志
    
    Args:
        name: 日志名称
        level: 日志级别
        log_dir: 日志目录
        console: 是否输出到控制台
        
    Returns:
        Logger对象
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 清除已有的handlers
    logger.handlers.clear()
    
    # 日志格式
    fmt = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)
    
    # 控制台handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 文件handler
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        log_path / f"{name}_{today}.log",
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


# 默认logger
default_logger = setup_logger()