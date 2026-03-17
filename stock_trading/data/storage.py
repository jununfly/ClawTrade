"""
数据存储模块
"""

import sqlite3
import json
import os
from typing import List, Dict, Optional
from datetime import datetime

from ..config import CONFIG


class Database:
    """SQLite数据库管理"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or CONFIG["database"]
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 持仓表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                buy_date TEXT,
                buy_price REAL,
                quantity INTEGER,
                current_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                trade_type TEXT,
                price REAL,
                quantity INTEGER,
                trade_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 策略信号表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                strategy_id TEXT,
                signal_type TEXT,
                confidence REAL,
                reason TEXT,
                signal_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 回测结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT,
                stock_code TEXT,
                start_date TEXT,
                end_date TEXT,
                initial_capital REAL,
                final_capital REAL,
                total_return REAL,
                annual_return REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                win_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def execute(self, sql: str, params: tuple = None):
        """执行SQL"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        conn.close()
    
    def fetch_all(self, sql: str, params: tuple = None) -> List:
        """查询所有"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return result
    
    def fetch_one(self, sql: str, params: tuple = None) -> Optional:
        """查询一条"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchone()
        conn.close()
        return result
    
    # 持仓操作
    def add_position(self, stock_code: str, stock_name: str, 
                    buy_date: str, buy_price: float, quantity: int):
        """添加持仓"""
        self.execute(
            "INSERT INTO positions (stock_code, stock_name, buy_date, buy_price, quantity) VALUES (?, ?, ?, ?, ?)",
            (stock_code, stock_name, buy_date, buy_price, quantity)
        )
    
    def update_position_price(self, stock_code: str, current_price: float):
        """更新持仓现价"""
        self.execute(
            "UPDATE positions SET current_price = ? WHERE stock_code = ?",
            (current_price, stock_code)
        )
    
    def get_positions(self) -> List:
        """获取所有持仓"""
        return self.fetch_all("SELECT * FROM positions")
    
    def delete_position(self, stock_code: str):
        """删除持仓"""
        self.execute("DELETE FROM positions WHERE stock_code = ?", (stock_code,))
    
    # 交易记录操作
    def add_trade(self, stock_code: str, stock_name: str, trade_type: str,
                  price: float, quantity: int, trade_date: str):
        """添加交易记录"""
        self.execute(
            "INSERT INTO trades (stock_code, stock_name, trade_type, price, quantity, trade_date) VALUES (?, ?, ?, ?, ?, ?)",
            (stock_code, stock_name, trade_type, price, quantity, trade_date)
        )
    
    def get_trades(self, stock_code: str = None) -> List:
        """获取交易记录"""
        if stock_code:
            return self.fetch_all("SELECT * FROM trades WHERE stock_code = ? ORDER BY trade_date DESC", (stock_code,))
        return self.fetch_all("SELECT * FROM trades ORDER BY trade_date DESC")
    
    # 信号操作
    def add_signal(self, stock_code: str, strategy_id: str, signal_type: str,
                   confidence: float, reason: str, signal_date: str):
        """添加信号"""
        self.execute(
            "INSERT INTO signals (stock_code, strategy_id, signal_type, confidence, reason, signal_date) VALUES (?, ?, ?, ?, ?, ?)",
            (stock_code, strategy_id, signal_type, confidence, reason, signal_date)
        )
    
    def get_signals(self, stock_code: str = None) -> List:
        """获取信号"""
        if stock_code:
            return self.fetch_all("SELECT * FROM signals WHERE stock_code = ? ORDER BY signal_date DESC", (stock_code,))
        return self.fetch_all("SELECT * FROM signals ORDER BY signal_date DESC")
    
    # 回测结果操作
    def save_backtest_result(self, result: dict):
        """保存回测结果"""
        self.execute(
            """INSERT INTO backtest_results 
               (strategy_id, stock_code, start_date, end_date, initial_capital, 
                final_capital, total_return, annual_return, sharpe_ratio, max_drawdown, win_rate) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (result.get("strategy_id"), result.get("stock_code"),
             result.get("start_date"), result.get("end_date"),
             result.get("initial_capital"), result.get("final_capital"),
             result.get("total_return"), result.get("annual_return"),
             result.get("sharpe_ratio"), result.get("max_drawdown"),
             result.get("win_rate"))
        )
    
    def get_backtest_results(self) -> List:
        """获取回测结果"""
        return self.fetch_all("SELECT * FROM backtest_results ORDER BY created_at DESC")