# -*- coding: utf-8 -*-
"""
数据存储模块
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


class DataStorage:
    """数据存储"""

    def __init__(self, db_path: Optional[Path] = None):
        """
        初始化存储
        
        Args:
            db_path: SQLite数据库路径
        """
        self.db_path = db_path or Path("data/stocks.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 股票日线数据表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_data (
                code TEXT,
                date TEXT,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                volume REAL,
                PRIMARY KEY (code, date)
            )
        """)
        
        # 候选股票表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                date TEXT,
                strategy TEXT,
                close REAL,
                turnover_n REAL,
                brick_growth REAL,
                created_at TEXT
            )
        """)
        
        # 选股结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                date TEXT,
                strategy TEXT,
                score REAL,
                verdict TEXT,
                comment TEXT,
                created_at TEXT
            )
        """)
        
        conn.commit()
        conn.close()

    def save_daily_data(self, code: str, df: pd.DataFrame) -> None:
        """保存日线数据"""
        conn = sqlite3.connect(self.db_path)
        df_to_save = df.copy()
        df_to_save["code"] = code
        df_to_save["date"] = df_to_save["date"].astype(str)
        df_to_save.to_sql("daily_data", conn, if_exists="append", index=False)
        conn.close()

    def load_daily_data(
        self,
        code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """加载日线数据"""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT * FROM daily_data WHERE code = ?"
        params = [code]
        
        if start:
            query += " AND date >= ?"
            params.append(start)
        if end:
            query += " AND date <= ?"
            params.append(end)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        
        return df

    def save_candidates(self, candidates: List[Dict[str, Any]]) -> None:
        """保存候选股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for c in candidates:
            cursor.execute("""
                INSERT INTO candidates (code, date, strategy, close, turnover_n, brick_growth, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                c.get("code"),
                c.get("date"),
                c.get("strategy"),
                c.get("close"),
                c.get("turnover_n"),
                c.get("brick_growth"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
        
        conn.commit()
        conn.close()

    def load_candidates(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """加载候选股票"""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT * FROM candidates"
        if date:
            query += f" WHERE date = '{date}'"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict("records") if not df.empty else []

    def save_recommendations(self, recommendations: List[Dict[str, Any]]) -> None:
        """保存推荐结果"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for r in recommendations:
            cursor.execute("""
                INSERT INTO recommendations (code, date, strategy, score, verdict, comment, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get("code"),
                r.get("date"),
                r.get("strategy"),
                r.get("score"),
                r.get("verdict"),
                r.get("comment"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
        
        conn.commit()
        conn.close()

    def load_recommendations(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """加载推荐结果"""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT * FROM recommendations"
        if date:
            query += f" WHERE date = '{date}'"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict("records") if not df.empty else []

    def save_json(self, filepath: Path, data: Any) -> None:
        """保存JSON文件"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def load_json(self, filepath: Path) -> Any:
        """加载JSON文件"""
        if not filepath.exists():
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def clear_candidates(self) -> None:
        """清空候选股票表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM candidates")
        conn.commit()
        conn.close()

    def clear_recommendations(self) -> None:
        """清空推荐结果表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM recommendations")
        conn.commit()
        conn.close()