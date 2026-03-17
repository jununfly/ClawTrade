"""
持仓管理模块
"""

from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from dataclasses import dataclass, field
from ..config import CONFIG


@dataclass
class Position:
    """持仓记录"""
    stock_code: str
    quantity: int      # 股数
    avg_cost: float    # 平均成本
    buy_date: str      # 买入日期
    buy_price: float   # 买入价格
    
    @property
    def total_cost(self) -> float:
        return self.quantity * self.avg_cost


@dataclass
class Trade:
    """交易记录"""
    date: str
    stock_code: str
    action: str        # "BUY" or "SELL"
    price: float
    quantity: int
    commission: float = 0.0
    
    @property
    def total_amount(self) -> float:
        return self.price * self.quantity


class PortfolioManager:
    """持仓管理器"""
    
    def __init__(self, initial_cash: float = None):
        self.initial_cash = initial_cash or CONFIG["default_initial_capital"]
        self.cash = self.initial_cash
        self.positions: Dict[str, Position] = {}  # stock_code -> Position
        self.trades: List[Trade] = []
        self.commission_rate = CONFIG["default_commission"]
    
    def buy(self, stock_code: str, price: float, quantity: int, date: str = None) -> bool:
        """
        买入股票
        
        Args:
            stock_code: 股票代码
            price: 买入价格
            quantity: 股数
            date: 交易日期
        
        Returns:
            是否成功
        """
        date = date or datetime.now().strftime("%Y%m%d")
        commission = price * quantity * self.commission_rate
        total_cost = price * quantity + commission
        
        if total_cost > self.cash:
            print(f"资金不足，需要 {total_cost:.2f}，当前可用 {self.cash:.2f}")
            return False
        
        self.cash -= total_cost
        
        if stock_code in self.positions:
            # 增持
            pos = self.positions[stock_code]
            total_shares = pos.quantity + quantity
            total_value = pos.total_cost + price * quantity
            pos.quantity = total_shares
            pos.avg_cost = total_value / total_shares
        else:
            # 新建持仓
            self.positions[stock_code] = Position(
                stock_code=stock_code,
                quantity=quantity,
                avg_cost=price,
                buy_date=date,
                buy_price=price
            )
        
        self.trades.append(Trade(
            date=date,
            stock_code=stock_code,
            action="BUY",
            price=price,
            quantity=quantity,
            commission=commission
        ))
        
        return True
    
    def sell(self, stock_code: str, price: float, quantity: int, date: str = None) -> bool:
        """
        卖出股票
        
        Args:
            stock_code: 股票代码
            price: 卖出价格
            quantity: 股数
            date: 交易日期
        
        Returns:
            是否成功
        """
        if stock_code not in self.positions:
            print(f"没有持仓 {stock_code}")
            return False
        
        pos = self.positions[stock_code]
        if quantity > pos.quantity:
            print(f"持仓不足，当前 {pos.quantity} 股，只能卖出 {pos.quantity} 股")
            return False
        
        date = date or datetime.now().strftime("%Y%m%d")
        commission = price * quantity * self.commission_rate
        proceeds = price * quantity - commission
        
        self.cash += proceeds
        pos.quantity -= quantity
        
        if pos.quantity == 0:
            del self.positions[stock_code]
        
        self.trades.append(Trade(
            date=date,
            stock_code=stock_code,
            action="SELL",
            price=price,
            quantity=quantity,
            commission=commission
        ))
        
        return True
    
    def get_positions(self) -> pd.DataFrame:
        """获取当前持仓"""
        if not self.positions:
            return pd.DataFrame()
        
        data = []
        for code, pos in self.positions.items():
            data.append({
                "stock_code": code,
                "quantity": pos.quantity,
                "avg_cost": pos.avg_cost,
                "total_cost": pos.total_cost,
                "buy_date": pos.buy_date,
            })
        
        return pd.DataFrame(data)
    
    def get_position(self, stock_code: str) -> Optional[Position]:
        """获取指定持仓"""
        return self.positions.get(stock_code)
    
    def get_total_value(self, prices: Dict[str, float] = None) -> float:
        """
        获取总资产
        
        Args:
            prices: 股票价格字典 {"600519": 1700.0}
        
        Returns:
            总资产
        """
        total = self.cash
        
        if prices:
            for code, pos in self.positions.items():
                price = prices.get(code, pos.avg_cost)
                total += pos.quantity * price
        else:
            # 按成本价计算
            for pos in self.positions.values():
                total += pos.total_cost
        
        return total
    
    def get_total_cost(self) -> float:
        """获取总成本"""
        return sum(pos.total_cost for pos in self.positions.values())
    
    def get_trades(self) -> pd.DataFrame:
        """获取交易记录"""
        if not self.trades:
            return pd.DataFrame()
        
        data = []
        for t in self.trades:
            data.append({
                "date": t.date,
                "stock_code": t.stock_code,
                "action": t.action,
                "price": t.price,
                "quantity": t.quantity,
                "amount": t.total_amount,
                "commission": t.commission,
            })
        
        return pd.DataFrame(data)
    
    def get_summary(self) -> Dict:
        """获取账户摘要"""
        return {
            "initial_cash": self.initial_cash,
            "cash": self.cash,
            "total_cost": self.get_total_cost(),
            "position_count": len(self.positions),
            "trade_count": len(self.trades),
        }
    
    def update_prices(self, prices: Dict[str, float]):
        """更新持仓价格并计算盈亏"""
        for code, pos in self.positions.items():
            current_price = prices.get(code, pos.avg_cost)
            pos.current_price = current_price
            pos.unrealized_pnl = (current_price - pos.avg_cost) * pos.quantity
            pos.unrealized_pnl_pct = (current_price - pos.avg_cost) / pos.avg_cost if pos.avg_cost > 0 else 0
    
    def clear(self):
        """清空所有持仓"""
        self.cash = self.initial_cash
        self.positions = {}
        self.trades = []