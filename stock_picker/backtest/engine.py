# -*- coding: utf-8 -*-
"""
回测引擎
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd
import numpy as np


@dataclass
class Trade:
    """交易记录"""
    code: str
    date: str
    action: str  # "buy" 或 "sell"
    price: float
    quantity: int
    commission: float = 0.0


@dataclass
class Position:
    """持仓"""
    code: str
    quantity: int
    avg_cost: float
    current_price: float = 0.0
    
    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price
    
    @property
    def profit(self) -> float:
        return (self.current_price - self.avg_cost) * self.quantity


@dataclass
class BacktestResult:
    """回测结果"""
    trades: List[Trade] = field(default_factory=list)
    positions: Dict[str, Position] = field(default_factory=dict)
    initial_capital: float = 1000000.0
    final_value: float = 0.0
    
    # 绩效指标
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "initial_capital": self.initial_capital,
            "final_value": self.final_value,
            "total_return": f"{self.total_return:.2%}",
            "annual_return": f"{self.annual_return:.2%}",
            "sharpe_ratio": f"{self.sharpe_ratio:.2f}",
            "max_drawdown": f"{self.max_drawdown:.2%}",
            "win_rate": f"{self.win_rate:.2%}",
            "total_trades": self.total_trades,
        }


class BacktestEngine:
    """回测引擎"""

    def __init__(
        self,
        initial_capital: float = 1000000.0,
        commission: float = 0.0003,
        slippage: float = 0.0,
    ):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金
            commission: 手续费率
            slippage: 滑点
        """
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.daily_values: List[float] = []

    def buy(
        self,
        code: str,
        date: str,
        price: float,
        quantity: int,
    ) -> bool:
        """
        买入
        
        Args:
            code: 股票代码
            date: 日期
            price: 价格
            quantity: 数量
            
        Returns:
            是否成功
        """
        # 考虑滑点
        actual_price = price * (1 + self.slippage)
        
        # 计算手续费
        amount = actual_price * quantity
        commission = amount * self.commission
        
        total_cost = amount + commission
        
        if total_cost > self.cash:
            return False
        
        self.cash -= total_cost
        
        # 更新持仓
        if code in self.positions:
            pos = self.positions[code]
            total_quantity = pos.quantity + quantity
            pos.avg_cost = (pos.avg_cost * pos.quantity + actual_price * quantity) / total_quantity
            pos.quantity = total_quantity
        else:
            self.positions[code] = Position(
                code=code,
                quantity=quantity,
                avg_cost=actual_price,
                current_price=actual_price,
            )
        
        # 记录交易
        self.trades.append(Trade(
            code=code,
            date=date,
            action="buy",
            price=actual_price,
            quantity=quantity,
            commission=commission,
        ))
        
        return True

    def sell(
        self,
        code: str,
        date: str,
        price: float,
        quantity: Optional[int] = None,
    ) -> bool:
        """
        卖出
        
        Args:
            code: 股票代码
            date: 日期
            price: 价格
            quantity: 卖出数量，None表示全部
            
        Returns:
            是否成功
        """
        if code not in self.positions:
            return False
        
        pos = self.positions[code]
        
        # 考虑滑点
        actual_price = price * (1 - self.slippage)
        
        # 卖出数量
        sell_qty = quantity if quantity else pos.quantity
        
        if sell_qty > pos.quantity:
            sell_qty = pos.quantity
        
        # 计算手续费
        amount = actual_price * sell_qty
        commission = amount * self.commission
        
        self.cash += amount - commission
        
        # 更新持仓
        pos.quantity -= sell_qty
        if pos.quantity <= 0:
            del self.positions[code]
        
        # 记录交易
        self.trades.append(Trade(
            code=code,
            date=date,
            action="sell",
            price=actual_price,
            quantity=sell_qty,
            commission=commission,
        ))
        
        return True

    def update_prices(self, prices: Dict[str, float]) -> None:
        """更新持仓价格"""
        for code, price in prices.items():
            if code in self.positions:
                self.positions[code].current_price = price

    def get_total_value(self) -> float:
        """获取总资产"""
        position_value = sum(pos.market_value for pos in self.positions.values())
        return self.cash + position_value

    def run(
        self,
        data: Dict[str, pd.DataFrame],
        signals: Dict[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BacktestResult:
        """
        运行回测
        
        Args:
            data: 股票代码到DataFrame的映射
            signals: 股票代码到信号日期列表的映射
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            回测结果
        """
        # 收集所有日期
        all_dates = set()
        for df in data.values():
            all_dates.update(df["date"].astype(str).tolist())
        
        if start_date:
            all_dates = {d for d in all_dates if d >= start_date}
        if end_date:
            all_dates = {d for d in all_dates if d <= end_date}
        
        all_dates = sorted(all_dates)
        
        # 按日期遍历
        for date in all_dates:
            # 更新持仓价格
            prices = {}
            for code, df in data.items():
                day_data = df[df["date"].astype(str) == date]
                if not day_data.empty:
                    prices[code] = day_data.iloc[0]["close"]
            
            self.update_prices(prices)
            
            # 记录每日价值
            self.daily_values.append(self.get_total_value())
            
            # 处理买入信号
            if date in signals:
                for code in signals[date]:
                    if code not in self.positions:
                        day_data = data[data].get(code)
                        if day_data is not None:
                            day_data = day_data[day_data["date"].astype(str) == date]
                            if not day_data.empty:
                                price = day_data.iloc[0]["close"]
                                # 买入1手
                                self.buy(code, date, price, 100)
        
        # 计算最终结果
        return self._calc_result(len(all_dates))

    def _calc_result(self, trading_days: int) -> BacktestResult:
        """计算回测结果"""
        result = BacktestResult(
            trades=self.trades.copy(),
            positions=self.positions.copy(),
            initial_capital=self.initial_capital,
            final_value=self.get_total_value(),
            total_trades=len(self.trades),
        )
        
        # 总收益率
        result.total_return = (result.final_value - self.initial_capital)) / self.initial_capital
        
        # 年化收益率
        if trading_days > 0:
            years = trading_days / 252
            result.annual_return = (1 + result.total_return) ** (1 / years) - 1
        
        # 夏普比率
        if len(self.daily_values) > 1:
            returns = np.diff(self.daily_values) / self.daily_values[:-1]
            if returns.std() > 0:
                result.sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
        
        # 最大回撤
        if len(self.daily_values) > 0:
            peak = self.daily_values[0]
            max_dd = 0
            for v in self.daily_values:
                if v > peak:
                    peak = v
                dd = (peak - v) / peak
                if dd > max_dd:
                    max_dd = dd
            result.max_drawdown = max_dd
        
        # 胜率
        if result.total_trades > 0:
            sell_trades = [t for t in self.trades if t.action == "sell"]
            if sell_trades:
                wins = 0
                for i, t in enumerate(sell_trades):
                    if t.action == "sell":
                        # 找对应的买入
                        code = t.code
                        buy_trades = [tr for tr in self.trades if tr.action == "buy" and tr.code == code]
                        if buy_trades:
                            last_buy = buy_trades[-1]
                            if t.price > last_buy.price:
                                wins += 1
                result.win_rate = wins / len(sell_trades)
        
        return result

    def print_result(self, result: BacktestResult) -> None:
        """打印回测结果"""
        print("\n" + "=" * 50)
        print("回测结果")
        print("=" * 50)
        print(f"初始资金: {result.initial_capital:,.2f}")
        print(f"最终价值: {result.final_value:,.2f}")
        print(f"总收益率: {result.total_return:.2%}")
        print(f"年化收益率: {result.annual_return:.2%}")
        print(f"夏普比率: {result.sharpe_ratio:.2f}")
        print(f"最大回撤: {result.max_drawdown:.2%}")
        print(f"胜率: {result.win_rate:.2%}")
        print(f"交易次数: {result.total_trades}")
        print("=" * 50)