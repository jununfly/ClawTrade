"""
回测引擎模块
"""

from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
import pandas as pd
import numpy as np
from datetime import datetime

from ..strategy.base import BaseStrategy
from ..data.fetcher import StockFetcher
from ..portfolio.manager import PortfolioManager
from ..config import CONFIG


@dataclass
class BacktestResult:
    """回测结果"""
    stock_code: str
    start_date: str
    end_date: str
    initial_capital: float
    final_value: float
    total_return: float
    total_return_pct: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    
    def to_dict(self) -> Dict:
        return {
            "stock_code": self.stock_code,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "initial_capital": self.initial_capital,
            "final_value": self.final_value,
            "total_return": self.total_return,
            "total_return_pct": self.total_return_pct,
            "annual_return": self.annual_return,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "trade_count": self.trade_count,
        }
    
    def __str__(self) -> str:
        return f"""
=====================================
回测结果: {self.stock_code}
=====================================
回测区间: {self.start_date} ~ {self.end_date}
初始资金: {self.initial_capital:,.2f}
最终价值: {self.final_value:,.2f}
总收益: {self.total_return:,.2f} ({self.total_return_pct:.2%})
年化收益: {self.annual_return:.2%}
夏普比率: {self.sharpe_ratio:.2f}
最大回撤: {self.max_drawdown:.2%}
胜率: {self.win_rate:.2%}
交易次数: {self.trade_count}
=====================================
"""


class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        strategy: BaseStrategy,
        initial_capital: float = None,
        commission: float = None,
        slippage: float = None,
    ):
        """
        Args:
            strategy: 交易策略实例
            initial_capital: 初始资金
            commission: 手续费率
            slippage: 滑点
        """
        self.strategy = strategy
        self.initial_capital = initial_capital or CONFIG["default_initial_capital"]
        self.commission = commission or CONFIG["default_commission"]
        self.slippage = slippage or CONFIG["default_slippage"]
        
        self.fetcher = StockFetcher()
        self.portfolio = PortfolioManager(initial_cash=self.initial_capital)
        
        self.data: pd.DataFrame = None
        self.trades: List[Dict] = []
        self.value_history: List[float] = []
    
    def load_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq"
    ) -> bool:
        """
        加载历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adjust: 复权类型
        
        Returns:
            是否成功
        """
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        
        df = self.fetcher.get_kline(
            stock_code=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )
        
        if df.empty:
            print(f"Failed to load data for {stock_code}")
            return False
        
        # 标准化列名
        df = df.rename(columns={
            '日期': 'date',
            '股票代码': 'code',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'change',
            '换手率': 'turnover',
        })
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        self.data = df
        return True
    
    def run(self, verbose: bool = True) -> BacktestResult:
        """
        运行回测
        
        Args:
            verbose: 是否打印详情
        
        Returns:
            BacktestResult
        """
        if self.data is None or self.data.empty:
            raise ValueError("No data loaded. Call load_data() first.")
        
        # 重置状态
        self.portfolio.clear()
        self.trades = []
        self.value_history = []
        
        # 生成信号
        signals_df = self.strategy.generate_signals(self.data)
        
        if 'signal' not in signals_df.columns:
            signals_df['signal'] = 'HOLD'
        
        # 逐日执行
        position = 0  # 当前持仓股数
        
        for i in range(len(self.data)):
            date = self.data.iloc[i]['date'].strftime('%Y%m%d')
            price = self.data.iloc[i]['close']
            signal = signals_df.iloc[i]['signal']
            
            # 买入信号
            if signal == 'BUY' and position == 0:
                # 使用滑点
                buy_price = price * (1 + self.slippage)
                max_shares = int(self.portfolio.cash / buy_price)
                
                if max_shares > 0:
                    self.portfolio.buy(
                        self.stock_code,
                        buy_price,
                        max_shares,
                        date
                    )
                    position = max_shares
                    self.trades.append({
                        'date': date,
                        'action': 'BUY',
                        'price': buy_price,
                        'quantity': max_shares,
                    })
            
            # 卖出信号
            elif signal == 'SELL' and position > 0:
                # 使用滑点
                sell_price = price * (1 - self.slippage)
                
                self.portfolio.sell(
                    self.stock_code,
                    sell_price,
                    position,
                    date
                )
                self.trades.append({
                    'date': date,
                    'action': 'SELL',
                    'price': sell_price,
                    'quantity': position,
                })
                position = 0
            
            # 记录资产变化
            current_value = self.portfolio.cash
            if position > 0:
                current_value += position * price
            
            self.value_history.append(current_value)
        
        # 计算结果
        result = self._calculate_result()
        
        if verbose:
            print(result)
        
        return result
    
    def _calculate_result(self) -> BacktestResult:
        """计算回测结果"""
        final_value = self.value_history[-1] if self.value_history else self.initial_capital
        total_return = final_value - self.initial_capital
        total_return_pct = total_return / self.initial_capital if self.initial_capital > 0 else 0
        
        # 年化收益率
        if self.start_date and self.end_date:
            start = datetime.strptime(self.start_date, '%Y%m%d')
            end = datetime.strptime(self.end_date, '%Y%m%d')
            days = (end - start).days
            years = days / 365
            annual_return = (final_value / self.initial_capital) ** (1 / years) - 1 if years > 0 else 0
        else:
            annual_return = 0
        
        # 计算收益率序列
        returns = []
        for i in range(1, len(self.value_history)):
            ret = (self.value_history[i] - self.value_history[i-1]) / self.value_history[i-1]
            returns.append(ret)
        
        # 夏普比率
        if returns:
            returns_arr = np.array(returns)
            excess_returns = returns_arr - 0.03 / 252
            sharpe_ratio = np.mean(excess_returns) / np.std(returns_arr) * np.sqrt(252) if np.std(returns_arr) > 0 else 0
        else:
            sharpe_ratio = 0
        
        # 最大回撤
        max_drawdown = 0
        peak = self.initial_capital
        for value in self.value_history:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 胜率
        winning_trades = 0
        total_trades = len(self.trades)
        
        if total_trades >= 2:
            for i in range(0, len(self.trades) - 1, 2):
                if i + 1 < len(self.trades):
                    buy_trade = self.trades[i]
                    sell_trade = self.trades[i + 1]
                    if sell_trade['price'] > buy_trade['price']:
                        winning_trades += 1
        
        win_rate = winning_trades / (total_trades / 2) if total_trades > 0 else 0
        
        return BacktestResult(
            stock_code=self.stock_code,
            start_date=self.start_date,
            end_date=self.end_date,
            initial_capital=self.initial_capital,
            final_value=final_value,
            total_return=total_return,
            total_return_pct=total_return_pct,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            trade_count=total_trades,
        )
    
    def get_trades_df(self) -> pd.DataFrame:
        """获取交易记录DataFrame"""
        return pd.DataFrame(self.trades)
    
    def get_value_history_df(self) -> pd.DataFrame:
        """获取资产变化历史"""
        if not self.value_history:
            return pd.DataFrame()
        
        if self.data is None or len(self.data) != len(self.value_history):
            return pd.DataFrame({'value': self.value_history})
        
        df = self.data[['date', 'close']].copy()
        df['value'] = self.value_history
        return df
    
    def plot_results(self, save_path: str = None):
        """绘制回测结果图表"""
        try:
            import matplotlib.pyplot as plt
            
            df = self.get_value_history_df()
            
            if df.empty:
                print("No data to plot")
                return
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # 资产曲线
            if 'value' in df.columns:
                ax1.plot(df.index, df['value'], label='Portfolio Value')
                ax1.axhline(y=self.initial_capital, color='r', linestyle='--', label='Initial Capital')
                ax1.set_title('Portfolio Value Over Time')
                ax1.set_xlabel('Days')
                ax1.set_ylabel('Value')
                ax1.legend()
                ax1.grid(True)
            
            # 价格曲线（如果有钱货）
            if 'close' in df.columns:
                ax2.plot(df.index, df['close'], label='Stock Price', alpha=0.7)
                ax2.set_title('Stock Price')
                ax2.set_xlabel('Days')
                ax2.set_ylabel('Price')
                ax2.legend()
                ax2.grid(True)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                print(f"Chart saved to {save_path}")
            else:
                plt.show()
                
        except ImportError:
            print("matplotlib not installed, cannot plot")