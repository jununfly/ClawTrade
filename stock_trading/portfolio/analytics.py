"""
持仓分析模块
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime


class PortfolioAnalytics:
    """持仓分析器"""
    
    def __init__(self, portfolio_manager):
        """
        Args:
            portfolio_manager: PortfolioManager 实例
        """
        self.pm = portfolio_manager
    
    def calculate_metrics(self, prices: Dict[str, float] = None) -> Dict:
        """
        计算持仓绩效指标
        
        Args:
            prices: 当前股价字典
        
        Returns:
            绩效指标字典
        """
        total_value = self.pm.get_total_value(prices)
        total_cost = self.pm.get_total_cost()
        initial_cash = self.pm.initial_cash
        
        # 总盈亏
        total_pnl = total_value - initial_cash
        total_pnl_pct = (total_value - initial_cash) / initial_cash if initial_cash > 0 else 0
        
        # 持仓盈亏
        position_pnl = total_value - self.pm.cash - total_cost
        
        return {
            "initial_cash": initial_cash,
            "current_cash": self.pm.cash,
            "total_value": total_value,
            "total_cost": total_cost,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "position_pnl": position_pnl,
            "position_count": len(self.pm.positions),
            "cash_ratio": self.pm.cash / total_value if total_value > 0 else 0,
        }
    
    def calculate_position_returns(self, prices: Dict[str, float]) -> pd.DataFrame:
        """
        计算各持仓的收益率
        
        Args:
            prices: 当前股价字典
        
        Returns:
            持仓收益率DataFrame
        """
        if not self.pm.positions:
            return pd.DataFrame()
        
        data = []
        for code, pos in self.pm.positions.items():
            current_price = prices.get(code, pos.avg_cost)
            pnl = (current_price - pos.avg_cost) * pos.quantity
            pnl_pct = (current_price - pos.avg_cost) / pos.avg_cost if pos.avg_cost > 0 else 0
            
            data.append({
                "stock_code": code,
                "quantity": pos.quantity,
                "avg_cost": pos.avg_cost,
                "current_price": current_price,
                "total_value": current_price * pos.quantity,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "weight": (current_price * pos.quantity) / self.pm.get_total_value(prices) if prices else 0,
            })
        
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values("pnl", ascending=False)
        
        return df
    
    def calculate_max_drawdown(self, value_history: List[float]) -> float:
        """
        计算最大回撤
        
        Args:
            value_history: 资产历史变化列表
        
        Returns:
            最大回撤比例
        """
        if not value_history:
            return 0.0
        
        df = pd.Series(value_history)
        cummax = df.cummax()
        drawdown = (df - cummax) / cummax
        
        return drawdown.min()
    
    def calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.03) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率列表
            risk_free_rate: 无风险利率
        
        Returns:
            夏普比率
        """
        if not returns:
            return 0.0
        
        returns = np.array(returns)
        excess_returns = returns - risk_free_rate / 252  # 日化无风险利率
        
        if np.std(returns) == 0:
            return 0.0
        
        return np.mean(excess_returns) / np.std(returns) * np.sqrt(252)
    
    def calculate_volatility(self, returns: List[float]) -> float:
        """
        计算波动率
        
        Args:
            returns: 收益率列表
        
        Returns:
            年化波动率
        """
        if not returns:
            return 0.0
        
        return np.std(returns) * np.sqrt(252)
    
    def get_risk_metrics(self, prices: Dict[str, float] = None) -> Dict:
        """
        获取风险指标
        
        Args:
            prices: 当前股价字典
        
        Returns:
            风险指标字典
        """
        # 简单计算：基于持仓权重和个股波动
        positions_df = self.calculate_position_returns(prices)
        
        if positions_df.empty:
            return {
                "max_position_weight": 0,
                "position_concentration": 0,
                "total_exposure": 0,
            }
        
        total_value = self.pm.get_total_value(prices)
        
        return {
            "max_position_weight": positions_df["weight"].max() if "weight" in positions_df else 0,
            "position_concentration": (positions_df["weight"] ** 2).sum() if "weight" in positions_df else 0,
            "total_exposure": total_value / self.pm.initial_cash if self.pm.initial_cash > 0 else 0,
        }
    
    def generate_report(self, prices: Dict[str, float] = None) -> str:
        """
        生成持仓分析报告
        
        Args:
            prices: 当前股价字典
        
        Returns:
            报告文本
        """
        metrics = self.calculate_metrics(prices)
        positions_df = self.calculate_position_returns(prices)
        risk = self.get_risk_metrics(prices)
        
        lines = [
            "=" * 50,
            "持仓分析报告",
            "=" * 50,
            f"初始资金: {metrics['initial_cash']:,.2f}",
            f"当前现金: {metrics['current_cash']:,.2f}",
            f"持仓成本: {metrics['total_cost']:,.2f}",
            f"总资产: {metrics['total_value']:,.2f}",
            f"总盈亏: {metrics['total_pnl']:,.2f} ({metrics['total_pnl_pct']:.2%})",
            f"持仓数量: {metrics['position_count']}",
            f"现金占比: {metrics['cash_ratio']:.2%}",
            "",
            "风险指标:",
            f"  最大持仓权重: {risk['max_position_weight']:.2%}",
            f"  持仓集中度: {risk['position_concentration']:.2%}",
            f"  总敞口: {risk['total_exposure']:.2%}",
            "",
            "持仓明细:",
        ]
        
        if not positions_df.empty:
            for _, row in positions_df.iterrows():
                lines.append(
                    f"  {row['stock_code']}: "
                    f"成本 {row['avg_cost']:.2f}, "
                    f"现价 {row['current_price']:.2f}, "
                    f"盈亏 {row['pnl']:.2f} ({row['pnl_pct']:.2%})"
                )
        else:
            lines.append("  (无持仓)")
        
        lines.append("=" * 50)
        
        return "\n".join(lines)