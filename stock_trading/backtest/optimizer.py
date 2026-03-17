"""
参数优化模块
"""

from typing import Dict, List, Callable, Any, Optional
import itertools
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .engine import BacktestEngine
from ..strategy.base import BaseStrategy
from ..config import CONFIG


class ParameterOptimizer:
    """参数优化器"""
    
    def __init__(
        self,
        strategy_class: type,
        stock_code: str,
        start_date: str,
        end_date: str,
        metric: str = "total_return_pct",
    ):
        """
        Args:
            strategy_class: 策略类
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            metric: 优化指标 ("total_return_pct", "sharpe_ratio", "win_rate")
        """
        self.strategy_class = strategy_class
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.metric = metric
        
        self.results: List[Dict] = []
        self.best_params: Optional[Dict] = None
        self.best_result: Optional[Any] = None
    
    def grid_search(
        self,
        param_grid: Dict[str, List],
        initial_capital: float = None,
        max_workers: int = 4,
        verbose: bool = True
    ) -> Dict:
        """
        网格搜索优化参数
        
        Args:
            param_grid: 参数网格，如 {"fast": [12, 15], "slow": [26, 30]}
            initial_capital: 初始资金
            max_workers: 并行数量
            verbose: 是否显示进度
        
        Returns:
            最优参数
        """
        # 生成所有参数组合
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        combinations = list(itertools.product(*param_values))
        
        if verbose:
            print(f"Total combinations: {len(combinations)}")
        
        self.results = []
        
        # 并行回测
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            for combo in combinations:
                params = dict(zip(param_names, combo))
                
                future = executor.submit(
                    self._run_single_backtest,
                    params,
                    initial_capital
                )
                futures[future] = params
            
            if verbose:
                futures_iter = tqdm(as_completed(futures), total=len(futures), desc="Optimizing")
            else:
                futures_iter = futures
            
            for future in futures_iter:
                try:
                    result = future.result()
                    if result:
                        self.results.append(result)
                except Exception as e:
                    pass
        
        # 找最优
        if self.results:
            # 按指标排序
            self.results.sort(key=lambda x: x[self.metric], reverse=True)
            
            self.best_params = self.results[0].copy()
            self.best_result = self.results[0]
            
            # 移除回测结果，只保留参数
            for key in ['stock_code', 'start_date', 'end_date', 'initial_capital',
                       'total_return', 'total_return_pct', 'annual_return', 
                       'sharpe_ratio', 'max_drawdown', 'win_rate', 'trade_count']:
                if key in self.best_params:
                    del self.best_params[key]
        
        if verbose and self.best_params:
            print(f"\nBest params: {self.best_params}")
            print(f"Best {self.metric}: {self.best_result[self.metric]:.4f}")
        
        return self.best_params or {}
    
    def _run_single_backtest(
        self,
        params: Dict,
        initial_capital: float = None
    ) -> Optional[Dict]:
        """运行单次回测"""
        try:
            # 创建策略实例
            strategy = self.strategy_class(**params)
            
            # 创建回测引擎
            engine = BacktestEngine(
                strategy=strategy,
                initial_capital=initial_capital or CONFIG["default_initial_capital"]
            )
            
            # 加载数据
            if not engine.load_data(
                self.stock_code,
                self.start_date,
                self.end_date
            ):
                return None
            
            # 运行回测
            result = engine.run(verbose=False)
            
            # 合并参数和结果
            return {
                **params,
                "stock_code": result.stock_code,
                "start_date": result.start_date,
                "end_date": result.end_date,
                "initial_capital": result.initial_capital,
                "total_return": result.total_return,
                "total_return_pct": result.total_return_pct,
                "annual_return": result.annual_return,
                "sharpe_ratio": result.sharpe_ratio,
                "max_drawdown": result.max_drawdown,
                "win_rate": result.win_rate,
                "trade_count": result.trade_count,
            }
            
        except Exception as e:
            return None
    
    def get_results_df(self) -> pd.DataFrame:
        """获取所有结果"""
        if not self.results:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.results)
        
        # 排序
        if self.metric in df.columns:
            df = df.sort_values(self.metric, ascending=False)
        
        return df
    
    def get_top_n(self, n: int = 10) -> pd.DataFrame:
        """获取Top N结果"""
        df = self.get_results_df()
        return df.head(n)
    
    def plot_optimization_results(self, x_param: str, save_path: str = None):
        """绘制优化结果"""
        try:
            import matplotlib.pyplot as plt
            
            df = self.get_results_df()
            if df.empty:
                return
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            
            # 收益率
            axes[0, 0].bar(range(len(df)), df['total_return_pct'])
            axes[0, 0].set_title('Total Return %')
            axes[0, 0].set_xlabel('Parameter Combination')
            axes[0, 0].set_ylabel('Return')
            
            # 夏普比率
            axes[0, 1].bar(range(len(df)), df['sharpe_ratio'])
            axes[0, 1].set_title('Sharpe Ratio')
            axes[0, 1].set_xlabel('Parameter Combination')
            axes[0, 1].set_ylabel('Sharpe')
            
            # 最大回撤
            axes[1, 0].bar(range(len(df)), df['max_drawdown'])
            axes[1, 0].set_title('Max Drawdown')
            axes[1, 0].set_xlabel('Parameter Combination')
            axes[1, 0].set_ylabel('Drawdown')
            
            # 交易次数
            axes[1, 1].bar(range(len(df)), df['trade_count'])
            axes[1, 1].set_title('Trade Count')
            axes[1, 1].set_xlabel('Parameter Combination')
            axes[1, 1].set_ylabel('Trades')
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
            else:
                plt.show()
                
        except ImportError:
            print("matplotlib not installed")


class WalkForwardOptimizer:
    """ Walk-Forward 优化器 """
    
    def __init__(
        self,
        strategy_class: type,
        stock_code: str,
        start_date: str,
        end_date: str,
        train_period: int = 250,  # 训练期天数
        test_period: int = 50,    # 测试期天数
    ):
        self.strategy_class = strategy_class
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.train_period = train_period
        self.test_period = test_period
        
        self.results: List[Dict] = []
    
    def run(self, param_grid: Dict[str, List]) -> List[Dict]:
        """运行 Walk-Forward 优化"""
        
        # 简单实现：只做一次train-test分割
        # 实际应该滚动窗口
        
        from datetime import datetime, timedelta
        
        start = datetime.strptime(self.start_date, '%Y%m%d')
        train_end = start + timedelta(days=self.train_period)
        test_end = train_end + timedelta(days=self.test_period)
        
        train_start_str = start.strftime('%Y%m%d')
        train_end_str = train_end.strftime('%Y%m%d')
        test_end_str = test_end.strftime('%Y%m%d')
        
        optimizer = ParameterOptimizer(
            self.strategy_class,
            self.stock_code,
            train_start_str,
            train_end_str
        )
        
        best_params = optimizer.grid_search(param_grid, verbose=True)
        
        # 用最优参数在测试集上回测
        strategy = self.strategy_class(**best_params)
        engine = BacktestEngine(strategy=strategy)
        
        if engine.load_data(self.stock_code, train_end_str, test_end_str):
            result = engine.run(verbose=False)
            
            self.results.append({
                "train_period": f"{train_start_str}-{train_end_str}",
                "test_period": f"{train_end_str}-{test_end_str}",
                "best_params": best_params,
                "test_return": result.total_return_pct,
                "test_sharpe": result.sharpe_ratio,
                "test_max_dd": result.max_drawdown,
            })
        
        return self.results