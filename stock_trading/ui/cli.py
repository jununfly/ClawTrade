"""
命令行界面模块
"""

import sys
import argparse
from typing import Optional
from ..data.fetcher import StockFetcher
from ..market.analyzer import MarketAnalyzer
from ..portfolio.manager import PortfolioManager
from ..portfolio.analytics import PortfolioAnalytics
from ..backtest.engine import BacktestEngine
from ..strategy.macd import MACDStrategy
from ..strategy.mean_reversion import MeanReversionStrategy
from ..config import CONFIG


class CLI:
    """命令行界面"""
    
    def __init__(self):
        self.fetcher = StockFetcher()
        self.analyzer = MarketAnalyzer()
        self.portfolio = PortfolioManager()
        self.analytics = PortfolioAnalytics(self.portfolio)
    
    def run(self, args: list = None):
        """运行CLI"""
        parser = self._create_parser()
        parsed = parser.parse_args(args)
        
        if hasattr(parsed, 'func'):
            parsed.func(parsed)
        else:
            parser.print_help()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description='股票交易系统 CLI',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        subparsers = parser.add_subparsers(title='命令', dest='command')
        
        # price 命令
        price_parser = subparsers.add_parser('price', help='查询股票价格')
        price_parser.add_argument('stock_code', help='股票代码')
        price_parser.set_defaults(func=self.cmd_price)
        
        # market 命令
        market_parser = subparsers.add_parser('market', help='查看市场行情')
        market_parser.set_defaults(func=self.cmd_market)
        
        # buy 命令
        buy_parser = subparsers.add_parser('buy', help='买入股票')
        buy_parser.add_argument('stock_code', help='股票代码')
        buy_parser.add_argument('--price', type=float, required=True, help='买入价格')
        buy_parser.add_argument('--quantity', type=int, required=True, help='买入股数')
        buy_parser.add_argument('--date', help='交易日期')
        buy_parser.set_defaults(func=self.cmd_buy)
        
        # sell 命令
        sell_parser = subparsers.add_parser('sell', help='卖出股票')
        sell_parser.add_argument('stock_code', help='股票代码')
        sell_parser.add_argument('--price', type=float, required=True, help='卖出价格')
        sell_parser.add_argument('--quantity', type=int, required=True, help='卖出股数')
        sell_parser.add_argument('--date', help='交易日期')
        sell_parser.set_defaults(func=self.cmd_sell)
        
        # portfolio 命令
        portfolio_parser = subparsers.add_parser('portfolio', help='查看持仓')
        portfolio_parser.set_defaults(func=self.cmd_portfolio)
        
        # trades 命令
        trades_parser = subparsers.add_parser('trades', help='查看交易记录')
        trades_parser.set_defaults(func=self.cmd_trades)
        
        # backtest 命令
        bt_parser = subparsers.add_parser('backtest', help='回测')
        bt_parser.add_argument('--code', '--stock', dest='stock_code', required=True, help='股票代码')
        bt_parser.add_argument('--start', dest='start_date', required=True, help='开始日期 YYYYMMDD')
        bt_parser.add_argument('--end', dest='end_date', required=True, help='结束日期 YYYYMMDD')
        bt_parser.add_argument('--strategy', default='macd', choices=['macd', 'mean_reversion'], help='策略')
        bt_parser.add_argument('--capital', type=float, help='初始资金')
        bt_parser.set_defaults(func=self.cmd_backtest)
        
        # info 命令
        info_parser = subparsers.add_parser('info', help='股票信息')
        info_parser.add_argument('stock_code', help='股票代码')
        info_parser.set_defaults(func=self.cmd_info)
        
        return parser
    
    def cmd_price(self, args):
        """查询价格"""
        stock_code = args.stock_code
        data = self.fetcher.get_realtime(stock_code)
        
        if data:
            print(f"\n{data.get('name', stock_code)} ({stock_code})")
            print(f"最新价: {data.get('price', 'N/A')}")
            print(f"涨跌幅: {data.get('change', 'N/A')}%")
            print(f"最高: {data.get('high', 'N/A')}")
            print(f"最低: {data.get('low', 'N/A')}")
            print(f"成交量: {data.get('volume', 'N/A')}")
            print(f"成交额: {data.get('amount', 'N/A')}")
        else:
            print(f"未找到股票 {stock_code} 的数据")
    
    def cmd_market(self, args):
        """市场行情"""
        overview = self.analyzer.get_market_overview()
        
        print("\n=== 市场概览 ===")
        print(f"股票总数: {overview.get('total_stocks', 0)}")
        print(f"上涨: {overview.get('up_count', 0)}")
        print(f"下跌: {overview.get('down_count', 0)}")
        print(f"平盘: {overview.get('flat_count', 0)}")
        print(f"平均涨跌幅: {overview.get('avg_change', 0):.2f}%")
        print(f"总成交额: {overview.get('total_amount', 0):,.0f}")
    
    def cmd_buy(self, args):
        """买入"""
        success = self.portfolio.buy(
            args.stock_code,
            args.price,
            args.quantity,
            args.date
        )
        
        if success:
            print(f"\n✅ 买入成功!")
            print(f"股票: {args.stock_code}")
            print(f"价格: {args.price}")
            print(f"股数: {args.quantity}")
            print(f"总额: {args.price * args.quantity:.2f}")
            print(f"现金余额: {self.portfolio.cash:.2f}")
        else:
            print("\n❌ 买入失败 - 资金不足")
    
    def cmd_sell(self, args):
        """卖出"""
        success = self.portfolio.sell(
            args.stock_code,
            args.price,
            args.quantity,
            args.date
        )
        
        if success:
            print(f"\n✅ 卖出成功!")
            print(f"股票: {args.stock_code}")
            print(f"价格: {args.price}")
            print(f"股数: {args.quantity}")
            print(f"总额: {args.price * args.quantity:.2f}")
            print(f"现金余额: {self.portfolio.cash:.2f}")
        else:
            print("\n❌ 卖出失败 - 持仓不足")
    
    def cmd_portfolio(self, args):
        """查看持仓"""
        summary = self.portfolio.get_summary()
        
        print("\n=== 账户概览 ===")
        print(f"初始资金: {summary['initial_cash']:,.2f}")
        print(f"当前现金: {summary['cash']:,.2f}")
        print(f"持仓成本: {summary['total_cost']:,.2f}")
        print(f"持仓数量: {summary['position_count']}")
        print(f"交易次数: {summary['trade_count']}")
        
        positions = self.portfolio.get_positions()
        if not positions.empty:
            print("\n=== 持仓明细 ===")
            print(f"{'代码':<10} {'股数':<8} {'成本':<12} {'总成本':<15}")
            print("-" * 50)
            for _, row in positions.iterrows():
                print(f"{row['stock_code']:<10} {row['quantity']:<8} {row['avg_cost']:<12.2f} {row['total_cost']:<15.2f}")
        else:
            print("\n暂无持仓")
    
    def cmd_trades(self, args):
        """查看交易记录"""
        trades = self.portfolio.get_trades()
        
        if not trades.empty:
            print("\n=== 交易记录 ===")
            print(f"{'日期':<12} {'代码':<10} {'操作':<6} {'价格':<10} {'股数':<8}")
            print("-" * 55)
            for _, row in trades.iterrows():
                action = "买入" if row['action'] == 'BUY' else "卖出"
                print(f"{row['date']:<12} {row['stock_code']:<10} {action:<6} {row['price']:<10.2f} {row['quantity']:<8}")
        else:
            print("\n暂无交易记录")
    
    def cmd_backtest(self, args):
        """回测"""
        print(f"\n回测 {args.stock_code} ({args.start_date} - {args.end_date})")
        print(f"策略: {args.strategy}")
        print("-" * 50)
        
        # 选择策略
        if args.strategy == 'macd':
            strategy = MACDStrategy()
        else:
            strategy = MeanReversionStrategy()
        
        # 创建回测引擎
        engine = BacktestEngine(
            strategy=strategy,
            initial_capital=args.capital or CONFIG["default_initial_capital"]
        )
        
        # 加载数据
        if not engine.load_data(args.stock_code, args.start_date, args.end_date):
            print("❌ 数据加载失败")
            return
        
        # 运行回测
        result = engine.run(verbose=False)
        print(result)
    
    def cmd_info(self, args):
        """股票信息"""
        stock_code = args.stock_code
        
        # 实时行情
        data = self.fetcher.get_realtime(stock_code)
        
        if not data:
            print(f"未找到股票 {stock_code} 的数据")
            return
        
        print(f"\n=== {data.get('name', stock_code)} ({stock_code}) ===")
        print(f"最新价: {data.get('price', 'N/A')}")
        print(f"涨跌幅: {data.get('change', 'N/A')}%")
        print(f"今开: {data.get('open', 'N/A')}")
        print(f"昨收: {data.get('close', 'N/A')}")
        print(f"最高: {data.get('high', 'N/A')}")
        print(f"最低: {data.get('low', 'N/A')}")
        print(f"成交量: {data.get('volume', 'N/A')}")
        print(f"成交额: {data.get('amount', 'N/A')}")
        
        # 资金流向
        flow = self.fetcher.get_fund_flow(stock_code)
        if flow:
            print(f"\n资金流向:")
            print(f"主力净流入: {flow.get('main_inflow', 'N/A')}")
            print(f"主力净流入占比: {flow.get('main_inflow_pct', 'N/A')}")


def main():
    """CLI入口"""
    cli = CLI()
    cli.run()


if __name__ == '__main__':
    main()