#!/usr/bin/env python3
"""
股票交易系统 - 主入口
"""

import sys
import argparse
from stock_trading.config import CONFIG


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description='股票交易系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # Web 界面
  python main.py web --port 5000

  # 命令行
  python main.py price 600519
  python main.py buy 600519 --price 1700 --quantity 100
  python main.py sell 600519 --price 1750 --quantity 50
  python main.py portfolio
  python main.py trades
  
  # 回测
  python main.py backtest --code 600519 --start 20230101 --end 20231231
  python main.py backtest --code 600519 --start 20230101 --end 20231231 --strategy mean_reversion
        """
    )
    
    parser.add_argument('--version', action='version', version='%(prog)s 2.0')
    
    subparsers = parser.add_subparsers(title='运行模式', dest='mode')
    
    # Web 模式
    web_parser = subparsers.add_parser('web', help='启动 Web 界面')
    web_parser.add_argument('--host', default='0.0.0.0', help='监听地址')
    web_parser.add_argument('--port', type=int, default=5000, help='监听端口')
    web_parser.add_argument('--debug', action='store_true', help='调试模式')
    web_parser.set_defaults(func=run_web)
    
    # CLI 模式
    cli_parser = subparsers.add_parser('cli', help='命令行界面')
    cli_parser.add_argument('args', nargs='*', help='CLI 参数')
    cli_parser.set_defaults(func=run_cli)
    
    # 回测模式
    bt_parser = subparsers.add_parser('backtest', help='策略回测')
    bt_parser.add_argument('--code', '--stock', dest='stock_code', required=True, help='股票代码')
    bt_parser.add_argument('--start', dest='start_date', required=True, help='开始日期')
    bt_parser.add_argument('--end', dest='end_date', required=True, help='结束日期')
    bt_parser.add_argument('--strategy', default='macd', help='策略 (macd/mean_reversion)')
    bt_parser.add_argument('--capital', type=float, help='初始资金')
    bt_parser.add_argument('--plot', action='store_true', help='绘制图表')
    bt_parser.set_defaults(func=run_backtest)
    
    # 价格查询
    price_parser = subparsers.add_parser('price', help='查询股票价格')
    price_parser.add_argument('stock_code', help='股票代码')
    price_parser.set_defaults(func=run_price)
    
    # 买入
    buy_parser = subparsers.add_parser('buy', help='买入股票')
    buy_parser.add_argument('stock_code', help='股票代码')
    buy_parser.add_argument('--price', type=float, required=True, help='价格')
    buy_parser.add_argument('--quantity', type=int, required=True, help='股数')
    buy_parser.set_defaults(func=run_buy)
    
    # 卖出
    sell_parser = subparsers.add_parser('sell', help='卖出股票')
    sell_parser.add_argument('stock_code', help='股票代码')
    sell_parser.add_argument('--price', type=float, required=True, help='价格')
    sell_parser.add_argument('--quantity', type=int, required=True, help='股数')
    sell_parser.set_defaults(func=run_sell)
    
    # 持仓
    portfolio_parser = subparsers.add_parser('portfolio', help='查看持仓')
    portfolio_parser.set_defaults(func=run_portfolio)
    
    args = parser.parse_args()
    args.func(args)


def run_web(args):
    """启动Web服务"""
    from stock_trading.ui.web import run
    print(f"启动 Web 服务: http://{args.host}:{args.port}")
    run(host=args.host, port=args.port, debug=args.debug)


def run_cli(args):
    """运行命令行"""
    from stock_trading.ui.cli import CLI
    cli = CLI()
    cli.run(args.args)


def run_backtest(args):
    """运行回测"""
    from stock_trading.backtest.engine import BacktestEngine
    from stock_trading.strategy.macd import MACDStrategy
    from stock_trading.strategy.mean_reversion import MeanReversionStrategy
    
    print(f"\n{'='*50}")
    print(f"回测: {args.stock_code}")
    print(f"时间: {args.start_date} ~ {args.end_date}")
    print(f"策略: {args.strategy}")
    print(f"{'='*50}\n")
    
    # 策略选择
    if args.strategy == 'macd':
        strategy = MACDStrategy()
    elif args.strategy == 'mean_reversion':
        strategy = MeanReversionStrategy()
    else:
        print(f"未知策略: {args.strategy}")
        return
    
    # 创建引擎
    engine = BacktestEngine(
        strategy=strategy,
        initial_capital=args.capital or CONFIG["default_initial_capital"]
    )
    
    # 加载数据
    if not engine.load_data(args.stock_code, args.start_date, args.end_date):
        print("❌ 数据加载失败")
        return
    
    # 运行
    result = engine.run(verbose=True)
    
    # 绘图
    if args.plot:
        engine.plot_results()


def run_price(args):
    """查询价格"""
    from stock_trading.data.fetcher import StockFetcher
    
    fetcher = StockFetcher()
    data = fetcher.get_realtime(args.stock_code)
    
    if data:
        print(f"\n{data.get('name', args.stock_code)} ({args.stock_code})")
        print(f"最新价: {data.get('price', 'N/A')}")
        print(f"涨跌幅: {data.get('change', 'N/A')}%")
    else:
        print(f"未找到 {args.stock_code}")


def run_buy(args):
    """买入"""
    from stock_trading.portfolio.manager import PortfolioManager
    
    pm = PortfolioManager()
    success = pm.buy(args.stock_code, args.price, args.quantity)
    
    if success:
        print(f"✅ 买入成功: {args.stock_code} x {args.quantity} @ {args.price}")
    else:
        print("❌ 买入失败")


def run_sell(args):
    """卖出"""
    from stock_trading.portfolio.manager import PortfolioManager
    
    pm = PortfolioManager()
    success = pm.sell(args.stock_code, args.price, args.quantity)
    
    if success:
        print(f"✅ 卖出成功: {args.stock_code} x {args.quantity} @ {args.price}")
    else:
        print("❌ 卖出失败")


def run_portfolio(args):
    """查看持仓"""
    from stock_trading.portfolio.manager import PortfolioManager
    
    pm = PortfolioManager()
    positions = pm.get_positions()
    
    print(f"\n{'='*50}")
    print(f"现金: {pm.cash:,.2f}")
    print(f"{'='*50}")
    
    if not positions.empty:
        print(f"\n{'代码':<10} {'股数':<8} {'成本':<12} {'总成本':<15}")
        print("-" * 50)
        for _, row in positions.iterrows():
            print(f"{row['stock_code']:<10} {row['quantity']:<8} {row['avg_cost']:<12.2f} {row['total_cost']:<15.2f}")
    else:
        print("\n暂无持仓")


if __name__ == '__main__':
    main()