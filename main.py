# -*- coding: utf-8 -*-
"""
ClawTrade CLI 入口
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

import config
from stock_picker.selector.b1_selector import B1Selector
from stock_picker.selector.brick_selector import BrickChartSelector
from stock_picker.data.fetcher import DataFetcher
from stock_picker.data.storage import DataStorage
from stock_picker.utils.logger import setup_logger


logger = setup_logger()


def cmd_fetch(args):
    """获取数据"""
    logger.info("开始获取数据...")
    
    fetcher = DataFetcher(
        data_source=config.DATA_SOURCE,
        data_dir=config.RAW_DATA_DIR,
    )
    
    codes = None
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    
    fetcher.fetch_all(
        codes=codes,
        start=args.start,
        end=args.end,
        workers=args.workers,
    )
    
    logger.info("数据获取完成")


def cmd_select(args):
    """选股"""
    logger.info("开始选股...")
    
    # 加载数据
    fetcher = DataFetcher(data_dir=config.RAW_DATA_DIR)
    data = fetcher.load_all_csv()
    
    if not data:
        logger.error("未找到数据，请先运行 fetch 命令")
        return
    
    # 选择选股器
    if args.strategy == "b1":
        selector = B1Selector(
            j_threshold=float(args.j_threshold) if args.j_threshold else 15.0,
            j_q_threshold=0.10,
        )
    elif args.strategy == "brick":
        selector = BrickChartSelector()
    else:
        logger.error(f"未知策略: {args.strategy}")
        return
    
    # 选股日期
    pick_date = pd.Timestamp(args.date) if args.date else pd.Timestamp.now()
    
    # 执行选股
    candidates = []
    for code, df in data.items():
        try:
            pf = selector.prepare_df(df)
            if selector.vec_picks_from_prepared(pf, start=pick_date, end=pick_date):
                row = pf.loc[pick_date]
                candidates.append({
                    "code": code,
                    "date": pick_date.strftime("%Y-%m-%d"),
                    "strategy": args.strategy,
                    "close": float(row["close"]),
                    "turnover_n": float(row["turnover_n"]) if "turnover_n" in row else 0,
                })
        except Exception as e:
            logger.debug(f"{code} 选股失败: {e}")
    
    # 保存结果
    storage = DataStorage(config.DATABASE_PATH)
    storage.save_candidates(candidates)
    
    # 打印结果
    print(f"\n选出 {len(candidates)} 只股票:")
    for c in candidates:
        print(f"  {c['code']} - {c['strategy']} - 收盘价: {c['close']:.2f}")
    
    logger.info("选股完成")


def cmd_backtest(args):
    """回测"""
    logger.info("开始回测...")
    
    from stock_picker.backtest import BacktestEngine
    from stock_picker.data.fetcher import DataFetcher
    
    # 获取数据
    fetcher = DataFetcher(data_dir=config.RAW_DATA_DIR)
    
    codes = args.stocks.split(",") if args.stocks else ["600519"]
    
    data = {}
    for code in codes:
        df = fetcher.load_csv(code.strip())
        if not df.empty:
            data[code.strip()] = df
    
    if not data:
        logger.error("未找到数据")
        return
    
    # 创建信号（这里简化处理，实际应根据策略生成）
    signals = {}
    for code in data:
        # 取最近几天作为信号
        dates = data[code]["date"].astype(str).tolist()[-5:]
        for d in dates:
            if d not in signals:
                signals[d] = []
            signals[d].append(code)
    
    # 运行回测
    engine = BacktestEngine(
        initial_capital=config.DEFAULT_INITIAL_CAPITAL,
        commission=args.commission if args.commission else config.DEFAULT_COMMISSION,
    )
    
    result = engine.run(
        data=data,
        signals=signals,
        start_date=args.start,
        end_date=args.end,
    )
    
    engine.print_result(result)
    
    logger.info("回测完成")


def cmd_result(args):
    """查看选股结果"""
    storage = DataStorage(config.DATABASE_PATH)
    
    if args.type == "candidates":
        data = storage.load_candidates(args.date)
        print(f"\n候选股票 ({len(data)} 只):")
        for c in data:
            print(f"  {c['code']} - {c['strategy']} - {c['close']:.2f}")
    elif args.type == "recommendations":
        data = storage.load_recommendations(args.date)
        print(f"\n推荐股票 ({len(data)} 只):")
        for r in data:
            print(f"  {r['code']} - {r['strategy']} - 评分: {r['score']:.1f} - {r['verdict']}")


def main():
    parser = argparse.ArgumentParser(description="ClawTrade 股票交易系统")
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # fetch 子命令
    fetch_parser = subparsers.add_parser("fetch", help="获取股票数据")
    fetch_parser.add_argument("--codes", type=str, help="股票代码，逗号分隔")
    fetch_parser.add_argument("--start", type=str, default="20240101", help="开始日期")
    fetch_parser.add_argument("--end", type=str, help="结束日期")
    fetch_parser.add_argument("--workers", type=int, default=4, help="并发数")
    fetch_parser.set_defaults(func=cmd_fetch)
    
    # select 子命令
    select_parser = subparsers.add_parser("select", help="选股")
    select_parser.add_argument("--strategy", type=str, default="b1", choices=["b1", "brick"], help="策略")
    select_parser.add_argument("--date", type=str, help="选股日期")
    select_parser.add_argument("--j-threshold", type=str, help="J值阈值")
    select_parser.set_defaults(func=cmd_select)
    
    # backtest 子命令
    backtest_parser = subparsers.add_parser("backtest", help="回测")
    backtest_parser.add_argument("--stocks", type=str, help="股票代码，逗号分隔")
    backtest_parser.add_argument("--start", type=str, help="开始日期")
    backtest_parser.add_argument("--end", type=str, help="结束日期")
    backtest_parser.add_argument("--commission", type=float, help="手续费率")
    backtest_parser.set_defaults(func=cmd_backtest)
    
    # result 子命令
    result_parser = subparsers.add_parser("result", help="查看结果")
    result_parser.add_argument("--type", type=str, choices=["candidates", "recommendations"], default="candidates")
    result_parser.add_argument("--date", type=str, help="日期")
    result_parser.set_defaults(func=cmd_result)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    # 导入pandas（避免循环导入）
    global pd
    import pandas as pd
    
    args.func(args)


if __name__ == "__main__":
    main()