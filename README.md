# ClawTrade 股票交易系统

基于 Python 的本地量化投资工具，支持策略生成、市场分析、持仓复盘、回测验证。

## 一、项目结构

```
ClawTrade/
├── main.py                     # 主入口文件
├── requirements.txt            # 依赖列表
├── stock_trading/              # 核心模块
│   ├── __init__.py
│   ├── config.py               # 配置文件
│   ├── data/                   # 数据层
│   │   ├── __init__.py
│   │   ├── fetcher.py          # 数据获取 (AkShare)
│   │   └── storage.py          # 数据存储 (SQLite)
│   ├── strategy/               # 策略模块
│   │   ├── __init__.py
│   │   ├── base.py             # 策略基类
│   │   ├── macd.py             # MACD 策略
│   │   └── mean_reversion.py   # 均值回归策略
│   ├── market/                 # 市场分析模块
│   │   ├── __init__.py
│   │   ├── analyzer.py         # 市场分析
│   │   └── flow.py             # 资金流向分析
│   ├── portfolio/              # 持仓管理模块
│   │   ├── __init__.py
│   │   ├── manager.py          # 持仓管理
│   │   └── analytics.py        # 持仓分析
│   ├── backtest/               # 回测模块
│   │   ├── __init__.py
│   │   ├── engine.py           # 回测引擎
│   │   └── optimizer.py        # 参数优化
│   └── ui/                     # 界面模块
│       ├── __init__.py
│       ├── web.py              # Web 界面 (Flask)
│       └── cli.py              # 命令行界面
├── data/                       # 数据目录
│   ├── cache/                  # 缓存
│   └── exports/                # 导出
└── logs/                       # 日志目录
```

## 二、系统运行的主要流程

### 2.1 命令行模式

```bash
# 查询股票价格
python main.py price 600519

# 买入股票
python main.py buy 600519 --price 1700 --quantity 100

# 卖出股票
python main.py sell 600519 --price 1750 --quantity 50

# 查看持仓
python main.py portfolio

# 策略回测
python main.py backtest --code 600519 --start 20230101 --end 20231231
```

### 2.2 Web 界面模式

```bash
# 启动 Web 服务
python main.py web --port 5000
# 访问 http://localhost:5000
```

### 2.3 核心模块调用流程

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   UI 层     │ ──▶ │   策略层    │ ──▶ │   数据层    │
│ (CLI/Web)   │     │ (MACD/均值) │     │ (AkShare)   │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   回测引擎   │ ──▶ │   持仓管理   │
                    │  (Backtest) │     │ (Portfolio) │
                    └─────────────┘     └─────────────┘
```

### 2.4 回测流程

1. **加载数据** → `StockFetcher.get_kline()` 获取历史K线
2. **生成信号** → 策略 `generate_signals()` 产生买入/卖出信号
3. **模拟交易** → `BacktestEngine.run()` 按信号执行交易
4. **计算结果** → 输出收益率、夏普比率、最大回撤等指标

## 三、本次更新主要修改的内容

### v2.0 更新 (2026-03-17)

本次更新实现了完整的单机版股票交易系统，主要包含：

#### 1. 数据层 (`stock_trading/data/`)
- `fetcher.py`: 集成 AkShare 获取实时行情、K线、资金流向
- `storage.py`: SQLite 数据持久化
- `__init__.py`: 模块导出

#### 2. 策略层 (`stock_trading/strategy/`)
- `base.py`: 抽象策略基类，定义信号生成接口
- `macd.py`: MACD 指标策略实现
- `mean_reversion.py`: 均值回归策略实现

#### 3. 市场分析层 (`stock_trading/market/`)
- `analyzer.py`: 市场概览、板块分析、个股查询
- `flow.py`: 资金流向分析、主力资金追踪

#### 4. 持仓管理层 (`stock_trading/portfolio/`)
- `manager.py`: 买入/卖出、持仓记录、交易流水
- `analytics.py`: 绩效分析、风险指标、报告生成

#### 5. 回测引擎 (`stock_trading/backtest/`)
- `engine.py`: 完整回测引擎，支持滑点、手续费设置
- `optimizer.py`: 参数网格搜索优化、Walk-Forward 分析

#### 6. 用户界面 (`stock_trading/ui/`)
- `web.py`: Flask Web 界面 (Dashboard/行情/持仓/回测)
- `cli.py`: 命令行工具，支持 price/buy/sell/portfolio/backtest

#### 7. 入口文件 (`main.py`)
- 统一命令行入口，支持多种运行模式
- 修复 Python 3.6 兼容性问题

### 修复的问题

- ✅ 修复 `data/fetcher.py` 导入路径错误
- ✅ 修复 `data/__init__.py` 类名不匹配
- ✅ 修复 Python 3.6 `argparse` 兼容性问题

## 四、依赖安装

```bash
pip install -r requirements.txt
```

主要依赖：
- `akshare` - 股票数据获取
- `pandas` - 数据处理
- `numpy` - 数值计算
- `flask` - Web 框架
- `matplotlib` - 图表绘制

## 五、注意事项

- 本系统仅供研究学习，不构成投资建议
- 回测结果 ≠ 实盘收益，需考虑滑点、手续费等因素
- 数据来源于 AkShare，需保持网络连接

---

**版本**: v2.0  
**更新日期**: 2026-03-17