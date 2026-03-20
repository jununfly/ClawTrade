# ClawTrade 股票交易系统

> 简化版：每日收盘后选股 + 策略回测 | CLI only

## 一、系统定位

### 核心功能：
- 每日收盘后选股
- 选股策略回测验证

### 不做：
- Web 界面
- 持仓管理
- 实时交易

---

## 二、系统架构

```
┌─────────────────────────────────────────┐
│            应用层 (Python CLI)          │
├─────────────────────────────────────────┤
│   ┌─────────────┐    ┌─────────────┐   │
│   │   选股器    │    │   回测引擎   │   │
│   └─────────────┘    └─────────────┘   │
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│                 数据层                  │
│                                         │
│   ┌──────────┐    ┌──────────┐         │
│   │ SQLite   │    │  AkShare  │         │
│   │ (本地存储)│    │ (行情数据) │         │
│   └──────────┘    └──────────┘         │
└─────────────────────────────────────────┘
```

---

## 三、核心功能

### 3.1 数据获取器 (Data Fetcher)

| 功能 | 说明 |
|------|------|
| 多数据源 | 支持 AkShare / Tushare |
| 分批获取 | 每批100只，批间等待避免限流 |
| 自动重试 | 失败后指数退避重试（2s→4s→8s） |
| 增量更新 | 仅获取最近N天数据 |
| 详细日志 | 错误分类显示，便于问题排查 |

### 3.2 选股器 (Stock Selector)

| 功能 | 说明 |
|------|------|
| B1策略选股 | 基于KDJ、知行线、周线均线的选股策略 |
| 砖型图选股 | 基于砖型图形态的选股策略 |

### 3.3 回测引擎 (Backtest Engine)

| 功能 | 说明 |
|------|------|
| 历史回测 | 基于历史数据回测策略 |
| 绩效分析 | 夏普比率、最大回撤、胜率 |

---

## 四、技术栈

| 组件 | 技术 |
|------|------|
| 语言 | Python 3.8+ |
| 数据获取 | AkShare / Tushare |
| 数据处理 | Pandas, NumPy, Numba |
| 回测框架 | 自研 |
| 数据库 | SQLite |

---

## 五、项目结构

```
ClawTrade/
├── main.py                 # CLI 入口
├── config.py               # 配置文件
├── requirements.txt        # 依赖
├── stock_picker/
│   ├── selector/           # 选股模块
│   │   ├── base.py         # 选股基类
│   │   ├── b1_selector.py  # B1策略选股器
│   │   └── brick_selector.py  # 砖型图选股器
│   ├── data/               # 数据模块
│   │   ├── fetcher.py      # 数据获取
│   │   └── storage.py      # 数据存储
│   ├── backtest/           # 回测模块
│   │   └── engine.py       # 回测引擎
│   └── utils/              # 工具模块
│       ├── indicators.py  # 技术指标
│       └── logger.py      # 日志工具
└── data/                   # 数据目录
```

---

## 六、快速开始

### 6.1 安装依赖

```bash
pip install -r requirements.txt
```

### 6.2 环境配置

```bash
# 设置Tushare Token（可选）
export TUSHARE_TOKEN=你的token

# 设置Gemini API Key（可选）
export GEMINI_API_KEY=你的key
```

### 6.3 数据获取器配置

DataFetcher 支持多种参数配置：

```python
from stock_picker.data.fetcher import DataFetcher

fetcher = DataFetcher(
    data_source="akshare",   # 数据源: akshare 或 tushare
    data_dir="data/raw",     # 数据存储目录
    request_delay=1.0,       # 请求间隔（秒）
    batch_size=100,          # 每批股票数量
    batch_delay=60,          # 每批之间等待秒数
    max_retries=3,          # 最大重试次数
    base_delay=2.0,         # 指数退避基础时间
)
```

### 6.4 使用方式

```bash
# 查看帮助
python main.py --help

# 获取数据（自动分批获取，避免被限流）
python main.py fetch --codes 600519,000001 --start 20240101

# 增量更新最近30天数据
python main.py fetch --update --days 30

# 选股
python main.py select --strategy b1 --date 20240315

# 回测
python main.py backtest --stocks 600519,000001 --start 20230101 --end 20231231

# 查看结果
python main.py result --type candidates
```

---

## 七、命令行功能

### 7.1 获取数据

```bash
# 获取指定股票
python main.py fetch --codes 600519,000001

# 获取所有A股（默认排除创业板、科创板、北交所）
python main.py fetch --start 20240101
```

### 7.2 选股

```bash
# B1策略选股
python main.py select --strategy b1

# 砖型图策略选股
python main.py select --strategy brick

# 指定日期
python main.py select --strategy b1 --date 20240315
```

### 7.3 回测

```bash
# 基础回测
python main.py backtest --stocks 600519

# 自定义参数
python main.py backtest --stocks 600519 --commission 0.0003 --start 20230101
```

---

## 八、选股策略

### B1策略

B1策略由以下四个Filter组成：
1. **KDJQuantileFilter** - J值低位（J<15或历史10%分位）
2. **ZXConditionFilter** - 知行线条件（close>zxdkx 且 zxdq>zxdkx）
3. **WeeklyMABullFilter** - 周线多头排列
4. **MaxVolNotBearishFilter** - 近20日成交量最大日非阴线

### 砖型图策略

砖型图策略基于砖型图形态进行选股：
1. **BrickPatternFilter** - 砖型图形态（红柱/绿柱 + 涨幅 + 连续绿柱数）
2. **ZXDQRatioFilter** - close < zxdq × ratio
3. **ZXConditionFilter** - zxdq > zxdkx
4. **WeeklyMABullFilter** - 周线多头排列

---

## 九、评分引擎

### 评分体系（总分110分）

| 评分维度 | 分值 | 评分项 |
|----------|------|--------|
| 趋势分 | 45分 | QXYQ(35分) + MA20趋势向上(10分) |
| 超卖分 | 25分 | KDJ J值≤16(15分) + CCI超卖(10分) |
| 成交量分 | 20分 | 缩量信号 |
| 结构分 | 20分 | N型结构(8分) + 假突破(6分) + 活跃信号(6分) |
| 额外分 | 10分 | CCI底背离 |

### 买入信号判断

硬条件检查：
1. KDJ J值 ≤ 16
2. 总评分 ≥ 70
3. VK_RECENT == 1（有底部放量信号）
4. TOTAL_RISK == 0（无风险标记）

### 使用示例

```python
from stock_picker.utils.tdx_engine import evaluate_df, check_buy_signal

# 评估整张表
df = evaluate_df(df)
last = df.iloc[-1]
signal = df.attrs['last_buy_signal']
print(f"TOTAL_SCORE: {last['TOTAL_SCORE']}")
print(f"BUY_SIGNAL: {signal['is_buy']}")
```

---

## 九、配置说明

在 `config.py` 中可以修改以下配置：

```python
# 数据源
DATA_SOURCE = "akshare"  # akshare 或 tushare

# 选股参数
MIN_VOLUME = 100000000  # 最小成交额

# 流动性池配置
TOP_M = 5000  # 取成交额最高的股票数量

# B1策略参数
B1_CONFIG = {
    "enabled": True,
    "j_threshold": 15.0,
    "j_q_threshold": 0.10,
}

# 砖型图策略参数
BRICK_CONFIG = {
    "enabled": False,
    "n": 8,
    "daily_return_threshold": 0.2,
}
```

---

## 十、注意事项

- 数据仅供研究，不构成投资建议
- 回测不等于实盘
- 需要网络连接获取数据
- 建议定期备份数据目录

---

## 版本

- **版本**: v3.1
- **更新**: 2026-03-20
- **更新内容**: 优化数据获取器，新增分批获取、自动重试、详细日志功能