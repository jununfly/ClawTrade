"""
Web 界面模块
"""

import os
from flask import Flask, render_template_string, request, jsonify
from ..data.fetcher import StockFetcher
from ..market.analyzer import MarketAnalyzer
from ..portfolio.manager import PortfolioManager
from ..portfolio.analytics import PortfolioAnalytics
from ..backtest.engine import BacktestEngine
from ..config import CONFIG


app = Flask(__name__)
app.secret_key = 'stock_trading_secret'

# 全局状态
portfolio = PortfolioManager()
analyzer = MarketAnalyzer()


# HTML 模板
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>股票交易系统</title>
    <meta charset="utf-8">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
               background: #f5f5f5; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }
        .nav { display: flex; gap: 10px; margin-bottom: 20px; }
        .nav a { padding: 10px 20px; background: #007bff; color: white; text-decoration: none; 
                 border-radius: 5px; }
        .nav a:hover { background: #0056b3; }
        .card { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f8f9fa; font-weight: 600; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: 500; }
        input, select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        button { padding: 10px 20px; background: #28a745; color: white; border: none; 
                 border-radius: 4px; cursor: pointer; }
        button:hover { background: #218838; }
        .alert { padding: 15px; background: #d4edda; border: 1px solid #c3e6cb; 
                 border-radius: 4px; margin-bottom: 15px; }
        .error { background: #f8d7da; border-color: #f5c6cb; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 股票交易系统</h1>
        
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/market">行情监控</a>
            <a href="/portfolio">持仓管理</a>
            <a href="/backtest">回测中心</a>
        </div>
        
        {% if page == 'dashboard' %}
        <div class="card">
            <h2>📈 市场概览</h2>
            <div id="market-overview">加载中...</div>
        </div>
        
        <div class="card">
            <h2>💰 账户概览</h2>
            <div id="account-overview">加载中...</div>
        </div>
        
        {% elif page == 'market' %}
        <div class="card">
            <h2>🔍 股票查询</h2>
            <form action="/api/price" method="post">
                <div class="form-group">
                    <label>股票代码</label>
                    <input type="text" name="stock_code" placeholder="如: 600519" required>
                </div>
                <button type="submit">查询</button>
            </form>
            <div id="price-result"></div>
        </div>
        
        {% elif page == 'portfolio' %}
        <div class="card">
            <h2>📋 持仓明细</h2>
            <table id="positions-table">
                <thead>
                    <tr>
                        <th>股票代码</th>
                        <th>股数</th>
                        <th>成本</th>
                        <th>总成本</th>
                        <th>买入日期</th>
                    </tr>
                </thead>
                <tbody id="positions-body"></tbody>
            </table>
        </div>
        
        <div class="card">
            <h2>➕ 买入/卖出</h2>
            <form action="/api/trade" method="post">
                <div class="form-group">
                    <label>股票代码</label>
                    <input type="text" name="stock_code" required>
                </div>
                <div class="form-group">
                    <label>价格</label>
                    <input type="number" step="0.01" name="price" required>
                </div>
                <div class="form-group">
                    <label>股数</label>
                    <input type="number" name="quantity" required>
                </div>
                <div class="form-group">
                    <label>操作</label>
                    <select name="action">
                        <option value="buy">买入</option>
                        <option value="sell">卖出</option>
                    </select>
                </div>
                <button type="submit">提交</button>
            </form>
        </div>
        
        {% elif page == 'backtest' %}
        <div class="card">
            <h2>🔄 策略回测</h2>
            <form action="/api/backtest" method="post">
                <div class="form-group">
                    <label>股票代码</label>
                    <input type="text" name="stock_code" placeholder="如: 600519" required>
                </div>
                <div class="form-group">
                    <label>开始日期</label>
                    <input type="text" name="start_date" placeholder="如: 20230101" required>
                </div>
                <div class="form-group">
                    <label>结束日期</label>
                    <input type="text" name="end_date" placeholder="如: 20231231" required>
                </div>
                <div class="form-group">
                    <label>策略</label>
                    <select name="strategy">
                        <option value="macd">MACD</option>
                        <option value="mean_reversion">均值回归</option>
                    </select>
                </div>
                <button type="submit">开始回测</button>
            </form>
            <div id="backtest-result"></div>
        </div>
        {% endif %}
    </div>
    
    <script>
    // 获取数据
    async function loadData(url) {
        const resp = await fetch(url);
        return await resp.json();
    }
    
    // 初始化 Dashboard
    if (document.getElementById('market-overview')) {
        loadData('/api/market').then(data => {
            document.getElementById('market-overview').innerHTML = JSON.stringify(data, null, 2);
        });
        loadData('/api/portfolio').then(data => {
            document.getElementById('account-overview').innerHTML = JSON.stringify(data, null, 2);
        });
    }
    
    // 初始化 Portfolio
    if (document.getElementById('positions-body')) {
        loadData('/api/portfolio').then(data => {
            const tbody = document.getElementById('positions-body');
            if (data.positions && data.positions.length > 0) {
                data.positions.forEach(p => {
                    const tr = document.createElement('tr');
                    tr.innerHTML = `<td>${p.stock_code}</td><td>${p.quantity}</td><td>${p.avg_cost}</td><td>${p.total_cost}</td><td>${p.buy_date}</td>`;
                    tbody.appendChild(tr);
                });
            } else {
                tbody.innerHTML = '<tr><td colspan="5">暂无持仓</td></tr>';
            }
        });
    }
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(INDEX_HTML, page='dashboard')


@app.route('/market')
def market():
    return render_template_string(INDEX_HTML, page='market')


@app.route('/portfolio')
def portfolio_page():
    return render_template_string(INDEX_HTML, page='portfolio')


@app.route('/backtest')
def backtest_page():
    return render_template_string(INDEX_HTML, page='backtest')


# API 路由
@app.route('/api/market')
def api_market():
    """市场概览API"""
    overview = analyzer.get_market_overview()
    return jsonify(overview)


@app.route('/api/price', methods=['POST'])
def api_price():
    """查询股价"""
    data = request.form
    stock_code = data.get('stock_code')
    
    if not stock_code:
        return jsonify({'error': '股票代码不能为空'}), 400
    
    price_data = analyzer.get_realtime_price(stock_code)
    return jsonify(price_data)


@app.route('/api/portfolio')
def api_portfolio():
    """持仓API"""
    portfolio = PortfolioManager()
    analytics = PortfolioAnalytics(portfolio)
    
    # 获取当前持仓
    positions_df = portfolio.get_positions()
    positions = positions_df.to_dict('records') if not positions_df.empty else []
    
    summary = portfolio.get_summary()
    metrics = analytics.calculate_metrics()
    
    return jsonify({
        'positions': positions,
        'summary': summary,
        'metrics': metrics,
    })


@app.route('/api/trade', methods=['POST'])
def api_trade():
    """交易API"""
    data = request.form
    stock_code = data.get('stock_code')
    price = float(data.get('price', 0))
    quantity = int(data.get('quantity', 0))
    action = data.get('action', 'buy')
    
    if not stock_code or price <= 0 or quantity <= 0:
        return jsonify({'error': '参数错误'}), 400
    
    if action == 'buy':
        success = portfolio.buy(stock_code, price, quantity)
    else:
        success = portfolio.sell(stock_code, price, quantity)
    
    if success:
        return jsonify({'success': True, 'message': f'{action.upper()} 成功'})
    else:
        return jsonify({'success': False, 'message': '交易失败'})


@app.route('/api/backtest', methods=['POST'])
def api_backtest():
    """回测API"""
    data = request.form
    stock_code = data.get('stock_code')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    strategy_name = data.get('strategy', 'macd')
    
    # 导入策略
    if strategy_name == 'macd':
        from ..strategy.macd import MACDStrategy
        strategy = MACDStrategy()
    else:
        from ..strategy.mean_reversion import MeanReversionStrategy
        strategy = MeanReversionStrategy()
    
    # 运行回测
    engine = BacktestEngine(strategy=strategy)
    
    if engine.load_data(stock_code, start_date, end_date):
        result = engine.run(verbose=False)
        return jsonify(result.to_dict())
    else:
        return jsonify({'error': '数据加载失败'}), 400


def run(host: str = '0.0.0.0', port: int = 5000, debug: bool = False):
    """启动Web服务"""
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run(debug=True)