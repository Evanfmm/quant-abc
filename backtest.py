"""
A股量化交易系统 - 因子回测模块
包含回测引擎、收益计算、风险指标（夏普比率、最大回撤）
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import config


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital=None, commission=0.0003, stamp_tax=0.001):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金，默认使用config配置
            commission: 手续费率，默认万三
            stamp_tax: 印花税率，默认千一（卖出）
        """
        self.initial_capital = initial_capital or config.INITIAL_CAPITAL
        self.commission = commission
        self.stamp_tax = stamp_tax
        
        self.cash = self.initial_capital
        self.positions = {}  # {ts_code: {'shares': int, 'cost': float}}
        self.portfolio_value = self.initial_capital
        self.portfolio_history = []
        self.trades = []
        
    def reset(self):
        """重置回测状态"""
        self.cash = self.initial_capital
        self.positions = {}
        self.portfolio_value = self.initial_capital
        self.portfolio_history = []
        self.trades = []
    
    def buy(self, ts_code, price, shares=None, amount=None):
        """
        买入股票
        
        Args:
            ts_code: 股票代码
            price: 买入价格
            shares: 买入股数（与amount二选一）
            amount: 买入金额（与shares二选一）
        """
        if shares is None and amount is None:
            raise ValueError("必须指定shares或amount")
        
        # 计算买入股数（100股整数倍）
        if amount:
            shares = int(amount / price / 100) * 100
        
        if shares <= 0:
            return False
        
        cost = shares * price
        commission_cost = cost * self.commission
        
        total_cost = cost + commission_cost
        
        if total_cost > self.cash:
            # 资金不足，调整买入数量
            available = self.cash / (price * (1 + self.commission))
            shares = int(available / 100) * 100
            if shares < 100:
                return False
            cost = shares * price
            commission_cost = cost * self.commission
            total_cost = cost + commission_cost
        
        # 执行买入
        self.cash -= total_cost
        
        if ts_code in self.positions:
            old_shares = self.positions[ts_code]['shares']
            old_cost = self.positions[ts_code]['cost'] * old_shares
            new_cost = old_cost + cost
            new_shares = old_shares + shares
            self.positions[ts_code] = {
                'shares': new_shares,
                'cost': new_cost / new_shares
            }
        else:
            self.positions[ts_code] = {'shares': shares, 'cost': price}
        
        # 记录交易
        self.trades.append({
            'date': self.current_date,
            'ts_code': ts_code,
            'action': 'buy',
            'price': price,
            'shares': shares,
            'cost': cost,
            'commission': commission_cost
        })
        
        return True
    
    def sell(self, ts_code, price, shares=None, ratio=None):
        """
        卖出股票
        
        Args:
            ts_code: 股票代码
            price: 卖出价格
            shares: 卖出股数（与ratio二选一）
            ratio: 卖出比例（0-1）
        """
        if ts_code not in self.positions:
            return False
        
        position = self.positions[ts_code]
        current_shares = position['shares']
        
        if shares is None:
            if ratio:
                shares = int(current_shares * ratio)
            else:
                shares = current_shares
        
        shares = min(shares, current_shares)
        if shares <= 0:
            return False
        
        revenue = shares * price
        commission_cost = revenue * self.commission
        tax_cost = revenue * self.stamp_tax
        total_cost = commission_cost + tax_cost
        
        net_revenue = revenue - total_cost
        
        # 更新持仓
        remaining = current_shares - shares
        if remaining > 0:
            self.positions[ts_code]['shares'] = remaining
        else:
            del self.positions[ts_code]
        
        self.cash += net_revenue
        
        # 记录交易
        self.trades.append({
            'date': self.current_date,
            'ts_code': ts_code,
            'action': 'sell',
            'price': price,
            'shares': shares,
            'revenue': revenue,
            'commission': commission_cost,
            'tax': tax_cost,
            'pnl': net_revenue - (shares * position['cost'])
        })
        
        return True
    
    def update_portfolio_value(self, date, prices):
        """
        更新投资组合市值
        
        Args:
            date: 当前日期
            prices: {ts_code: price} 价格字典
        """
        positions_value = 0
        for ts_code, pos in self.positions.items():
            if ts_code in prices:
                positions_value += pos['shares'] * prices[ts_code]
        
        self.portfolio_value = self.cash + positions_value
        self.current_date = date
        
        self.portfolio_history.append({
            'date': date,
            'cash': self.cash,
            'positions_value': positions_value,
            'total_value': self.portfolio_value,
            'positions': {k: v['shares'] for k, v in self.positions.items()}
        })
    
    def get_positions_value(self, prices):
        """获取当前持仓市值"""
        return sum(
            pos['shares'] * prices.get(code, 0)
            for code, pos in self.positions.items()
        )


class BacktestResult:
    """回测结果分析"""
    
    def __init__(self, portfolio_history, trades, initial_capital):
        self.portfolio_history = portfolio_history
        self.trades = trades
        self.initial_capital = initial_capital
        
        self.df = self._build_dataframe()
        
    def _build_dataframe(self):
        """构建DataFrame"""
        if not self.portfolio_history:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.portfolio_history)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_index()
        
        # 计算每日收益率
        df['daily_return'] = df['total_value'].pct_change()
        
        # 计算累计收益率
        df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
        
        # 计算资金曲线
        df['equity_curve'] = self.initial_capital * (1 + df['cumulative_return'])
        
        return df
    
    def total_return(self):
        """总收益率"""
        if self.df.empty:
            return 0
        return (self.portfolio_value() - self.initial_capital) / self.initial_capital
    
    def portfolio_value(self):
        """最终资产净值"""
        if self.df.empty:
            return self.initial_capital
        return self.df['total_value'].iloc[-1]
    
    def annual_return(self):
        """年化收益率"""
        if self.df.empty or len(self.df) < 2:
            return 0
        
        total_days = (self.df.index[-1] - self.df.index[0]).days
        if total_days == 0:
            return 0
        
        total_ret = self.total_return()
        years = total_days / 365
        return (1 + total_ret) ** (1 / years) - 1
    
    def sharpe_ratio(self, risk_free_rate=0.03):
        """
        夏普比率
        
        Args:
            risk_free_rate: 无风险利率（年化）
        """
        if self.df.empty or 'daily_return' not in self.df:
            return 0
        
        returns = self.df['daily_return'].dropna()
        if len(returns) == 0 or returns.std() == 0:
            return 0
        
        # 年化
        daily_rf = risk_free_rate / 252
        excess_returns = returns - daily_rf
        
        return np.sqrt(252) * excess_returns.mean() / returns.std()
    
    def max_drawdown(self):
        """最大回撤"""
        if self.df.empty:
            return 0
        
        equity = self.df['equity_curve']
        cummax = equity.cummax()
        drawdown = (equity - cummax) / cummax
        
        return drawdown.min()
    
    def max_drawdown_duration(self):
        """最大回撤持续时间"""
        if self.df.empty:
            return 0
        
        equity = self.df['equity_curve']
        cummax = equity.cummax()
        drawdown = equity - cummax
        
        # 找到回撤开始和恢复的日期
        in_drawdown = drawdown < 0
        if not in_drawdown.any():
            return 0
        
        # 计算最大回撤持续天数
        max_duration = 0
        current_duration = 0
        
        for is_dd in in_drawdown:
            if is_dd:
                current_duration += 1
            else:
                max_duration = max(max_duration, current_duration)
                current_duration = 0
        
        return max(max_duration, current_duration)
    
    def calmar_ratio(self):
        """Calmar比率 = 年化收益 / 最大回撤"""
        ann_ret = self.annual_return()
        mdd = abs(self.max_drawdown())
        
        if mdd == 0:
            return 0
        
        return ann_ret / mdd
    
    def win_rate(self):
        """胜率"""
        if not self.trades:
            return 0
        
        sells = [t for t in self.trades if t['action'] == 'sell']
        if not sells:
            return 0
        
        wins = sum(1 for t in sells if t.get('pnl', 0) > 0)
        return wins / len(sells)
    
    def profit_loss_ratio(self):
        """盈亏比"""
        if not self.trades:
            return 0
        
        sells = [t for t in self.trades if t['action'] == 'sell']
        if not sells:
            return 0
        
        profits = [t['pnl'] for t in sells if t.get('pnl', 0) > 0]
        losses = [abs(t['pnl']) for t in sells if t.get('pnl', 0) < 0]
        
        if not profits or not losses:
            return 0
        
        return np.mean(profits) / np.mean(losses)
    
    def total_trades(self):
        """总交易次数"""
        return len(self.trades)
    
    def summary(self):
        """回测结果汇总"""
        return {
            'initial_capital': self.initial_capital,
            'final_value': self.portfolio_value(),
            'total_return': self.total_return(),
            'annual_return': self.annual_return(),
            'sharpe_ratio': self.sharpe_ratio(),
            'max_drawdown': self.max_drawdown(),
            'calmar_ratio': self.calmar_ratio(),
            'win_rate': self.win_rate(),
            'profit_loss_ratio': self.profit_loss_ratio(),
            'total_trades': self.total_trades(),
        }
    
    def print_summary(self):
        """打印回测结果"""
        s = self.summary()
        print("\n" + "=" * 50)
        print("回测结果汇总")
        print("=" * 50)
        print(f"初始资金:        {s['initial_capital']:,.2f}")
        print(f"最终资产:        {s['final_value']:,.2f}")
        print(f"总收益率:        {s['total_return']*100:.2f}%")
        print(f"年化收益率:      {s['annual_return']*100:.2f}%")
        print(f"夏普比率:        {s['sharpe_ratio']:.2f}")
        print(f"最大回撤:        {s['max_drawdown']*100:.2f}%")
        print(f"Calmar比率:      {s['calmar_ratio']:.2f}")
        print(f"胜率:            {s['win_rate']*100:.2f}%")
        print(f"盈亏比:          {s['profit_loss_ratio']:.2f}")
        print(f"总交易次数:      {s['total_trades']}")
        print("=" * 50)


class FactorBacktest:
    """因子回测 - 评估因子有效性"""
    
    def __init__(self, initial_capital=100000, commission=0.0003, stamp_tax=0.001):
        self.engine = BacktestEngine(initial_capital, commission, stamp_tax)
        
    def run(self, signals, prices_data, start_date=None, end_date=None):
        """
        运行回测
        
        Args:
            signals: {date: [{ts_code, signal, weight}]} 信号数据
            prices_data: {ts_code: {date: price}} 价格数据
            start_date: 开始日期
            end_date: 结束日期
        """
        # 按日期排序信号
        sorted_dates = sorted(signals.keys())
        
        if start_date:
            sorted_dates = [d for d in sorted_dates if d >= start_date]
        if end_date:
            sorted_dates = [d for d in sorted_dates if d <= end_date]
        
        # 遍历每个交易日
        for date in sorted_dates:
            day_signals = signals.get(date, [])
            day_prices = {}
            
            # 获取当日价格
            for ts_code in prices_data:
                if date in prices_data[ts_code]:
                    day_prices[ts_code] = prices_data[ts_code][date]
            
            if not day_prices:
                continue
            
            # 更新组合价值
            self.engine.current_date = date
            self.engine.update_portfolio_value(date, day_prices)
            
            # 处理交易信号
            target_positions = {}  # 目标持仓
            
            for sig in day_signals:
                ts_code = sig['ts_code']
                signal = sig.get('signal', 1)  # 1=买入, -1=卖出, 0=持有
                weight = sig.get('weight', 0.25)
                
                if signal > 0:  # 买入信号
                    target_positions[ts_code] = weight
            
            # 调仓逻辑
            current_codes = set(self.engine.positions.keys())
            target_codes = set(target_positions.keys())
            
            # 卖出不在目标中的持仓
            for ts_code in current_codes - target_codes:
                if ts_code in day_prices:
                    self.engine.sell(ts_code, day_prices[ts_code], ratio=1)
            
            # 买入新目标
            for ts_code in target_codes - current_codes:
                if ts_code in day_prices:
                    target_value = self.engine.portfolio_value() * target_positions[ts_code]
                    self.engine.buy(ts_code, day_prices[ts_code], amount=target_value)
            
            # 调整现有持仓权重
            for ts_code in current_codes & target_codes:
                target_weight = target_positions[ts_code]
                current_value = self.engine.positions[ts_code]['shares'] * day_prices[ts_code]
                target_value = self.engine.portfolio_value() * target_weight
                
权重高于目标，卖出
                if                # 如果当前 current_value > target_value * 1.1:
                    diff_value = current_value - target_value
                    shares = int(diff_value / day_prices[ts_code] / 100) * 100
                    if shares > 0:
                        self.engine.sell(ts_code, day_prices[ts_code], shares=shares)
                # 如果当前权重低于目标，买入
                elif current_value < target_value * 0.9:
                    diff_value = target_value - current_value
                    self.engine.buy(ts_code, day_prices[ts_code], amount=diff_value)
        
        # 最终更新
        if sorted_dates:
            final_date = sorted_dates[-1]
            final_prices = {}
            for ts_code in prices_data:
                if final_date in prices_data[ts_code]:
                    final_prices[ts_code] = prices_data[ts_code][final_date]
            self.engine.update_portfolio_value(final_date, final_prices)
        
        return BacktestResult(
            self.engine.portfolio_history,
            self.engine.trades,
            self.engine.initial_capital
        )


def calculate_factor_ic(factor_values, returns, n_groups=5):
    """
    计算因子IC（信息系数）
    
    Args:
        factor_values: 因子值DataFrame，index为日期，columns为股票代码
        returns: 收益率DataFrame，index为日期，columns为股票代码
        n_groups: 分组数
    
    Returns:
        ic_series: 每日IC值
        ic_mean: 平均IC
        ic_ir: IC_IR比率
    """
    ics = []
    
    for date in factor_values.index:
        if date not in returns.index:
            continue
        
        factor_row = factor_values.loc[date].dropna()
        return_row = returns.loc[date].dropna()
        
        # 找到共同的股票
        common = factor_row.index.intersection(return_row.index)
        if len(common) < 10:
            continue
        
        # 计算IC
        ic = np.corrcoef(factor_row[common], return_row[common])[0, 1]
        ics.append({'date': date, 'ic': ic})
    
    if not ics:
        return pd.Series(dtype=float), 0, 0
    
    ic_df = pd.DataFrame(ics).set_index('date')
    ic_series = ic_df['ic']
    
    ic_mean = ic_series.mean()
    ic_std = ic_series.std()
    ic_ir = ic_mean / ic_std if ic_std > 0 else 0
    
    return ic_series, ic_mean, ic_ir


def calculate_factor_returns(factor_values, returns, n_groups=5):
    """
    计算因子分组回测收益
    
    Args:
        factor_values: 因子值DataFrame
        returns: 收益率DataFrame
        n_groups: 分组数
    
    Returns:
        group_returns: 各组累计收益
    """
    group_cum_returns = {i: [] for i in range(n_groups)}
    group_dates = []
    
    for date in factor_values.index:
        if date not in returns.index:
            continue
        
        factor_row = factor_values.loc[date].dropna()
        return_row = returns.loc[date].dropna()
        
        common = factor_row.index.intersection(return_row.index)
        if len(common) < n_groups:
            continue
        
        # 分组
        ranks = factor_row[common].rank()
        n_per_group = len(common) / n_groups
        
        group_returns = {i: [] for i in range(n_groups)}
        
        for stock in common:
            group = int(ranks[stock] / n_per_group)
            group = min(group, n_groups - 1)
            group_returns[group].append(return_row[stock])
        
        # 计算各组平均收益
        for i in range(n_groups):
            if group_returns[i]:
                group_cum_returns[i].append(np.mean(group_returns[i]))
            else:
                group_cum_returns[i].append(0)
        
        group_dates.append(date)
    
    # 转换为累计收益
    result = {}
    for i in range(n_groups):
        arr = np.array(group_cum_returns[i])
        result[f'Group_{i+1}'] = (1 + arr).cumprod() - 1
    
    return pd.DataFrame(result, index=group_dates)


if __name__ == "__main__":
    # 简单测试
    print("因子回测模块测试")
    
    # 模拟数据
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    
    # 模拟信号
    signals = {}
    for i, date in enumerate(dates[::5]):  # 每5天调仓
        date_str = date.strftime('%Y-%m-%d')
        signals[date_str] = [
            {'ts_code': '000001.SZ', 'signal': 1, 'weight': 0.5},
            {'ts_code': '000002.SZ', 'signal': 1, 'weight': 0.5},
        ]
    
    # 模拟价格数据
    prices_data = {
        '000001.SZ': {d.strftime('%Y-%m-%d'): 10 + i * 0.1 for i, d in enumerate(dates)},
        '000002.SZ': {d.strftime('%Y-%m-%d'): 20 + i * 0.05 for i, d in enumerate(dates)},
    }
    
    # 运行回测
    bt = FactorBacktest(initial_capital=100000)
    result = bt.run(signals, prices_data)
    
    # 输出结果
    result.print_summary()
