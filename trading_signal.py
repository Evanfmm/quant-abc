"""
A股多因子量化交易系统 - 交易信号与风控模块 v3
根据策略专家建议修改：
1. 添加市场环境判断（下跌趋势时降仓）
2. 添加动态止盈（盈利>15%后启用移动止损）
3. 添加熔断机制（单日亏损>5%暂停）
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config
from risk_manager import RiskManager, PositionSizer, StopLossManager
from broker_api import create_broker


class TradingSignal:
    """交易信号生成（含增强风控）"""
    
    def __init__(self, broker=None):
        self.broker = broker
        self.risk_manager = RiskManager()
        self.position_sizer = PositionSizer()
        self.stop_loss_manager = StopLossManager()
        self.capital = config.INITIAL_CAPITAL
        self.positions = {}  # {ts_code: {'shares': int, 'cost': float, 'industry': str, 'entry_price': float}}
        self.trade_log = []
        
        # 熔断相关
        self.circuit_breaker_triggered = False
        self.daily_pnl = 0  # 当日盈亏
        self.daily_start_capital = config.INITIAL_CAPITAL
        
        # 市场环境
        self.market_trend = 'neutral'
        self.position_limit = 1.0  # 仓位限制系数
        
    def update_market_trend(self):
        """更新市场环境判断"""
        try:
            self.market_trend = data_fetcher.get_market_trend()
            trend = self.market_trend.get('trend', 'neutral')
            index_change = self.market_trend.get('index_change_5d', 0)
            
            # 根据市场趋势调整仓位
            if trend == 'down':
                # 下跌趋势：降仓到50%
                self.position_limit = 0.5
                print(f"⚠️ 市场下跌趋势，仓位限制: 50%")
            elif trend == 'neutral':
                # 中性趋势：保持70%
                self.position_limit = 0.7
                print(f"📊 市场中性趋势，仓位限制: 70%")
            else:
                # 上涨趋势：保持100%
                self.position_limit = 1.0
                print(f"📈 市场上涨趋势，仓位限制: 100%")
                
        except Exception as e:
            print(f"获取市场趋势失败: {e}")
            self.position_limit = 1.0
    
    def check_circuit_breaker(self):
        """检查熔断机制"""
        if self.circuit_breaker_triggered:
            print("🚨 熔断机制已触发，禁止交易")
            return True
        
        # 计算当日盈亏比例
        daily_loss_pct = (self.daily_start_capital - self.capital) / self.daily_start_capital
        
        if daily_loss_pct > config.DAILY_LOSS_CIRCUIT:
            self.circuit_breaker_triggered = True
            print(f"🚨 熔断触发！单日亏损 {daily_loss_pct*100:.2f}%，超过阈值 {config.DAILY_LOSS_CIRCUIT*100}%")
            return True
        
        return False
    
    def reset_daily_PnL(self):
        """重置当日盈亏计算"""
        self.daily_start_capital = self._calculate_equity()
        self.daily_pnl = 0
    
    def calculate_position_size(self, price, position_value=None):
        """计算仓位（考虑市场环境限制）"""
        if position_value is None:
            position_value = self.capital / config.MAX_POSITION
        
        # 应用市场环境限制
        position_value = position_value * self.position_limit
        
        # 向下取整到100股
        shares = int(position_value / price / 100) * 100
        return shares
    
    def generate_signal(self, selected_stocks, current_positions=None, market_data=None):
        """生成交易信号（含增强风控检查）"""
        if current_positions is None:
            current_positions = self.positions
        
        # 更新市场环境
        self.update_market_trend()
        
        # 检查熔断
        if self.check_circuit_breaker():
            print("🚨 熔断期间，仅处理止损")
            return self._generate_stop_loss_signals(current_positions, market_data)
        
        signals = []
        
        # 无推荐股票，全部卖出
        if selected_stocks is None or len(selected_stocks) == 0:
            for ts_code in current_positions.keys():
                signals.append({
                    'action': 'SELL',
                    'ts_code': ts_code,
                    'reason': '无推荐股票'
                })
            return signals
        
        if 'ts_code' not in selected_stocks.columns:
            return signals
        
        # ========== 风控检查 ==========
        
        # 1. 检查回撤限制
        current_equity = self._calculate_equity(market_data)
        can_trade, drawdown = self.risk_manager.check_drawdown(current_equity)
        if not can_trade:
            print(f"⚠️ 触发回撤限制 {drawdown*100:.1f}%，禁止新开仓")
            return self._generate_stop_loss_signals(current_positions, market_data, add_trailing=True)
        
        # 2. 检查仓位限制（考虑市场环境）
        can_trade, reason, max_position_value = self.risk_manager.check_position_limits(
            current_positions
        )
        
        # 应用市场环境限制
        max_position_value = max_position_value * self.position_limit
        
        # 获取当前持仓
        current_holdings = set(current_positions.keys())
        target_holdings = set(selected_stocks['ts_code'].tolist())
        
        # ========== 卖出信号 ==========
        for ts_code in current_holdings:
            position = current_positions[ts_code]
            
            # 检查是否需要止损/止盈
            if market_data and ts_code in market_data:
                should_exit, exit_reason = self.stop_loss_manager.should_exit(
                    ts_code,
                    position.get('cost', 0),
                    market_data[ts_code]
                )
                
                # 增强版：添加动态止盈检查
                current_price = market_data[ts_code]
                entry_price = position.get('entry_price', position.get('cost', 0))
                profit_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
                
                # 盈利>15%后启用移动止损
                if profit_pct > config.TRAILING_STOP_ACTIVATION:
                    # 移动止损：保本+利润回吐
                    trailing_stop_price = entry_price * (1 + config.TRAILING_STOP_ACTIVATION - config.TRAILING_STOP_PCT)
                    if current_price < trailing_stop_price:
                        signals.append({
                            'action': 'SELL',
                            'ts_code': ts_code,
                            'reason': f'移动止损(盈利{profit_pct*100:.1f}%)'
                        })
                        continue
                
                if should_exit:
                    signals.append({
                        'action': 'SELL',
                        'ts_code': ts_code,
                        'reason': f'{exit_reason}'
                    })
                    continue
            
            # 不在目标持仓中
            if ts_code not in target_holdings:
                signals.append({
                    'action': 'SELL',
                    'ts_code': ts_code,
                    'reason': '不在目标持仓中'
                })
        
        # ========== 买入信号 ==========
        if can_trade and self.position_limit > 0.1:  # 下跌趋势不完全禁止买入
            for _, row in selected_stocks.iterrows():
                ts_code = row['ts_code']
                if ts_code not in current_holdings:
                    # 计算仓位（考虑市场限制）
                    shares = self.calculate_position_size(row['close'], max_position_value)
                    if shares > 0:
                        # 检查行业限制
                        industry = row.get('industry', '未知')
                        can_buy, reason = self.risk_manager.check_industry_limits(
                            current_positions,
                            industry,
                            shares * row['close'],
                            current_equity
                        )
                        if not can_buy:
                            print(f"⚠️ 行业限制: {reason}")
                            continue
                        
                        signals.append({
                            'action': 'BUY',
                            'ts_code': ts_code,
                            'name': row['name'],
                            'price': row['close'],
                            'shares': shares,
                            'amount': shares * row['close'],
                            'industry': industry,
                            'reason': f"多因子得分: {row.get('factor_score', 0):.3f}"
                        })
        else:
            if self.position_limit <= 0.1:
                print(f"⚠️ 市场下跌趋势，限制买入")
            else:
                print(f"⚠️ 仓位限制: {reason}")
        
        return signals
    
    def _calculate_equity(self, market_data=None):
        """计算当前权益"""
        position_value = 0
        for ts_code, pos in self.positions.items():
            price = market_data.get(ts_code, pos.get('cost', 0)) if market_data else pos.get('cost', 0)
            position_value += pos.get('shares', 0) * price
        
        return self.capital + position_value
    
    def _generate_stop_loss_signals(self, positions, market_data, add_trailing=False):
        """生成止损信号"""
        signals = []
        for ts_code, pos in positions.items():
            if market_data and ts_code in market_data:
                should_exit, reason = self.stop_loss_manager.should_exit(
                    ts_code,
                    pos.get('cost', 0),
                    market_data[ts_code]
                )
                
                # 动态止盈：盈利>15%后启用移动止损
                if add_trailing:
                    current_price = market_data[ts_code]
                    entry_price = pos.get('entry_price', pos.get('cost', 0))
                    profit_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
                    
                    if profit_pct > config.TRAILING_STOP_ACTIVATION:
                        trailing_stop_price = entry_price * (1 + config.TRAILING_STOP_ACTIVATION - config.TRAILING_STOP_PCT)
                        if current_price < trailing_stop_price:
                            signals.append({
                                'action': 'SELL',
                                'ts_code': ts_code,
                                'reason': f'移动止损(盈利{profit_pct*100:.1f}%)'
                            })
                            continue
                
                if should_exit:
                    signals.append({
                        'action': 'SELL',
                        'ts_code': ts_code,
                        'reason': f'stop_loss_{reason}'
                    })
        return signals
    
    def simulate_trade(self, signal, current_price=None):
        """模拟交易"""
        if signal['action'] == 'BUY':
            cost = signal['shares'] * (current_price or signal['price'])
            commission = cost * config.COMMISSION_RATE
            total_cost = cost + commission
            
            # 更新持仓记录
            self.positions[signal['ts_code']] = {
                'shares': signal['shares'],
                'cost': current_price or signal['price'],
                'entry_price': current_price or signal['price'],
                'industry': signal.get('industry', '未知'),
                'entry_date': datetime.now()
            }
            
            return {
                'action': 'BUY',
                'ts_code': signal['ts_code'],
                'name': signal['name'],
                'shares': signal['shares'],
                'price': current_price or signal['price'],
                'cost': total_cost,
                'commission': commission
            }
        else:  # SELL
            revenue = signal['shares'] * current_price
            commission = revenue * config.COMMISSION_RATE
            tax = revenue * config.STAMP_TAX
            net_revenue = revenue - commission - tax
            
            # 更新权益
            self.capital += net_revenue
            
            # 记录盈亏
            if signal['ts_code'] in self.positions:
                position = self.positions[signal['ts_code']]
                pnl = net_revenue - position['shares'] * position.get('entry_price', position['cost'])
                self.daily_pnl += pnl
            
            # 清除持仓
            if signal['ts_code'] in self.positions:
                del self.positions[signal['ts_code']]
            
            return {
                'action': 'SELL',
                'ts_code': signal['ts_code'],
                'shares': signal['shares'],
                'price': current_price,
                'revenue': net_revenue,
                'commission': commission,
                'tax': tax
            }
    
    def get_risk_status(self):
        """获取风控状态"""
        return {
            'market_trend': self.market_trend.get('trend', 'neutral'),
            'position_limit': self.position_limit,
            'circuit_breaker': self.circuit_breaker_triggered,
            'daily_pnl': self.daily_pnl,
            'positions': len(self.positions)
        }


class Backtest:
    """回测引擎（增强版 v3）"""
    
    def __init__(self, initial_capital=50000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = {}
        self.trades = []
        self.equity_curve = []
        self.daily_results = []
        
        # 风控
        self.risk_manager = RiskManager()
        self.position_sizer = PositionSizer()
        
        # 增强风控
        self.circuit_breaker_triggered = False
        self.market_trend = 'neutral'
        
    def update_market_trend(self):
        """更新市场趋势"""
        try:
            self.market_trend = data_fetcher.get_market_trend()
        except:
            self.market_trend = {'trend': 'neutral'}
    
    def check_circuit_breaker(self):
        """熔断检查"""
        if self.circuit_breaker_triggered:
            return True
        
        # 检查是否需要触发熔断
        if len(self.equity_curve) > 0:
            current = self.equity_curve[-1]['total']
            daily_loss = (self.initial_capital - current) / self.initial_capital
            if daily_loss > config.DAILY_LOSS_CIRCUIT:
                self.circuit_breaker_triggered = True
                return True
        
        return False
    
    def run(self, signals, price_data, start_date=None, end_date=None):
        """运行回测"""
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        dates = pd.date_range(start_date, end_date, freq='B')
        
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            
            # 更新市场趋势
            self.update_market_trend()
            
            # 检查熔断
            if self.circuit_breaker_triggered:
                print(f"🚨 熔断触发，暂停交易")
                break
            
            # 获取当日价格
            daily_prices = self._get_daily_prices(date_str)
            
            # 更新持仓价格
            for ts_code in self.positions:
                if ts_code in daily_prices:
                    self.positions[ts_code]['current_price'] = daily_prices[ts_code]
            
            # 记录权益
            self._record_equity(date)
            
            # 检查止损
            self._check_stop_loss(daily_prices)
            
            # 执行信号
            for signal in signals:
                if signal['action'] == 'BUY':
                    self._execute_buy(signal, daily_prices)
                else:
                    self._execute_sell(signal, daily_prices)
        
        return self.get_performance()
    
    def _get_daily_prices(self, date_str):
        """获取当日价格"""
        # 简化：使用配置价格
        return {}
    
    def _execute_buy(self, signal, price_data):
        """执行买入"""
        ts_code = signal['ts_code']
        price = price_data.get(ts_code, signal.get('price', 0))
        
        if price <= 0:
            return
        
        # 根据市场趋势调整仓位
        position_limit = 1.0
        if self.market_trend.get('trend') == 'down':
            position_limit = 0.5
        elif self.market_trend.get('trend') == 'neutral':
            position_limit = 0.7
        
        available_capital = (self.capital / max(1, config.MAX_POSITION - len(self.positions))) * position_limit
        shares = int(available_capital / price / 100) * 100
        
        if shares > 0 and self.capital >= shares * price * (1 + config.COMMISSION_RATE):
            cost = shares * price * (1 + config.COMMISSION_RATE)
            self.capital -= cost
            
            self.positions[ts_code] = {
                'shares': shares,
                'cost': price,
                'current_price': price,
                'entry_price': price,
                'entry_date': datetime.now(),
                'industry': signal.get('industry', '未知')
            }
            
            self.trades.append({
                'date': datetime.now(),
                'action': 'BUY',
                'ts_code': ts_code,
                'shares': shares,
                'price': price,
                'cost': cost
            })
    
    def _execute_sell(self, signal, price_data):
        """执行卖出"""
        ts_code = signal['ts_code']
        if ts_code not in self.positions:
            return
        
        price = price_data.get(ts_code, signal.get('price', 0))
        if price <= 0:
            return
        
        position = self.positions[ts_code]
        
        revenue = position['shares'] * price * (1 - config.COMMISSION_RATE - config.STAMP_TAX)
        self.capital += revenue
        
        self.trades.append({
            'date': datetime.now(),
            'action': 'SELL',
            'ts_code': ts_code,
            'shares': position['shares'],
            'price': price,
            'revenue': revenue,
            'pnl': revenue - position['shares'] * position['cost']
        })
        
        del self.positions[ts_code]
    
    def _check_stop_loss(self, price_data):
        """检查止损"""
        to_sell = []
        
        for ts_code, pos in self.positions.items():
            if ts_code not in price_data:
                continue
            
            current_price = price_data[ts_code]
            entry_price = pos.get('entry_price', pos.get('cost', 0))
            
            # 止损检查
            loss_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
            if loss_pct < -config.STOP_LOSS_PCT:
                to_sell.append(ts_code)
                continue
            
            # 动态止盈检查
            if loss_pct > config.TRAILING_STOP_ACTIVATION:
                trailing_stop_price = entry_price * (1 + config.TRAILING_STOP_ACTIVATION - config.TRAILING_STOP_PCT)
                if current_price < trailing_stop_price:
                    to_sell.append(ts_code)
        
        for ts_code in to_sell:
            self._execute_sell({'ts_code': ts_code, 'price': price_data[ts_code]}, price_data)
    
    def _record_equity(self, date):
        """记录权益"""
        position_value = sum(
            pos.get('shares', 0) * pos.get('current_price', pos.get('cost', 0))
            for pos in self.positions.values()
        )
        
        self.equity_curve.append({
            'date': date,
            'cash': self.capital,
            'position_value': position_value,
            'total': self.capital + position_value
        })
    
    def get_performance(self):
        """获取绩效指标"""
        total_return = (self.capital - self.initial_capital) / self.initial_capital
        
        # 计算最大回撤
        equity = self.initial_capital
        peak = equity
        max_drawdown = 0
        
        for record in self.equity_curve:
            total = record['total']
            if total > peak:
                peak = total
            
            drawdown = (peak - total) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 计算夏普比率（简化）
        if len(self.equity_curve) > 1:
            returns = [e['total'] / self.equity_curve[0]['total'] - 1 
                      for e in self.equity_curve]
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        else:
            sharpe = 0
        
        return {
            'initial_capital': self.initial_capital,
            'final_capital': self.capital,
            'total_return': total_return,
            'total_return_pct': total_return * 100,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown * 100,
            'num_trades': len(self.trades),
            'sharpe_ratio': sharpe,
            'positions': self.positions,
            'trades': self.trades,
            'equity_curve': self.equity_curve,
            'circuit_breaker_triggered': self.circuit_breaker_triggered
        }


# 便捷函数
stop_loss_manager = StopLossManager()


def run_simple_backtest():
    """运行简单回测"""
    print("=" * 50)
    print("开始回测（增强版 v3）...")
    print("=" * 50)
    
    bt = Backtest(config.INITIAL_CAPITAL)
    
    print("回测完成")
    perf = bt.get_performance()
    print(f"初始资金: {perf['initial_capital']:.2f}")
    print(f"最终资金: {perf['final_capital']:.2f}")
    print(f"总收益: {perf['total_return_pct']:.2f}%")
    print(f"最大回撤: {perf['max_drawdown_pct']:.2f}%")
    print(f"熔断触发: {'是' if perf['circuit_breaker_triggered'] else '否'}")
    
    return perf


if __name__ == "__main__":
    run_simple_backtest()
