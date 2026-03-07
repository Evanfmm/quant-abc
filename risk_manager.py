"""
A股量化交易系统 - 风险控制模块
包含仓位控制、回撤管理、单股风险、组合风险等多维度风控
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import config


class RiskManager:
    """风险管理器"""
    
    def __init__(self):
        # 风险参数
        self.max_position = config.MAX_POSITION
        self.max_single_position = config.MAX_SINGLE_POSITION
        self.max_drawdown = config.MAX_DRAWDOWN
        self.max_single_loss = config.MAX_SINGLE_LOSS  # 统一为8%
        self.max_industry_weight = config.MAX_INDUSTRY_WEIGHT
        self.max_hold_days = config.MAX_HOLD_DAYS  # 时间止损：5天
        
        # 状态
        self.peak_equity = 0
        self.current_drawdown = 0
        self.daily_loss_count = 0
        self.daily_loss = 0  # 当日亏损
        self.risk_events = []
        self.positions_entry_date = {}  # {ts_code: entry_date}
        
    def check_position_limits(self, positions, new_position_value=None):
        """
        检查仓位限制
        返回: (can_trade, reason, max_position_value)
        """
        current_positions = len(positions)
        
        # 检查持仓数量
        if current_positions >= self.max_position:
            return False, f"已达最大持仓数{self.max_position}只", 0
        
        # 计算可用仓位
        available_slots = self.max_position - current_positions
        
        # 如果有新持仓，计算平均每只的仓位
        if new_position_value:
            max_per_position = new_position_value / available_slots
        else:
            max_per_position = config.INITIAL_CAPITAL / self.max_position
        
        return True, "OK", max_per_position
    
    def check_single_position(self, position_value, total_equity):
        """
        检查单只股票仓位限制
        """
        position_ratio = position_value / total_equity if total_equity > 0 else 0
        
        if position_ratio > self.max_single_position:
            return False, f"单只仓位比例{position_ratio*100:.1f}%超过限制{self.max_single_position*100}%"
        
        return True, "OK"
    
    def check_industry_limits(self, positions, new_industry, new_position_value, total_equity):
        """
        检查行业仓位限制
        positions: {ts_code: {'shares': int, 'cost': float, 'industry': str}}
        """
        # 计算当前行业权重
        industry_weights = defaultdict(float)
        
        for ts_code, pos in positions.items():
            industry = pos.get('industry', '未知')
            value = pos.get('shares', 0) * pos.get('current_price', pos.get('cost', 0))
            industry_weights[industry] += value
        
        # 添加新仓位
        industry_weights[new_industry] += new_position_value
        
        # 检查限制
        for industry, weight in industry_weights.items():
            weight_ratio = weight / total_equity if total_equity > 0 else 0
            if weight_ratio > self.max_industry_weight:
                return False, f"行业{industry}权重{weight_ratio*100:.1f}%超过限制{self.max_industry_weight*100}%"
        
        return True, "OK"
    
    def check_drawdown(self, current_equity):
        """
        检查回撤限制
        返回: (can_trade, drawdown_pct)
        """
        # 更新峰值
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity
        
        # 计算回撤
        if self.peak_equity > 0:
            self.current_drawdown = (self.peak_equity - current_equity) / self.peak_equity
        
        if self.current_drawdown > self.max_drawdown:
            return False, self.current_drawdown
        
        return True, self.current_drawdown
    
    def check_stop_loss(self, position):
        """
        检查止损信号
        position: {'cost': float, 'current_price': float, 'shares': int}
        """
        if not position.get('current_price') or not position.get('cost'):
            return False, None
        
        cost = position['cost']
        current = position['current_price']
        loss_ratio = (current - cost) / cost
        
        if loss_ratio < -self.max_single_loss:
            return True, loss_ratio
        
        return False, loss_ratio
    
    def check_take_profit(self, position):
        """
        检查止盈信号
        """
        if not position.get('current_price') or not position.get('cost'):
            return False, None
        
        cost = position['cost']
        current = position['current_price']
        profit_ratio = (current - cost) / cost
        
        # 止盈点：盈利超过20%
        if profit_ratio > 0.20:
            return True, profit_ratio
        
        return False, profit_ratio
    
    def calculate_var(self, positions, confidence=0.95):
        """
        计算VaR (Value at Risk) - 风险价值
        简化版本：基于持仓波动率
        """
        if not positions:
            return 0
        
        total_value = 0
        var = 0
        
        for ts_code, pos in positions.items():
            value = pos.get('shares', 0) * pos.get('current_price', pos.get('cost', 0))
            # 假设波动率20%（简化）
            volatility = 0.20
            var += value * volatility * 1.65  # 95%置信度
            total_value += value
        
        return var
    
    def calculate_portfolio_volatility(self, positions):
        """
        计算组合波动率（简化）
        """
        if not positions:
            return 0
        
        # 简化为等权重组合
        n = len(positions)
        single_vol = 0.20  # 单股波动率
        
        # 假设相关性0.3
        correlation = 0.3
        portfolio_vol = single_vol * np.sqrt(correlation + (1-correlation)/n)
        
        return portfolio_vol
    
    def get_risk_report(self, positions, current_equity):
        """
        生成风险报告
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'current_equity': current_equity,
            'peak_equity': self.peak_equity,
            'current_drawdown': self.current_drawdown,
            'max_drawdown': self.max_drawdown,
            'num_positions': len(positions),
            'max_position': self.max_position,
            'var_95': self.calculate_var(positions),
            'portfolio_volatility': self.calculate_portfolio_volatility(positions),
            'risk_events': self.risk_events[-5:]  # 最近5个风险事件
        }
        
        # 计算各行业权重
        industry_weights = defaultdict(float)
        for ts_code, pos in positions.items():
            industry = pos.get('industry', '未知')
            value = pos.get('shares', 0) * pos.get('current_price', pos.get('cost', 0))
            industry_weights[industry] += value
        
        total = sum(industry_weights.values())
        report['industry_weights'] = {k: v/total if total > 0 else 0 for k, v in industry_weights.items()}
        
        return report
    
    def log_risk_event(self, event_type, details):
        """记录风险事件"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'details': details
        }
        self.risk_events.append(event)
    
    def check_daily_circuit(self, current_equity, initial_capital):
        """
        检查日内熔断
        当日亏损超过5%时自动停止交易
        返回: (circuit_triggered, loss_pct)
        """
        daily_loss_pct = (initial_capital - current_equity) / initial_capital
        
        if daily_loss_pct > config.DAILY_LOSS_CIRCUIT:
            self.log_risk_event('DAILY_CIRCUIT', {
                'loss_pct': daily_loss_pct,
                'threshold': config.DAILY_LOSS_CIRCUIT,
                'current_equity': current_equity
            })
            return True, daily_loss_pct
        
        self.daily_loss = daily_loss_pct
        return False, daily_loss_pct
    
    def check_time_stop_loss(self, ts_code, current_price, entry_price, entry_date):
        """
        检查时间止损
        持有超过5天且无盈利时强制卖出
        返回: (should_exit, reason)
        """
        if not entry_date:
            return False, None
        
        # 更新持仓入场日期
        if ts_code not in self.positions_entry_date:
            self.positions_entry_date[ts_code] = entry_date
        
        # 计算持有天数
        hold_days = (datetime.now() - entry_date).days
        
        if hold_days > self.max_hold_days:
            # 持有超过5天，检查是否盈利
            profit_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
            
            if profit_pct <= 0:
                # 无盈利，强制卖出
                self.log_risk_event('TIME_STOP_LOSS', {
                    'ts_code': ts_code,
                    'hold_days': hold_days,
                    'profit_pct': profit_pct
                })
                return True, f'时间止损(持有{hold_days}天, 盈利{profit_pct*100:.1f}%)'
        
        return False, None
    
    def reset_daily_loss(self):
        """重置当日亏损计数"""
        self.daily_loss = 0


class PositionSizer:
    """仓位管理器 - 动态计算仓位"""
    
    def __init__(self):
        self.base_position = config.INITIAL_CAPITAL / config.MAX_POSITION
        self.volatility_scaling = True
        self.risk_budget = config.MAX_DRAWDOWN
        
    def calculate_kelly_fraction(self, win_rate, avg_win, avg_loss):
        """
        Kelly公式计算仓位
        f* = (bp - q) / b
        b: 赔率, p: 胜率, q: 1-p
        """
        if avg_loss == 0 or win_rate <= 0 or win_rate >= 1:
            return 0.25  # 默认25%
        
        b = avg_win / avg_loss  # 赔率
        p = win_rate
        q = 1 - p
        
        kelly = (b * p - q) / b
        
        # 半Kelly降低风险
        return max(0, min(kelly / 2, 0.25))
    
    def calculate_equal_weight(self, num_positions, total_capital):
        """等权重仓位"""
        if num_positions == 0:
            return total_capital / self.max_position
        return total_capital / num_positions
    
    def calculate_risk_parity(self, positions, total_capital):
        """
        风险平价仓位 - 每只股票贡献相同的风险
        简化版本
        """
        if not positions:
            return total_capital / config.MAX_POSITION
        
        # 每只股票等风险
        n = len(positions)
        vol = 0.20  # 简化波动率
        
        # 风险平价权重
        weight = 1 / n
        position_value = total_capital * weight
        
        return position_value
    
    def calculate_volatility_weighted(self, positions, total_capital, lookback_vol=None):
        """
        波动率加权仓位 - 波动率低的仓位更大
        """
        if not positions:
            return total_capital / config.MAX_POSITION
        
        # 简化：使用固定波动率
        vols = {ts_code: 0.20 for ts_code in positions.keys()}
        
        # 反比于波动率
        total_inv_vol = sum(1/v for v in vols.values())
        
        weights = {ts_code: (1/v) / total_inv_vol for ts_code, v in vols.items()}
        
        return {ts_code: total_capital * w for ts_code, w in weights.items()}
    
    def get_position_size(self, strategy='equal_weight', **kwargs):
        """
        获取仓位大小
        strategy: equal_weight, kelly, risk_parity, volatility_weighted
        """
        total_capital = kwargs.get('total_capital', config.INITIAL_CAPITAL)
        num_positions = kwargs.get('num_positions', 1)
        
        if strategy == 'kelly':
            win_rate = kwargs.get('win_rate', 0.5)
            avg_win = kwargs.get('avg_win', 0.1)
            avg_loss = kwargs.get('avg_loss', 0.05)
            return self.calculate_kelly_fraction(win_rate, avg_win, avg_loss) * total_capital
        
        elif strategy == 'risk_parity':
            positions = kwargs.get('positions', {})
            return self.calculate_risk_parity(positions, total_capital)
        
        elif strategy == 'volatility_weighted':
            positions = kwargs.get('positions', {})
            return self.calculate_volatility_weighted(positions, total_capital)
        
        else:  # equal_weight
            return self.calculate_equal_weight(num_positions, total_capital)


class StopLossManager:
    """止损止盈管理器"""
    
    def __init__(self):
        self.stop_loss_pct = config.STOP_LOSS_PCT
        self.take_profit_pct = config.TAKE_PROFIT_PCT
        self.trailing_stop_enabled = True
        self.trailing_stop_pct = config.TRAILING_STOP_PCT
        
        # 跟踪最高价
        self.highest_prices = {}  # {ts_code: highest_price}
        
    def update_highest_price(self, ts_code, current_price):
        """更新最高价（用于跟踪止损）"""
        if ts_code not in self.highest_prices:
            self.highest_prices[ts_code] = current_price
        else:
            self.highest_prices[ts_code] = max(self.highest_prices[ts_code], current_price)
    
    def check_stop_loss(self, ts_code, entry_price, current_price):
        """检查止损"""
        if entry_price <= 0 or current_price <= 0:
            return False
        
        loss_pct = (entry_price - current_price) / entry_price
        
        if loss_pct >= self.stop_loss_pct:
            return True
        
        return False
    
    def check_take_profit(self, ts_code, entry_price, current_price):
        """检查止盈"""
        if entry_price <= 0 or current_price <= 0:
            return False
        
        profit_pct = (current_price - entry_price) / entry_price
        
        if profit_pct >= self.take_profit_pct:
            return True
        
        return False
    
    def check_trailing_stop(self, ts_code, entry_price, current_price):
        """检查跟踪止损"""
        if not self.trailing_stop_enabled:
            return False
        
        highest = self.highest_prices.get(ts_code, current_price)
        
        # 从最高点回撤超过阈值
        drawdown = (highest - current_price) / highest
        
        if drawdown >= self.trailing_stop_pct:
            return True
        
        return False
    
    def should_exit(self, ts_code, entry_price, current_price):
        """
        综合判断是否应该退出
        返回: (should_exit, reason)
        """
        # 更新最高价
        self.update_highest_price(ts_code, current_price)
        
        # 检查各种退出条件
        if self.check_stop_loss(ts_code, entry_price, current_price):
            return True, 'stop_loss'
        
        if self.check_take_profit(ts_code, entry_price, current_price):
            return True, 'take_profit'
        
        if self.check_trailing_stop(ts_code, entry_price, current_price):
            return True, 'trailing_stop'
        
        return False, None
    
    def reset(self, ts_code):
        """重置某只股票的跟踪状态"""
        if ts_code in self.highest_prices:
            del self.highest_prices[ts_code]


# 便捷函数
def create_risk_manager():
    """创建风险管理器"""
    return RiskManager()


def create_position_sizer():
    """创建仓位管理器"""
    return PositionSizer()


def create_stop_loss_manager():
    """创建止损管理器"""
    return StopLossManager()


if __name__ == "__main__":
    # 测试
    rm = RiskManager()
    print("风险管理器测试")
    
    # 测试仓位检查
    can_trade, reason, max_pos = rm.check_position_limits({'001': {}, '002': {}})
    print(f"仓位检查: {can_trade}, {reason}")
    
    # 测试回撤检查
    can_trade, dd = rm.check_drawdown(45000)
    print(f"回撤检查: {can_trade}, 回撤{dd*100:.1f}%")
    
    # 测试仓位计算
    ps = PositionSizer()
    size = ps.get_position_size('equal_weight', total_capital=100000, num_positions=4)
    print(f"等权仓位: {size:.2f}")
