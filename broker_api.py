"""
A股量化交易系统 - 券商API对接模块
支持多种券商接口，回测/实盘模式切换
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import json
import os
import config


class BrokerAPI(ABC):
    """券商API抽象基类"""
    
    @abstractmethod
    def connect(self):
        """连接券商"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """断开连接"""
        pass
    
    @abstractmethod
    def get_account_info(self):
        """获取账户信息"""
        pass
    
    @abstractmethod
    def get_positions(self):
        """获取持仓"""
        pass
    
    @abstractmethod
    def get_orders(self):
        """获取订单"""
        pass
    
    @abstractmethod
    def place_order(self, ts_code, direction, volume, order_type='limit'):
        """下单"""
        pass
    
    @abstractmethod
    def cancel_order(self, order_id):
        """撤单"""
        pass
    
    @abstractmethod
    def get_quote(self, ts_code):
        """获取实时行情"""
        pass


class TushareBroker(BrokerAPI):
    """Tushare数据源（模拟交易）"""
    
    def __init__(self):
        self.connected = False
        self.account = {
            'cash': config.INITIAL_CAPITAL,
            'total_assets': config.INITIAL_CAPITAL,
            'available_cash': config.INITIAL_CAPITAL,
        }
        self.positions = {}  # {ts_code: {'shares': int, 'cost': float}}
        self.orders = []  # [{'order_id': str, 'ts_code': str, ...}]
        self.order_id_counter = 1000
    
    def connect(self):
        """连接（实际上只是初始化）"""
        import tushare as ts
        ts.set_token(config.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.connected = True
        print("Tushare数据源已连接")
    
    def disconnect(self):
        self.connected = False
        print("Tushare已断开")
    
    def get_account_info(self):
        return self.account
    
    def get_positions(self):
        return self.positions
    
    def get_orders(self):
        return self.orders
    
    def place_order(self, ts_code, direction, volume, price=None, order_type='limit'):
        """模拟下单"""
        if not self.connected:
            return None
        
        order_id = f"ORDER_{self.order_id_counter}"
        self.order_id_counter += 1
        
        # 尝试获取价格
        if price is None:
            try:
                df = self.pro.daily(ts_code=ts_code, trade_date=datetime.now().strftime('%Y%m%d'))
                if len(df) > 0:
                    price = df.iloc[0]['close']
                else:
                    price = 0
            except:
                price = 0
        
        order = {
            'order_id': order_id,
            'ts_code': ts_code,
            'direction': direction,  # 'BUY' or 'SELL'
            'volume': volume,
            'price': price,
            'order_type': order_type,
            'status': 'FILLED',  # 模拟直接成交
            'filled_volume': volume if direction == 'BUY' else 0,
            'create_time': datetime.now().isoformat()
        }
        
        # 执行模拟成交
        if direction == 'BUY':
            cost = volume * price * (1 + config.COMMISSION_RATE)
            if self.account['available_cash'] >= cost:
                self.account['available_cash'] -= cost
                if ts_code in self.positions:
                    old_shares = self.positions[ts_code]['shares']
                    old_cost = self.positions[ts_code]['cost'] * old_shares
                    new_cost = old_cost + volume * price
                    new_shares = old_shares + volume
                    self.positions[ts_code] = {
                        'shares': new_shares,
                        'cost': new_cost / new_shares
                    }
                else:
                    self.positions[ts_code] = {
                        'shares': volume,
                        'cost': price
                    }
                order['filled_price'] = price
                order['filled_volume'] = volume
            else:
                order['status'] = 'REJECTED'
                order['error'] = '资金不足'
        else:  # SELL
            if ts_code in self.positions and self.positions[ts_code]['shares'] >= volume:
                revenue = volume * price * (1 - config.COMMISSION_RATE - config.STAMP_TAX)
                self.account['available_cash'] += revenue
                self.positions[ts_code]['shares'] -= volume
                if self.positions[ts_code]['shares'] == 0:
                    del self.positions[ts_code]
                order['filled_price'] = price
                order['filled_volume'] = volume
            else:
                order['status'] = 'REJECTED'
                order['error'] = '持仓不足'
        
        self.orders.append(order)
        
        # 更新总资产
        self._update_total_assets()
        
        return order
    
    def cancel_order(self, order_id):
        for order in self.orders:
            if order['order_id'] == order_id and order['status'] == 'PENDING':
                order['status'] = 'CANCELLED'
                return True
        return False
    
    def get_quote(self, ts_code):
        """获取实时行情"""
        if not self.connected:
            return None
        
        try:
            df = self.pro.daily(ts_code=ts_code, trade_date=datetime.now().strftime('%Y%m%d'))
            if len(df) > 0:
                row = df.iloc[0]
                return {
                    'ts_code': ts_code,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['vol'],
                    'pct_chg': row['pct_chg']
                }
        except:
            pass
        return None
    
    def _update_total_assets(self):
        """更新总资产"""
        position_value = 0
        for ts_code, pos in self.positions.items():
            # 简化：使用成本价计算
            position_value += pos['shares'] * pos.get('current_price', pos['cost'])
        self.account['total_assets'] = self.account['available_cash'] + position_value


class FutuBroker(BrokerAPI):
    """富途证券API（需安装futu API）"""
    
    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 11111
        self.connected = False
        self.order_id_counter = 10000
    
    def connect(self):
        """连接富途牛牛"""
        try:
            from futu import OpenQuoteContext, OpenTradeContext
            
            self.quote_ctx = OpenQuoteContext(self.host, self.port)
            self.trade_ctx = OpenTradeContext(self.host, self.port)
            self.connected = True
            print("富途证券已连接")
        except ImportError:
            print("警告: 未安装futu-api，请运行 pip install futu")
            self.connected = False
        except Exception as e:
            print(f"连接富途失败: {e}")
            self.connected = False
    
    def disconnect(self):
        if self.connected:
            self.quote_ctx.close()
            self.trade_ctx.close()
            self.connected = False
    
    def get_account_info(self):
        if not self.connected:
            return None
        # 实现获取账户信息
        return {}
    
    def get_positions(self):
        if not self.connected:
            return {}
        # 实现获取持仓
        return {}
    
    def place_order(self, ts_code, direction, volume, price=None, order_type='limit'):
        if not self.connected:
            return None
        # 实现下单
        return {}
    
    def cancel_order(self, order_id):
        if not self.connected:
            return False
        return False
    
    def get_quote(self, ts_code):
        if not self.connected:
            return None
        # 实现获取行情
        return None


class JoinQuantBroker(BrokerAPI):
    """聚宽API（需安装jqdata）"""
    
    def __init__(self):
        self.connected = False
    
    def connect(self):
        try:
            import jqdata
            self.connected = True
            print("聚宽数据源已连接")
        except ImportError:
            print("警告: 未安装jqdata")
            self.connected = False
    
    def disconnect(self):
        self.connected = False
    
    def get_account_info(self):
        if not self.connected:
            return None
        # 实现获取账户信息
        return {}
    
    def get_positions(self):
        if not self.connected:
            return {}
        return {}
    
    def place_order(self, ts_code, direction, volume, price=None, order_type='limit'):
        if not self.connected:
            return None
        return {}
    
    def cancel_order(self, order_id):
        return False
    
    def get_quote(self, ts_code):
        if not self.connected:
            return None
        return None


class BacktestBroker(BrokerAPI):
    """回测专用Broker"""
    
    def __init__(self, initial_capital=50000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}
        self.orders = []
        self.order_id_counter = 10000
        self.trades = []
        self.equity_curve = []
        self.connected = True
    
    def connect(self):
        self.connected = True
    
    def disconnect(self):
        self.connected = False
    
    def get_account_info(self):
        position_value = sum(
            pos['shares'] * pos.get('current_price', pos['cost']) 
            for pos in self.positions.values()
        )
        return {
            'cash': self.cash,
            'position_value': position_value,
            'total_assets': self.cash + position_value,
            'available_cash': self.cash
        }
    
    def get_positions(self):
        return self.positions
    
    def place_order(self, ts_code, direction, volume, price=None, order_type='limit'):
        order_id = f"BT_{self.order_id_counter}"
        self.order_id_counter += 1
        
        order = {
            'order_id': order_id,
            'ts_code': ts_code,
            'direction': direction,
            'volume': volume,
            'price': price,
            'order_type': order_type,
            'status': 'FILLED',
            'filled_price': price,
            'filled_volume': volume,
            'timestamp': datetime.now()
        }
        
        if direction == 'BUY':
            cost = volume * price * (1 + config.COMMISSION_RATE)
            if self.cash >= cost:
                self.cash -= cost
                if ts_code in self.positions:
                    old = self.positions[ts_code]
                    old_value = old['shares'] * old['cost']
                    new_value = volume * price
                    total_value = old_value + new_value
                    total_shares = old['shares'] + volume
                    self.positions[ts_code] = {
                        'shares': total_shares,
                        'cost': total_value / total_shares,
                        'current_price': price
                    }
                else:
                    self.positions[ts_code] = {
                        'shares': volume,
                        'cost': price,
                        'current_price': price
                    }
                self.trades.append({
                    'ts_code': ts_code,
                    'direction': 'BUY',
                    'volume': volume,
                    'price': price,
                    'cost': cost
                })
        else:
            if ts_code in self.positions and self.positions[ts_code]['shares'] >= volume:
                revenue = volume * price * (1 - config.COMMISSION_RATE - config.STAMP_TAX)
                self.cash += revenue
                self.positions[ts_code]['shares'] -= volume
                if self.positions[ts_code]['shares'] == 0:
                    del self.positions[ts_code]
                self.trades.append({
                    'ts_code': ts_code,
                    'direction': 'SELL',
                    'volume': volume,
                    'price': price,
                    'revenue': revenue
                })
        
        self.orders.append(order)
        return order
    
    def update_prices(self, prices_dict):
        """更新持仓价格（用于计算市值）"""
        for ts_code, price in prices_dict.items():
            if ts_code in self.positions:
                self.positions[ts_code]['current_price'] = price
    
    def get_equity_curve(self):
        """获取权益曲线"""
        return self.equity_curve
    
    def snapshot(self, date):
        """记录快照"""
        position_value = sum(
            pos['shares'] * pos.get('current_price', pos['cost'])
            for pos in self.positions.values()
        )
        total = self.cash + position_value
        self.equity_curve.append({
            'date': date,
            'cash': self.cash,
            'position_value': position_value,
            'total_assets': total
        })
    
    def get_orders(self):
        return self.orders
    
    def cancel_order(self, order_id):
        return False
    
    def get_quote(self, ts_code):
        return None


def create_broker(broker_type='tushare', **kwargs):
    """创建Broker实例"""
    brokers = {
        'tushare': TushareBroker,
        'futu': FutuBroker,
        'joinquant': JoinQuantBroker,
        'backtest': BacktestBroker
    }
    
    if broker_type not in brokers:
        raise ValueError(f"不支持的Broker类型: {broker_type}")
    
    return brokers[broker_type](**kwargs)


if __name__ == "__main__":
    # 测试
    print("测试Tushare Broker")
    broker = create_broker('tushare')
    broker.connect()
    
    # 获取账户信息
    print("\n账户信息:", broker.get_account_info())
    
    # 测试下单
    print("\n测试买入...")
    order = broker.place_order('000001.SZ', 'BUY', 100, 10.0)
    print("订单:", order)
    
    print("\n持仓:", broker.get_positions())
