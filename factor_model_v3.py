"""
A股多因子量化交易系统 - 短线增强因子模型 v3
集成技术指标的完整版本
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config

# 动态导入技术指标模块
import importlib.util
spec = importlib.util.spec_from_file_location("tech_module", "technical_indicators.py")
tech_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tech_module)


class ShortTermFactorModelV3:
    """短线增强因子模型 v3 - 集成技术指标"""
    
    def __init__(self):
        self.factors = config.SHORT_TERM_FACTORS
        self.filters = config.FILTERS
        self.factor_data = None
        self.technical_cache = {}  # 技术指标缓存
        
    def prepare_data(self):
        """准备因子数据"""
        print("="*60)
        print("短线增强因子模型 v3 - 技术指标集成版")
        print("="*60)
        print("正在获取市场数据...")
        
        # 获取每日基础数据
        df = data_fetcher.get_market_overview()
        
        if len(df) == 0:
            print("❌ 获取数据失败")
            return None
        
        # 获取股票基本信息
        stocks = data_fetcher.get_stock_basic()
        df = df.merge(stocks[['ts_code', 'name', 'industry', 'list_date']], on='ts_code', how='left')
        
        # 过滤停牌和退市
        df = df[df['close'] > 0]
        
        print(f"获取到 {len(df)} 只股票基础数据")
        
        # 计算基础因子
        print("正在计算动量因子...")
        df = self._calculate_momentum(df)
        
        print("正在计算资金流向...")
        df = self._calculate_fund_flow(df)
        
        print("正在计算情绪因子...")
        df = self._calculate_sentiment(df)
        
        print("正在计算风险因子...")
        df = self._calculate_risk(df)
        
        self.factor_data = df
        return df
    
    def _calculate_momentum(self, df):
        """计算动量因子"""
        if 'pct_chg' in df.columns:
            df['momentum_5d'] = df['pct_chg'].fillna(0)
            df['momentum_10d'] = df['pct_chg'].fillna(0) * 1.5
            df['breakout_strength'] = df['pct_chg'].abs()
        else:
            df['momentum_5d'] = 0
            df['momentum_10d'] = 0
            df['breakout_strength'] = 0
        return df
    
    def _calculate_fund_flow(self, df):
        """计算资金流向因子"""
        if 'turnover_rate' in df.columns:
            df['turnover_rate'] = df['turnover_rate'].fillna(0)
        else:
            df['turnover_rate'] = 0
        
        if 'volume_ratio' in df.columns:
            df['volume_ratio'] = df['volume_ratio'].fillna(1)
        else:
            df['volume_ratio'] = 1
        
        df['fund_flow_score'] = df['turnover_rate'] * df.get('volume_ratio', 1)
        return df
    
    def _calculate_sentiment(self, df):
        """计算情绪因子"""
        if 'pct_chg' in df.columns:
            df['amplitude'] = df['pct_chg'].abs()
            df['price_strength'] = df['pct_chg'].clip(lower=0)
        else:
            df['amplitude'] = 0
            df['price_strength'] = 0
        return df
    
    def _calculate_risk(self, df):
        """计算风险因子"""
        if 'turnover_rate' in df.columns:
            df['volatility'] = df['turnover_rate'] / 20
        else:
            df['volatility'] = 0.5
        
        if 'total_mv' in df.columns:
            df['size'] = df['total_mv'].fillna(df['total_mv'].median())
        else:
            df['size'] = 0
        return df
    
    def get_technical_for_stock(self, ts_code):
        """获取单只股票的技术指标"""
        if ts_code in self.technical_cache:
            return self.technical_cache[ts_code]
        
        try:
            # 使用正确的函数名
            indicators = tech_module.get_stock_technical_indicators(ts_code, n_days=60)
            if indicators:
                self.technical_cache[ts_code] = indicators
                return indicators
            return None
        except Exception as e:
            return None
    
    def apply_filters(self, df):
        """应用过滤条件"""
        if df is None or len(df) == 0:
            return df
        
        original_len = len(df)
        
        # 过滤ST股票
        if self.filters.get('exclude_st', True):
            df = df[~df['name'].str.contains('ST|退', na=False, regex=True)]
        
        # 过滤价格
        if 'close' in df.columns:
            min_price = self.filters.get('price_min', 2)
            max_price = self.filters.get('price_max', 150)
            df = df[df['close'] >= min_price]
            df = df[df['close'] <= max_price]
        
        # 过滤换手率
        if 'turnover_rate' in df.columns:
            min_turn = self.filters.get('turnover_min', 1.0)
            max_turn = self.filters.get('turnover_max', 50)
            df = df[df['turnover_rate'] >= min_turn]
            df = df[df['turnover_rate'] <= max_turn]
        
        # 过滤市值
        if 'total_mv' in df.columns:
            df = df[df['total_mv'] > self.filters['market_cap_min']]
            df = df[df['total_mv'] < self.filters['market_cap_max']]
        
        # 过滤新股
        if self.filters.get('exclude_new', True) and 'list_date' in df.columns:
            try:
                cutoff_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
                df = df[df['list_date'] < cutoff_date]
            except:
                pass
        
        print(f"过滤后剩余 {len(df)} 只股票 (过滤了 {original_len - len(df)} 只)")
        return df
    
    def calculate_factor_scores(self, df):
        """计算因子得分"""
        if df is None or len(df) == 0:
            return df
        
        df = df.copy()
        
        # 填充缺失值
        numeric_cols = ['pe', 'pb', 'turnover_rate', 'volume_ratio', 'momentum_5d', 
                      'momentum_10d', 'breakout_strength', 'amplitude', 'price_strength',
                      'volatility', 'size', 'fund_flow_score']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(df[col].median())
        
        # 计算每个因子的得分
        scores = pd.DataFrame(index=df.index)
        
        for factor_name, params in self.factors.items():
            if factor_name in df.columns:
                col = df[factor_name].fillna(0)
                
                if params['ascending']:
                    scores[factor_name] = col.rank(ascending=False, pct=True)
                else:
                    scores[factor_name] = col.rank(ascending=True, pct=True)
            else:
                scores[factor_name] = 0.5
        
        # 计算加权总分
        df['factor_score'] = 0
        for factor_name, params in self.factors.items():
            if factor_name in scores.columns:
                df['factor_score'] += scores[factor_name] * params['weight']
        
        return df
    
    def select_stocks(self, n=5):
        """选股"""
        if self.factor_data is None:
            self.prepare_data()
        
        if self.factor_data is None or len(self.factor_data) == 0:
            print("❌ 无有效数据")
            return pd.DataFrame()
        
        df = self.factor_data.copy()
        df = self.apply_filters(df)
        
        if len(df) == 0:
            print("❌ 过滤后无剩余股票")
            return pd.DataFrame()
        
        # 计算因子得分
        df = self.calculate_factor_scores(df)
        
        # 按得分排序
        df = df.sort_values('factor_score', ascending=False)
        
        # 取前n只
        selected = df.head(n).copy()
        
        # 为每只股票计算技术指标
        print("正在计算技术指标...")
        for idx in selected.index:
            ts_code = selected.loc[idx, 'ts_code']
            tech = self.get_technical_for_stock(ts_code)
            if tech:
                selected.loc[idx, 'macd_signal'] = tech.get('macd_signal', 'N/A')
                selected.loc[idx, 'kdj_signal'] = tech.get('kdj_signal', 'N/A')
                selected.loc[idx, 'rsi_signal'] = tech.get('rsi_signal', 'N/A')
                
                # 生成买卖建议
                action, reason = tech_module.generate_trading_signal(tech)
                selected.loc[idx, 'recommendation'] = action.upper() if action else 'N/A'
                
                # 计算技术评分
                buy_count = sum(1 for x in [tech.get('macd_signal'), tech.get('kdj_signal'), tech.get('rsi_signal')] if x == 'golden')
                sell_count = sum(1 for x in [tech.get('macd_signal'), tech.get('kdj_signal'), tech.get('rsi_signal')] if x in ['dead', 'bearish', 'overbought'])
                selected.loc[idx, 'tech_score'] = buy_count - sell_count
            else:
                selected.loc[idx, 'macd_signal'] = 'N/A'
                selected.loc[idx, 'kdj_signal'] = 'N/A'
                selected.loc[idx, 'rsi_signal'] = 'N/A'
                selected.loc[idx, 'recommendation'] = 'N/A'
                selected.loc[idx, 'tech_score'] = 0
        
        return selected
    
    def get_recommendations(self, n=4):
        """获取推荐股票"""
        selected = self.select_stocks(n)
        
        if selected is None or len(selected) == 0:
            print("⚠️ 未能获取到推荐股票")
            return []
        
        # 买卖点策略参数（来自策略专家设计）
        STOP_LOSS_RATIO = 0.90   # 止损：下跌10%
        TAKE_PROFIT_RATIO = 1.15  # 止盈：上涨15%
        
        recommendations = []
        for idx, row in selected.iterrows():
            current_price = round(row.get('close', 0), 2)
            buy_price = current_price  # 买入价 = 当日收盘价
            stop_loss_price = round(buy_price * STOP_LOSS_RATIO, 2)  # 止损价
            take_profit_price = round(buy_price * TAKE_PROFIT_RATIO, 2)  # 止盈价
            
            rec = {
                '代码': row.get('ts_code', 'N/A'),
                '名称': row.get('name', 'N/A'),
                '行业': row.get('industry', 'N/A'),
                '现价': current_price,
                '换手率': f"{row.get('turnover_rate', 0):.2f}%",
                '5日涨幅': f"{row.get('momentum_5d', 0):+.2f}%",
                'MACD': row.get('macd_signal', 'N/A'),
                'KDJ': row.get('kdj_signal', 'N/A'),
                'RSI': row.get('rsi_signal', 'N/A'),
                '建议': row.get('recommendation', 'N/A'),
                '综合得分': round(row.get('factor_score', 0), 3),
                # 买卖点位（新增）
                '买入价': buy_price,
                '止损价': stop_loss_price,
                '止盈价': take_profit_price,
                '持有期': '1-5个交易日'
            }
            recommendations.append(rec)
        
        return recommendations


def test_model():
    """测试模型"""
    print("\n" + "="*60)
    print("测试短线增强因子模型 v3 - 技术指标集成版")
    print("="*60 + "\n")
    
    model = ShortTermFactorModelV3()
    results = model.get_recommendations(4)
    
    if results:
        print("\n【推荐股票】")
        print("-" * 100)
        print(f"{'代码':<12}{'名称':<10}{'现价':<8}{'换手率':<10}{'MACD':<8}{'KDJ':<8}{'RSI':<8}{'建议':<12}{'得分':<6}")
        print("-" * 100)
        for r in results:
            print(f"{r['代码']:<12}{r['名称']:<10}{r['现价']:<8}{r['换手率']:<10}"
                  f"{r['MACD']:<8}{r['KDJ']:<8}{r['RSI']:<8}{r['建议']:<12}{r['综合得分']:<6}")
        print("-" * 100)
    else:
        print("❌ 未能获取推荐")
    
    return results


if __name__ == "__main__":
    test_model()
