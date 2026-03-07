"""
A股多因子量化交易系统 - 短线增强因子模型 v2
根据策略专家建议优化：动量+资金流向+情绪为核心
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config
import technical_indicators as ti

class ShortTermFactorModelV2:
    """短线增强因子模型 v2 - 专家优化版"""
    
    def __init__(self):
        self.factors = config.SHORT_TERM_FACTORS
        self.filters = config.FILTERS
        self.factor_data = None
        
    def prepare_data(self):
        """准备因子数据"""
        print("="*60)
        print("短线增强因子模型 v2 - 专家优化版")
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
        
        # 计算动量因子
        print("正在计算动量因子...")
        df = self._calculate_momentum(df)
        
        # 计算资金流向因子
        print("正在计算资金流向...")
        df = self._calculate_fund_flow(df)
        
        # 计算情绪因子
        print("正在计算情绪因子...")
        df = self._calculate_sentiment(df)
        
        # 计算风险因子
        print("正在计算风险因子...")
        df = self._calculate_risk(df)
        
        self.factor_data = df
        return df
    
    def _calculate_momentum(self, df):
        """计算动量因子"""
        # 使用当日涨跌幅模拟短期动量
        if 'pct_chg' in df.columns:
            df['momentum_5d'] = df['pct_chg'].fillna(0)
            df['momentum_10d'] = df['pct_chg'].fillna(0) * 1.5
            # 突破强度：用涨跌幅绝对值
            df['breakout_strength'] = df['pct_chg'].abs()
        else:
            df['momentum_5d'] = 0
            df['momentum_10d'] = 0
            df['breakout_strength'] = 0
        
        return df
    
    def _calculate_fund_flow(self, df):
        """计算资金流向因子"""
        # 换手率作为资金活跃度指标
        if 'turnover_rate' in df.columns:
            df['turnover_rate'] = df['turnover_rate'].fillna(0)
        else:
            df['turnover_rate'] = 0
        
        # 量比
        if 'volume_ratio' in df.columns:
            df['volume_ratio'] = df['volume_ratio'].fillna(1)
        else:
            df['volume_ratio'] = 1
        
        # 资金流向综合得分（换手率*量比）
        df['fund_flow_score'] = df['turnover_rate'] * df.get('volume_ratio', 1)
        
        return df
    
    def _calculate_sentiment(self, df):
        """计算情绪因子"""
        # 振幅
        if 'pct_chg' in df.columns:
            df['amplitude'] = df['pct_chg'].abs()
            # 价格强度：涨跌幅为正且绝对值较大
            df['price_strength'] = df['pct_chg'].clip(lower=0)
        else:
            df['amplitude'] = 0
            df['price_strength'] = 0
        
        return df
    
    def _calculate_risk(self, df):
        """计算风险因子"""
        # 波动率：使用换手率作为代理（高换手=高波动）
        if 'turnover_rate' in df.columns:
            df['volatility'] = df['turnover_rate'] / 20  # 归一化
        else:
            df['volatility'] = 0.5
        
        # 小市值
        if 'total_mv' in df.columns:
            df['size'] = df['total_mv'].fillna(df['total_mv'].median())
        else:
            df['size'] = 0
        
        return df
    
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
        
        # 过滤换手率（短线需要活跃度）
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
                
                # 标准化处理 - 使用百分位排名
                if params['ascending']:
                    # 越小越好
                    scores[factor_name] = col.rank(ascending=False, pct=True)
                else:
                    # 越大越好
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
        
        # 应用过滤
        df = self.apply_filters(df)
        
        if len(df) == 0:
            print("❌ 过滤后无剩余股票")
            return pd.DataFrame()
        
        # 计算因子得分
        df = self.calculate_factor_scores(df)
        
        # 按得分排序
        df = df.sort_values('factor_score', ascending=False)
        
        # 取前n只
        selected_cols = ['ts_code', 'name', 'industry', 'close', 'pe', 'pb', 
                        'turnover_rate', 'volume_ratio', 'momentum_5d', 'factor_score']
        available_cols = [c for c in selected_cols if c in df.columns]
        selected = df.head(n)[available_cols].copy()
        
        return selected
    
    def get_recommendations(self, n=4, include_tech=True):
        """获取推荐股票
        
        Args:
            n: 推荐股票数量
            include_tech: 是否包含技术指标分析
        """
        selected = self.select_stocks(n)
        
        if selected is None or len(selected) == 0:
            print("⚠️ 未能获取到推荐股票")
            return []
        
        recommendations = []
        
        # 获取每只股票的技术指标
        tech_cache = {}
        if include_tech:
            print("\n正在计算技术指标...")
            codes = selected['ts_code'].tolist()
            for code in codes:
                try:
                    tech = ti.get_stock_technical_indicators(code)
                    if tech:
                        tech_cache[code] = tech
                except Exception as e:
                    print(f"  计算{code}技术指标失败: {e}")
                    continue
        
        for idx, row in selected.iterrows():
            ts_code = row.get('ts_code', 'N/A')
            
            rec = {
                '代码': ts_code,
                '名称': row.get('name', 'N/A'),
                '行业': row.get('industry', 'N/A'),
                '现价': round(row.get('close', 0), 2),
                '换手率': f"{row.get('turnover_rate', 0):.2f}%",
                '量比': f"{row.get('volume_ratio', 1):.2f}",
                '5日涨幅': f"{row.get('momentum_5d', 0):+.2f}%",
                '综合得分': round(row.get('factor_score', 0), 3)
            }
            
            # 添加技术指标信息
            if include_tech and ts_code in tech_cache:
                tech = tech_cache[ts_code]
                action, reason = ti.generate_trading_signal(tech)
                
                rec['MACD'] = tech.get('macd_signal', 'N/A')
                rec['MACD说明'] = tech.get('macd_desc', '')
                rec['KDJ'] = tech.get('kdj_signal', 'N/A')
                rec['KDJ说明'] = tech.get('kdj_desc', '')
                rec['RSI'] = tech.get('rsi_signal', 'N/A')
                rec['RSI说明'] = tech.get('rsi_desc', '')
                rec['操作建议'] = action.upper()
                rec['建议理由'] = reason
            
            recommendations.append(rec)
        
        return recommendations


def test_model():
    """测试模型"""
    print("\n" + "="*60)
    print("测试短线增强因子模型 v2")
    print("="*60 + "\n")
    
    model = ShortTermFactorModelV2()
    results = model.get_recommendations(4)
    
    if results:
        print("\n【推荐股票】")
        print("-" * 85)
        print(f"{'代码':<12}{'名称':<10}{'行业':<12}{'现价':<8}{'换手率':<10}{'量比':<8}{'5日涨幅':<10}{'得分':<6}")
        print("-" * 85)
        for r in results:
            print(f"{r['代码']:<12}{r['名称']:<10}{r['行业']:<12}{r['现价']:<8}"
                  f"{r['换手率']:<10}{r['量比']:<8}{r['5日涨幅']:<10}{r['综合得分']:<6}")
        print("-" * 85)
    else:
        print("❌ 未能获取推荐")
    
    return results


if __name__ == "__main__":
    test_model()
