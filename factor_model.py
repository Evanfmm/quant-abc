"""
A股多因子量化交易系统 - 多因子选股模块
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config

class MultiFactorModel:
    """多因子选股模型"""
    
    def __init__(self):
        self.factors = config.FACTORS
        self.filters = config.FILTERS
        self.factor_data = None
        
    def prepare_data(self):
        """准备因子数据"""
        print("正在获取市场数据...")
        
        # 强制获取最新数据，不使用缓存
        print("正在获取新数据...")
        df = data_fetcher.get_market_overview()
        if len(df) > 0:
            # 获取股票基本信息
            stocks = data_fetcher.get_stock_basic()
            df = df.merge(stocks[['ts_code', 'name', 'industry', 'list_date']], on='ts_code', how='left')
            
            # 过滤停牌和退市
            df = df[df['close'] > 0]
            
            self.factor_data = df
            data_fetcher.save_data(df, "daily_basic_cache")
            print(f"获取到 {len(df)} 只股票数据")
    
    def calculate_momentum(self, df):
        """计算动量因子"""
        # 这里需要历史数据，简化处理
        # 实际应该获取N天的涨跌幅
        df['momentum_5d'] = 0  # 简化
        return df
    
    def apply_filters(self, df):
        """应用过滤条件"""
        if df is None or len(df) == 0:
            return df
        
        original_len = len(df)
        
        # 过滤ST股票
        if self.filters['exclude_st']:
            df = df[~df['name'].str.contains('ST|退', na=False, regex=True)]
        
        # 过滤市盈率 - 只过滤负值
        df = df[df['pe'] > 0]  # 排除亏损
        
        # 过滤市值 - 保留有市值的股票
        df = df[df['total_mv'].notna() & (df['total_mv'] > 0)]
        
        # 过滤价格过低的股票
        df = df[df['close'] > 1]  # 排除1元以下的仙股
        
        print(f"过滤后剩余 {len(df)} 只股票 (过滤了 {original_len - len(df)} 只)")
        
        # 如果过滤后为空，使用宽松条件
        if len(df) < 10:
            print("过滤条件过严，使用宽松条件...")
            df = df[df['pe'] > 0]  # 只排除亏损
            df = df[df['close'] > 0]
            print(f"宽松过滤后剩余 {len(df)} 只股票")
        
        return df
    
    def calculate_factor_scores(self, df):
        """计算因子得分"""
        if df is None or len(df) == 0:
            return df
        
        df = df.copy()
        
        # 确保有必要的列
        required_cols = ['pe', 'pb', 'turnover_rate', 'total_mv']
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0
        
        # 填充缺失值
        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(df[col].median())
        
        # 计算每个因子的得分
        scores = pd.DataFrame(index=df.index)
        
        for factor_name, params in self.factors.items():
            if factor_name == 'momentum_5d':
                # 动量因子需要特殊处理，默认给0.5
                scores[factor_name] = 0.5
                continue
                
            if factor_name in df.columns:
                col = df[factor_name].fillna(0)
                
                # 标准化处理
                if params['ascending']:
                    # 越小越好，排名越靠前得分越高
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
        
        df = self.factor_data.copy()
        
        # 应用过滤
        df = self.apply_filters(df)
        
        if len(df) == 0:
            # 返回空DataFrame但保留列名
            return pd.DataFrame(columns=['ts_code', 'name', 'industry', 'close', 'pe', 'pb', 
                                         'turnover_rate', 'total_mv', 'factor_score'])
        
        # 计算因子得分
        df = self.calculate_factor_scores(df)
        
        # 按得分排序
        df = df.sort_values('factor_score', ascending=False)
        
        # 取前n只
        selected = df.head(n)[['ts_code', 'name', 'industry', 'close', 'pe', 'pb', 
                               'turnover_rate', 'total_mv', 'factor_score']].copy()
        
        return selected
    
    def get_recommendations(self, n=4):
        """获取推荐股票"""
        selected = self.select_stocks(n)
        
        # 检查是否有数据
        if selected is None or len(selected) == 0:
            print("警告: 未能获取到推荐股票")
            return []
        
        recommendations = []
        for idx, row in selected.iterrows():
            recommendations.append({
                '代码': row['ts_code'],
                '名称': row['name'],
                '行业': row['industry'],
                '现价': round(row['close'], 2),
                'PE': round(row['pe'], 2) if pd.notna(row['pe']) else 'N/A',
                'PB': round(row['pb'], 2) if pd.notna(row['pb']) else 'N/A',
                '市值(亿)': round(row['total_mv'] / 10000, 2) if pd.notna(row['total_mv']) else 'N/A',
                '综合得分': round(row['factor_score'], 3)
            })
        
        return recommendations

if __name__ == "__main__":
    model = MultiFactorModel()
    print("开始选股...")
    result = model.get_recommendations(4)
    print("\n推荐股票:")
    for i, stock in enumerate(result, 1):
        print(f"{i}. {stock['代码']} {stock['名称']} - 得分: {stock['综合得分']}")
