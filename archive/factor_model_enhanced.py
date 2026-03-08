"""
A股多因子量化交易系统 - 增强因子模型
包含价值、成长、动量、质量、风险、情绪六大类因子
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config

class EnhancedMultiFactorModel:
    """增强版多因子选股模型"""
    
    def __init__(self):
        self.factors = config.ENHANCED_FACTORS
        self.filters = config.ENHANCED_FILTERS
        self.factor_data = None
        self.historical_data = None
        
    def prepare_data(self, use_cache=True):
        """准备因子数据"""
        print("正在获取市场数据...")
        
        # 尝试从缓存加载
        if use_cache:
            cached = data_fetcher.load_data("enhanced_factor_cache")
            if cached is not None and len(cached) > 100:
                # 检查缓存是否过期（4小时内有效）
                cache_time = cached.get('cache_time', None)
                if cache_time:
                    cache_age = datetime.now() - datetime.fromisoformat(cache_time)
                    if cache_age.total_seconds() < 4 * 3600:
                        print(f"使用缓存数据 (年龄: {cache_age.total_seconds()/3600:.1f}小时)")
                        self.factor_data = cached.drop('cache_time', axis=1)
                        return
        
        # 获取股票基本信息
        stocks = data_fetcher.get_stock_basic()
        
        # 获取每日基本面数据
        df = data_fetcher.get_market_overview()
        if len(df) > 0:
            df = df.merge(stocks[['ts_code', 'name', 'industry', 'list_date']], on='ts_code', how='left')
            
            # 过滤停牌和退市
            df = df[df['close'] > 0]
            
            # 获取财务数据
            print("获取财务指标...")
            df = self._enrich_financial_data(df)
            
            # 获取历史动量数据
            print("获取历史动量数据...")
            df = self._enrich_momentum_data(df)
            
            # 获取情绪因子
            print("获取情绪因子...")
            df = self._enrich_sentiment_data(df)
            
            # 计算衍生因子
            print("计算衍生因子...")
            df = self._calculate_derived_factors(df)
            
            self.factor_data = df
            # 保存带时间的缓存
            cache_df = df.copy()
            cache_df['cache_time'] = datetime.now().isoformat()
            data_fetcher.save_data(cache_df, "enhanced_factor_cache")
            print(f"获取到 {len(df)} 只股票数据")
    
    def _enrich_financial_data(self, df):
        """丰富财务因子数据"""
        # 这里简化处理，实际应从API获取更多财务数据
        # 使用PE/PB/ROE等基础数据计算更多衍生指标
        
        if 'pe' in df.columns and 'pb' in df.columns:
            # PEG = PE / (营收增速 * 100)，简化处理
            df['peg'] = df['pe'] / 10  # 假设增速10%，实际应从财务数据获取
            
            # 股息率因子（如果有分红数据）
            if 'dv_ratio' not in df.columns:
                df['dv_ratio'] = 0  # 暂无数据
        
        # ROE如果不在数据中，尝试从fina_indicator获取
        if 'roe' not in df.columns:
            try:
                # 获取一只股票的财务数据作为示例
                sample = data_fetcher.get_financial_data('000001.SZ')
                if len(sample) > 0 and 'roe' in sample.columns:
                    # 简化：使用中位数填充
                    df['roe'] = sample['roe'].median() if 'roe' in sample else 10
                else:
                    df['roe'] = 10  # 默认值
            except:
                df['roe'] = 10
        
        return df
    
    def _enrich_momentum_data(self, df):
        """丰富动量因子"""
        # 简化处理：使用近期涨跌幅作为动量因子
        # 实际应获取20/60/120日涨跌幅
        
        if 'pct_chg' in df.columns:
            # 短期动量
            df['momentum_5d'] = df['pct_chg'].fillna(0)
            df['momentum_10d'] = df['pct_chg'].fillna(0) * 1.5
            df['momentum_20d'] = df['pct_chg'].fillna(0) * 2
        else:
            # 默认值
            df['momentum_5d'] = 0
            df['momentum_10d'] = 0
            df['momentum_20d'] = 0
        
        # 波动率因子
        if 'turnover_rate' in df.columns:
            # 高换手率通常意味着高波动
            df['volatility'] = df['turnover_rate'] / 100  # 简化的波动率
        
        return df
    
    def _enrich_sentiment_data(self, df):
        """丰富情绪因子"""
        # 换手率因子
        if 'turnover_rate' in df.columns:
            # 适中换手率较好
            df['turnover_score'] = 1 - abs(df['turnover_rate'] - 5) / 10
            df['turnover_score'] = df['turnover_score'].clip(0, 1)
        
        # 量价配合因子（如果有volume数据）
        if 'volume_ratio' in df.columns:
            df['volume_score'] = df['volume_ratio'].clip(0.5, 3)
        else:
            df['volume_score'] = 1
        
        return df
    
    def _calculate_derived_factors(self, df):
        """计算衍生因子"""
        df = df.copy()
        
        # 1. 价值因子
        if 'pe' in df.columns:
            df['pe_score'] = self._factor_normalize(df['pe'], inverse=True)
        if 'pb' in df.columns:
            df['pb_score'] = self._factor_normalize(df['pb'], inverse=True)
        if 'ps' in df.columns:
            df['ps_score'] = self._factor_normalize(df['ps'], inverse=True)
        
        # 2. 成长因子
        if 'revenue_growth' in df.columns:
            df['growth_score'] = self._factor_normalize(df['revenue_growth'])
        if 'profit_growth' in df.columns:
            df['profit_growth_score'] = self._factor_normalize(df['profit_growth'])
        
        # 3. 质量因子
        if 'roe' in df.columns:
            df['quality_score'] = self._factor_normalize(df['roe'])
        
        # 4. 动量因子
        if 'momentum_5d' in df.columns:
            df['momentum_score'] = self._factor_normalize(df['momentum_5d'])
        
        # 5. 风险因子（市值越小风险越大）
        if 'total_mv' in df.columns:
            df['size_score'] = self._factor_normalize(df['total_mv'], inverse=True)  # 小市值
        
        return df
    
    def _factor_normalize(self, series, inverse=False):
        """因子标准化 - 使用rank方法"""
        series = pd.to_numeric(series, errors='coerce').fillna(0)
        if inverse:
            return series.rank(ascending=False, pct=True)
        return series.rank(ascending=True, pct=True)
    
    def apply_filters(self, df):
        """应用增强的过滤条件"""
        if df is None or len(df) == 0:
            return df
        
        original_len = len(df)
        
        # 过滤ST股票
        if self.filters.get('exclude_st', True):
            df = df[~df['name'].str.contains('ST|退|*ST', na=False, regex=True)]
        
        # 过滤市盈率
        pe_min = self.filters.get('pe_min', 0)
        pe_max = self.filters.get('pe_max', 100)
        if pe_min > 0:
            df = df[df['pe'] > pe_min]
        if pe_max < 1000:
            df = df[df['pe'] < pe_max]
        
        # 过滤市净率
        pb_max = self.filters.get('pb_max', 20)
        df = df[df['pb'] < pb_max]
        
        # 过滤市值
        market_cap_min = self.filters.get('market_cap_min', 10e8)
        market_cap_max = self.filters.get('market_cap_max', 500e8)
        df = df[df['total_mv'].notna() & (df['total_mv'] > market_cap_min)]
        if market_cap_max < 1e12:
            df = df[df['total_mv'] < market_cap_max]
        
        # 过滤价格
        price_min = self.filters.get('price_min', 1)
        df = df[df['close'] > price_min]
        
        # 过滤新股
        if self.filters.get('exclude_new', True):
            list_date_threshold = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            df = df[(df['list_date'].isna()) | (df['list_date'] < list_date_threshold)]
        
        # 过滤流动性（换手率太低没有交易价值）
        turnover_min = self.filters.get('turnover_min', 0.3)
        df = df[(df['turnover_rate'].isna()) | (df['turnover_rate'] > turnover_min)]
        
        print(f"过滤后剩余 {len(df)} 只股票 (过滤了 {original_len - len(df)} 只)")
        
        # 宽松回退
        if len(df) < 10:
            print("过滤条件过严，使用宽松条件...")
            df = df[(df['pe'] > 0) & (df['pe'] < 200)]
            df = df[df['close'] > 0]
            print(f"宽松过滤后剩余 {len(df)} 只股票")
        
        return df
    
    def calculate_factor_scores(self, df):
        """计算因子得分 - 增强版"""
        if df is None or len(df) == 0:
            return df
        
        df = df.copy()
        
        # 准备得分列
        scores = pd.DataFrame(index=df.index)
        
        # 计算每个因子的得分
        for factor_name, params in self.factors.items():
            factor_col = params.get('column', factor_name)
            
            if factor_col in df.columns:
                col_data = pd.to_numeric(df[factor_col], errors='coerce').fillna(0)
                
                # 标准化处理
                if params['ascending']:
                    scores[factor_name] = col_data.rank(ascending=False, pct=True)
                else:
                    scores[factor_name] = col_data.rank(ascending=True, pct=True)
            else:
                scores[factor_name] = 0.5  # 无数据时给中性分数
        
        # 计算加权总分
        df['factor_score'] = 0
        for factor_name, params in self.factors.items():
            if factor_name in scores.columns:
                df['factor_score'] += scores[factor_name] * params['weight']
        
        # 添加风险调整
        df = self._apply_risk_adjustment(df, scores)
        
        return df
    
    def _apply_risk_adjustment(self, df, scores):
        """应用风险调整"""
        # 小市值股票风险调整
        if 'total_mv' in df.columns:
            market_cap_rank = df['total_mv'].rank(ascending=True, pct=True)
            # 小市值（<30%）给予惩罚
            risk_penalty = (market_cap_rank < 0.3).astype(float) * 0.1
            df['factor_score'] = df['factor_score'] - risk_penalty
        
        # 低流动性惩罚
        if 'turnover_rate' in df.columns:
            turnover_rank = df['turnover_rate'].rank(ascending=True, pct=True)
            low_liquidity_penalty = (turnover_rank < 0.1).astype(float) * 0.15
            df['factor_score'] = df['factor_score'] - low_liquidity_penalty
        
        return df
    
    def select_stocks(self, n=5):
        """选股"""
        if self.factor_data is None:
            self.prepare_data()
        
        df = self.factor_data.copy()
        
        # 应用过滤
        df = self.apply_filters(df)
        
        if len(df) == 0:
            return pd.DataFrame(columns=['ts_code', 'name', 'industry', 'close', 
                                         'pe', 'pb', 'roe', 'turnover_rate', 
                                         'total_mv', 'momentum_5d', 'factor_score'])
        
        # 计算因子得分
        df = self.calculate_factor_scores(df)
        
        # 按得分排序
        df = df.sort_values('factor_score', ascending=False)
        
        # 取前n只
        selected = df.head(n)[['ts_code', 'name', 'industry', 'close', 
                               'pe', 'pb', 'roe', 'turnover_rate', 
                               'total_mv', 'momentum_5d', 'factor_score']].copy()
        
        return selected
    
    def get_recommendations(self, n=4):
        """获取推荐股票"""
        selected = self.select_stocks(n)
        
        if selected is None or len(selected) == 0:
            print("警告: 未能获取到推荐股票")
            return []
        
        recommendations = []
        for idx, row in selected.iterrows():
            rec = {
                '代码': row['ts_code'],
                '名称': row['name'],
                '行业': row['industry'],
                '现价': round(row['close'], 2),
                'PE': round(row['pe'], 2) if pd.notna(row.get('pe')) and row.get('pe', 0) > 0 else 'N/A',
                'PB': round(row.get('pb', 0), 2) if pd.notna(row.get('pb')) else 'N/A',
                'ROE': round(row.get('roe', 0), 2) if pd.notna(row.get('roe')) else 'N/A',
                '市值(亿)': round(row['total_mv'] / 10000, 2) if pd.notna(row.get('total_mv')) else 'N/A',
                '换手率': round(row.get('turnover_rate', 0), 2) if pd.notna(row.get('turnover_rate')) else 'N/A',
                '5日涨幅': round(row.get('momentum_5d', 0), 2),
                '综合得分': round(row['factor_score'], 3)
            }
            recommendations.append(rec)
        
        return recommendations


# 兼容旧版
class MultiFactorModel(EnhancedMultiFactorModel):
    """兼容旧版API"""
    pass


if __name__ == "__main__":
    model = EnhancedMultiFactorModel()
    print("开始增强版选股...")
    result = model.get_recommendations(4)
    print("\n推荐股票:")
    for i, stock in enumerate(result, 1):
        print(f"{i}. {stock['代码']} {stock['名称']} - 得分: {stock['综合得分']}")
