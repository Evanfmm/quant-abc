"""
A股量化交易系统 - 机器学习预测模块
使用LightGBM回归模型预测未来收益，支持随机森林备选

功能：
1. 数据预处理（缺失值处理、标准化）
2. 时间序列划分训练/验证集
3. LightGBM/RandomForest模型训练与预测
4. 与因子分数融合
5. 评估指标（IC、RMSE、回测收益）
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import warnings
warnings.filterwarnings('ignore')

# 尝试导入LightGBM
try:
    import lightgbm as lgb
    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False
    print("警告: lightgbm未安装，将使用RandomForest作为默认模型")

import config
import data_fetcher


class MLPredictor:
    """
    机器学习预测器 - 预测股票未来收益
    
    使用LightGBM或RandomForest回归模型预测未来N日收益率，
    并支持与因子分数融合生成最终选股信号。
    """
    
    def __init__(self, model_type='lightgbm', params=None):
        """
        初始化预测器
        
        Args:
            model_type: 'lightgbm' 或 'randomforest'
            params: 模型参数字典
        """
        self.model_type = model_type if (model_type == 'randomforest' or HAS_LIGHTGBM) else 'randomforest'
        self.params = params or self._get_default_params()
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.is_fitted = False
        
    def _get_default_params(self):
        """获取默认模型参数"""
        if self.model_type == 'lightgbm':
            return {
                'objective': 'regression',
                'metric': 'rmse',
                'boosting_type': 'gbdt',
                'num_leaves': 31,
                'learning_rate': 0.05,
                'feature_fraction': 0.8,
                'bagging_fraction': 0.8,
                'bagging_freq': 5,
                'verbose': -1,
                'n_estimators': 100,
                'min_child_samples': 20,
                'reg_alpha': 0.1,
                'reg_lambda': 0.1,
            }
        else:  # randomforest
            return {
                'n_estimators': 100,
                'max_depth': 10,
                'min_samples_split': 10,
                'min_samples_leaf': 5,
                'random_state': 42,
                'n_jobs': -1,
            }
    
    def prepare_features(self, df, target_days=5):
        """
        准备特征和目标变量
        
        Args:
            df: 原始股票数据DataFrame
            target_days: 预测未来N日的收益
            
        Returns:
            处理后的DataFrame，包含特征和目标变量
        """
        df = df.copy()
        
        # 确保必要的列存在
        required_cols = ['ts_code', 'trade_date', 'close']
        for col in required_cols:
            if col not in df.columns:
                print(f"警告: 缺少必要列 {col}")
                return pd.DataFrame()
        
        # 按股票和日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算未来N日收益率（目标变量）
        df['future_return'] = df.groupby('ts_code')['close'].pct_change(-target_days) * 100
        
        # 构建技术特征
        for col in ['close', 'vol', 'open', 'high', 'low']:
            if col in df.columns:
                # 基础特征
                df[f'{col}_lag1'] = df.groupby('ts_code')[col].shift(1)
                df[f'{col}_lag5'] = df.groupby('ts_code')[col].shift(5)
                
                # 移动平均
                df[f'{col}_ma5'] = df.groupby('ts_code')[col].transform(
                    lambda x: x.rolling(5, min_periods=1).mean()
                )
                df[f'{col}_ma10'] = df.groupby('ts_code')[col].transform(
                    lambda x: x.rolling(10, min_periods=1).mean()
                )
                
                # 波动率
                if col == 'close':
                    df[f'{col}_volatility'] = df.groupby('ts_code')[col].transform(
                        lambda x: x.pct_change().rolling(5, min_periods=1).std()
                    )
                
                # 价格变化率
                df[f'{col}_pct_change'] = df.groupby('ts_code')[col].pct_change()
        
        # 动量特征
        if 'close' in df.columns:
            df['momentum_5d'] = df.groupby('ts_code')['close'].pct_change(5) * 100
            df['momentum_10d'] = df.groupby('ts_code')['close'].pct_change(10) * 100
        
        # 量价特征
        if 'vol' in df.columns and 'close' in df.columns:
            df['volume_price_ratio'] = df['vol'] / (df['vol'].rolling(5, min_periods=1).mean() + 1)
        
        # 如果有因子数据，也作为特征
        factor_cols = ['pe', 'pb', 'turnover_rate', 'total_mv', 'roe', 'revenue_growth']
        for col in factor_cols:
            if col in df.columns:
                df[f'{col}_feature'] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def select_features(self, df):
        """
        选择特征列
        
        Args:
            df: 特征DataFrame
            
        Returns:
            特征列名列表
        """
        # 排除非特征列
        exclude_cols = ['ts_code', 'trade_date', 'future_return', 'name', 'industry', 
                       'list_date', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        
        feature_cols = []
        for col in df.columns:
            if col not in exclude_cols and df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
                feature_cols.append(col)
        
        return feature_cols
    
    def handle_missing_values(self, df, feature_cols):
        """
        处理缺失值
        
        Args:
            df: 数据DataFrame
            feature_cols: 特征列名列表
            
        Returns:
            处理后的DataFrame
        """
        df = df.copy()
        
        # 用中位数填充数值型缺失值
        for col in feature_cols:
            if col in df.columns:
                median_val = df[col].median()
                if pd.isna(median_val):
                    median_val = 0
                df[col] = df[col].fillna(median_val)
        
        # 移除目标变量为空的行
        if 'future_return' in df.columns:
            df = df[df['future_return'].notna()]
        
        # 移除仍有缺失值的行
        df = df.dropna(subset=feature_cols)
        
        return df
    
    def train_test_split_timeseries(self, df, test_ratio=0.2):
        """
        时间序列划分训练集和测试集
        
        Args:
            df: 数据DataFrame
            test_ratio: 测试集比例
            
        Returns:
            (train_df, test_df)
        """
        # 按时间排序
        df = df.sort_values('trade_date')
        
        n = len(df)
        split_idx = int(n * (1 - test_ratio))
        
        train_df = df.iloc[:split_idx].copy()
        test_df = df.iloc[split_idx:].copy()
        
        print(f"时间序列划分: 训练集 {len(train_df)} 样本, 测试集 {len(test_df)} 样本")
        
        return train_df, test_df
    
    def fit(self, df, target_days=5, test_ratio=0.2):
        """
        训练模型
        
        Args:
            df: 原始股票数据DataFrame
            target_days: 预测未来N日的收益
            test_ratio: 测试集比例
            
        Returns:
            self
        """
        print(f"开始训练 {self.model_type} 模型...")
        
        # 准备特征
        df_features = self.prepare_features(df, target_days)
        if len(df_features) == 0:
            print("错误: 特征准备失败")
            return self
        
        # 选择特征
        self.feature_columns = self.select_features(df_features)
        print(f"使用 {len(self.feature_columns)} 个特征")
        
        # 处理缺失值
        df_features = self.handle_missing_values(df_features, self.feature_columns)
        print(f"处理后数据量: {len(df_features)}")
        
        if len(df_features) < 100:
            print("错误: 数据量不足")
            return self
        
        # 时间序列划分
        train_df, test_df = self.train_test_split_timeseries(df_features, test_ratio)
        
        # 准备训练数据
        X_train = train_df[self.feature_columns].values
        y_train = train_df['future_return'].values
        
        # 标准化
        X_train_scaled = self.scaler.fit_transform(X_train)
        
        # 训练模型
        if self.model_type == 'lightgbm' and HAS_LIGHTGBM:
            self.model = lgb.LGBMRegressor(**self.params)
            self.model.fit(X_train_scaled, y_train)
        else:
            self.model = RandomForestRegressor(**self.params)
            self.model.fit(X_train_scaled, y_train)
        
        self.is_fitted = True
        
        # 在测试集上评估
        if len(test_df) > 0:
            X_test = test_df[self.feature_columns].values
            y_test = test_df['future_return'].values
            X_test_scaled = self.scaler.transform(X_test)
            
            y_pred = self.model.predict(X_test_scaled)
            
            # 计算评估指标
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            
            # 计算IC (Information Coefficient)
            ic = np.corrcoef(y_test, y_pred)[0, 1]
            
            print(f"测试集评估结果:")
            print(f"  RMSE: {rmse:.4f}")
            print(f"  IC: {ic:.4f}")
        
        print("模型训练完成")
        return self
    
    def predict(self, df):
        """
        预测未来收益
        
        Args:
            df: 待预测的股票数据DataFrame
            
        Returns:
            预测结果DataFrame，包含股票代码和预测收益
        """
        if not self.is_fitted:
            print("错误: 模型未训练，请先调用fit()")
            return pd.DataFrame()
        
        # 准备特征
        df_features = self.prepare_features(df, target_days=5)
        if len(df_features) == 0:
            return pd.DataFrame()
        
        # 获取最新数据（每个股票的最后一条）
        df_features = df_features.groupby('ts_code').last().reset_index()
        
        # 处理缺失值
        df_features = self.handle_missing_values(df_features, self.feature_columns)
        
        if len(df_features) == 0:
            return pd.DataFrame()
        
        # 准备特征
        X = df_features[self.feature_columns].values
        
        # 标准化
        X_scaled = self.scaler.transform(X)
        
        # 预测
        predictions = self.model.predict(X_scaled)
        
        # 返回结果
        result = df_features[['ts_code']].copy()
        result['predicted_return'] = predictions
        
        return result
    
    def predict_with_factor_fusion(self, df, factor_scores, fusion_method='weighted', 
                                     ml_weight=0.5, factor_weight=0.5):
        """
        融合机器学习预测与因子分数
        
        Args:
            df: 股票数据DataFrame
            factor_scores: 因子分数DataFrame，需包含 'ts_code' 和 'factor_score' 列
            fusion_method: 'weighted' (加权平均) 或 'rank' (排名融合)
            ml_weight: 机器学习预测权重
            factor_weight: 因子分数权重
            
        Returns:
            融合后的结果DataFrame
        """
        # 获取ML预测
        ml_predictions = self.predict(df)
        
        if len(ml_predictions) == 0:
            print("警告: ML预测为空，返回原始因子分数")
            return factor_scores
        
        # 合并数据
        if 'factor_score' not in factor_scores.columns:
            print("警告: 因子分数中无factor_score列")
            factor_scores = factor_scores.copy()
            factor_scores['factor_score'] = 0.5
        
        merged = ml_predictions.merge(
            factor_scores[['ts_code', 'factor_score']], 
            on='ts_code', 
            how='outer'
        )
        
        # 填充缺失值
        merged['predicted_return'] = merged['predicted_return'].fillna(0)
        merged['factor_score'] = merged['factor_score'].fillna(0.5)
        
        if fusion_method == 'weighted':
            # 加权融合
            merged['final_score'] = (
                merged['predicted_return'].rank(pct=True) * ml_weight + 
                merged['factor_score'] * factor_weight
            )
        else:
            # 排名融合
            merged['ml_rank'] = merged['predicted_return'].rank(pct=True)
            merged['factor_rank'] = merged['factor_score'].rank(pct=True)
            merged['final_score'] = (merged['ml_rank'] + merged['factor_rank']) / 2
        
        return merged.sort_values('final_score', ascending=False)
    
    def evaluate_ic(self, df):
        """
        计算IC (Information Coefficient)
        
        Args:
            df: 包含真实收益和预测收益的DataFrame
            
        Returns:
            IC值
        """
        if 'future_return' not in df.columns or 'predicted_return' not in df.columns:
            return None
        
        # 移除缺失值
        valid_df = df.dropna(subset=['future_return', 'predicted_return'])
        
        if len(valid_df) < 10:
            return None
        
        ic = np.corrcoef(valid_df['future_return'], valid_df['predicted_return'])[0, 1]
        return ic
    
    def evaluate_rmse(self, df):
        """
        计算RMSE (Root Mean Square Error)
        
        Args:
            df: 包含真实收益和预测收益的DataFrame
            
        Returns:
            RMSE值
        """
        if 'future_return' not in df.columns or 'predicted_return' not in df.columns:
            return None
        
        valid_df = df.dropna(subset=['future_return', 'predicted_return'])
        
        if len(valid_df) < 10:
            return None
        
        rmse = np.sqrt(mean_squared_error(valid_df['future_return'], valid_df['predicted_return']))
        return rmse
    
    def backtest_returns(self, df, top_n=10):
        """
        回测收益对比 - 模拟按预测排名买入的效果
        
        Args:
            df: 包含预测收益的DataFrame
            top_n: 买入排名前N的股票
            
        Returns:
            回测收益统计字典
        """
        if 'predicted_return' not in df.columns:
            return None
        
        # 按预测收益排序
        df_sorted = df.sort_values('predicted_return', ascending=False)
        
        # 选取前N只
        top_stocks = df_sorted.head(top_n)
        
        if len(top_stocks) == 0:
            return None
        
        # 计算平均预测收益
        avg_predicted = top_stocks['predicted_return'].mean()
        
        # 如果有真实收益
        if 'future_return' in top_stocks.columns:
            actual_returns = top_stocks['future_return'].dropna()
            if len(actual_returns) > 0:
                avg_actual = actual_returns.mean()
            else:
                avg_actual = None
        else:
            avg_actual = None
        
        return {
            'top_n': top_n,
            'avg_predicted_return': avg_predicted,
            'avg_actual_return': avg_actual,
            'stocks': top_stocks['ts_code'].tolist()
        }
    
    def get_feature_importance(self):
        """
        获取特征重要性
        
        Returns:
            特征重要性DataFrame
        """
        if not self.is_fitted or self.model is None:
            return pd.DataFrame()
        
        if self.model_type == 'lightgbm':
            importance = self.model.feature_importances_
        else:
            importance = self.model.feature_importances_
        
        importance_df = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        return importance_df


def get_training_data(n_days=60):
    """
    获取训练数据
    
    Args:
        n_days: 获取最近N天的数据
        
    Returns:
        股票数据DataFrame
    """
    print(f"获取最近{n_days}天的训练数据...")
    
    # 获取历史日线数据
    df = data_fetcher.get_recent_daily_data(n_days=n_days, batch_size=30)
    
    if len(df) == 0:
        print("获取数据失败，使用模拟数据...")
        # 生成模拟数据用于测试
        dates = pd.date_range(end=datetime.now(), periods=n_days, freq='D')
        stocks = ['000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ']
        
        data = []
        for ts_code in stocks:
            for date in dates:
                data.append({
                    'ts_code': ts_code,
                    'trade_date': date.strftime('%Y%m%d'),
                    'close': np.random.uniform(10, 100),
                    'open': np.random.uniform(10, 100),
                    'high': np.random.uniform(10, 100),
                    'low': np.random.uniform(10, 100),
                    'vol': np.random.randint(1000000, 10000000),
                })
        
        df = pd.DataFrame(data)
    
    # 合并基本面数据
    try:
        stocks_basic = data_fetcher.get_stock_basic()
        if stocks_basic is not None and len(stocks_basic) > 0:
            basic_cols = ['ts_code', 'pe', 'pb', 'total_mv']
            available_cols = [c for c in basic_cols if c in stocks_basic.columns]
            if available_cols:
                df = df.merge(stocks_basic[available_cols], on='ts_code', how='left')
    except Exception as e:
        print(f"获取基本面数据失败: {e}")
    
    print(f"获取到 {len(df)} 条数据")
    return df


def train_and_evaluate(n_days=60, model_type='lightgbm'):
    """
    训练并评估模型
    
    Args:
        n_days: 训练数据天数
        model_type: 模型类型
        
    Returns:
        训练好的预测器
    """
    # 获取数据
    df = get_training_data(n_days)
    
    if len(df) == 0:
        print("错误: 无法获取训练数据")
        return None
    
    # 初始化预测器
    predictor = MLPredictor(model_type=model_type)
    
    # 训练
    predictor.fit(df, target_days=5, test_ratio=0.2)
    
    # 输出特征重要性
    importance_df = predictor.get_feature_importance()
    if len(importance_df) > 0:
        print("\n特征重要性 (Top 10):")
        print(importance_df.head(10).to_string(index=False))
    
    return predictor


def predict_with_fusion(predictor, current_df, factor_df, fusion_method='weighted'):
    """
    使用融合方法进行预测
    
    Args:
        predictor: 训练好的MLPredictor
        current_df: 当前股票数据
        factor_df: 因子分数DataFrame
        fusion_method: 融合方法
        
    Returns:
        融合后的结果DataFrame
    """
    if predictor is None:
        print("错误: 预测器未训练")
        return factor_df
    
    result = predictor.predict_with_factor_fusion(
        current_df, 
        factor_df, 
        fusion_method=fusion_method,
        ml_weight=0.5,
        factor_weight=0.5
    )
    
    return result


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("机器学习预测模块测试")
    print("=" * 50)
    
    # 训练模型
    predictor = train_and_evaluate(n_days=60, model_type='lightgbm')
    
    if predictor:
        print("\n模型训练成功!")
        
        # 显示特征重要性
        importance = predictor.get_feature_importance()
        if len(importance) > 0:
            print("\n特征重要性 (Top 10):")
            print(importance.head(10))
    else:
        print("模型训练失败")
