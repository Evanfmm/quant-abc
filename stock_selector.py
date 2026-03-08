"""
股票池选择器 - 统一管理股票池获取逻辑

支持三种模式：
- test_mode: 冒烟测试，随机采样
- dev_mode: 开发验证，SH/SZ分层采样
- production_mode: 正式推荐，全量候选池（经过滤）
"""
import pandas as pd
import numpy as np
import config
from data_fetcher import get_stock_basic


def get_stock_pool(n=None, exclude_st=True, exclude_new=True, min_turnover=1.0):
    """
    根据运行模式获取股票池
    
    Args:
        n: 可选，强制指定数量（主要用于 test_mode）
        exclude_st: 是否排除 ST 股
        exclude_new: 是否排除新股（60日内）
        min_turnover: 最小换手率要求
    
    Returns:
        DataFrame: 符合筛选条件的股票池
    """
    mode = getattr(config, 'RUN_MODE', 'production')
    
    # 获取全量股票
    stocks = get_stock_basic()
    
    # 基础过滤
    filtered = _apply_basic_filters(stocks, exclude_st, exclude_new, min_turnover)
    
    if mode == "test":
        return _test_mode(filtered, n)
    elif mode == "dev":
        return _dev_mode(filtered, n)
    else:  # production
        return filtered


def _test_mode(stocks, n=None):
    """测试模式：随机采样"""
    n = n or getattr(config, 'TEST_SAMPLE_SIZE', 50)
    return stocks.sample(n=min(n, len(stocks)), random_state=42)


def _dev_mode(stocks, n=None):
    """开发模式：SH/SZ 分层采样"""
    sh_n = n or getattr(config, 'DEV_SH_SAMPLE', 25)
    sz_n = n or getattr(config, 'DEV_SZ_SAMPLE', 25)
    
    sh_stocks = stocks[stocks['ts_code'].str.endswith('.SH')]
    sz_stocks = stocks[stocks['ts_code'].str.endswith('.SZ')]
    
    sh_sample = sh_stocks.sample(n=min(sh_n, len(sh_stocks)), random_state=42)
    sz_sample = sz_stocks.sample(n=min(sz_n, len(sz_stocks)), random_state=42)
    
    return pd.concat([sh_sample, sz_sample])


def _apply_basic_filters(stocks, exclude_st=True, exclude_new=True, min_turnover=1.0):
    """
    基础过滤规则
    
    过滤条件：
    - ST 股票（代码以 ST/ *ST/ S*ST 开头）
    - 新股（上市不足 60 个交易日）
    - 换手率过低（可选）
    - 停牌股票（在实时数据获取时过滤）
    """
    df = stocks.copy()
    
    # 排除 ST
    if exclude_st:
        df = df[~df['name'].str.contains(r'ST|\*ST|S\*ST', na=False, regex=True)]
    
    # 排除新股（60日内）
    if exclude_new:
        try:
            from datetime import datetime, timedelta
            cutoff = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            df = df[df['list_date'] < cutoff]
        except:
            pass  # 若无法获取日期则跳过
    
    return df


def get_stock_pool_stats():
    """获取当前股票池统计"""
    stocks = get_stock_pool()
    
    # 正确分类各市场
    def classify(code):
        if code.startswith('688'):
            return 'KCB'  # 科创板
        elif code.startswith('30'):
            return 'CYB'  # 创业板
        elif code.endswith('.SH'):
            return 'SH'
        elif code.endswith('.SZ'):
            return 'SZ'
        elif code.endswith('.BJ'):
            return 'BJ'
        return 'OTHER'
    
    stocks['market'] = stocks['ts_code'].apply(classify)
    counts = stocks['market'].value_counts().to_dict()
    
    return {
        'total': len(stocks),
        'SH': counts.get('SH', 0),
        'SZ': counts.get('SZ', 0),
        'CYB': counts.get('CYB', 0),
        'KCB': counts.get('KCB', 0),
        'BJ': counts.get('BJ', 0),
        'mode': getattr(config, 'RUN_MODE', 'production')
    }


if __name__ == "__main__":
    # 测试
    print("=== 股票池统计 ===")
    for mode in ['test', 'dev', 'production']:
        config.RUN_MODE = mode
        stats = get_stock_pool_stats()
        print(f"\n{mode}:")
        print(f"  总数: {stats['total']}")
        print(f"  SH: {stats['SH']}")
        print(f"  SZ: {stats['SZ']}")
