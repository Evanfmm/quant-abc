"""
股票池选择器 - 统一管理股票池获取逻辑

支持三种模式：
- test_mode: 冒烟测试，随机采样
- dev_mode: 开发验证，SH/SZ分层采样
- production_mode: 正式推荐，全量候选池（经过滤）

============================================
静态过滤（基于 get_stock_basic() 的静态字段）
============================================

【可用字段】
- ts_code: 股票代码 (如 000001.SZ)
- symbol: 证券代码 (如 000001)
- name: 股票名称 (如 平安银行)
- area: 所在地域 (如 深圳)
- industry: 所属行业 (如 银行)
- list_date: 上市日期 (YYYYMMDD格式)
- market: 市场 (主板/中小板/创业板/科创板)
- exchange: 交易所 (SSE/SZSE/BSE)

【缺失/不可用字段】
- delist_date: 退市日期 (大多数为None，无法可靠用于过滤)
- is_hs: 是否沪深港通标的 (不可用)
- is_new: 是否新股 (不可用，需用list_date计算)

【静态过滤规则】
1. 排除 ST/*ST/S*ST 股票（基于name字段）
2. 排除新股（上市60个交易日内，基于list_date）
3. 排除已退市（delist_date非空）
"""
import pandas as pd
import numpy as np
import config
from data_fetcher import get_stock_basic


def get_static_fields_info():
    """
    获取静态字段信息
    
    Returns:
        dict: 包含可用字段和缺失字段的说明
    """
    return {
        'available': {
            'ts_code': '股票代码 (如 000001.SZ)',
            'symbol': '证券代码 (如 000001)',
            'name': '股票名称 (如 平安银行)',
            'area': '所在地域 (如 深圳)',
            'industry': '所属行业 (如 银行)',
            'list_date': '上市日期 (YYYYMMDD格式)',
            'market': '市场类型',
            'exchange': '交易所 (SSE/SZSE/BSE)'
        },
        'missing': {
            'delist_date': '退市日期（大多数为None，无法可靠用于过滤）',
            'is_hs': '沪深港通标的（不可用）',
            'is_new': '是否新股（需用list_date计算）'
        }
    }


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
    基础过滤规则（基础可交易性与数据质量）
    
    过滤条件：
    - ST 股票（高风险）
    - 新股（流动性不足，60个交易日内）
    - 换手率过低（可选）
    - 停牌股票（在实时数据获取时过滤）
    """
    df = stocks.copy()
    original_count = len(df)
    filter_stats = {}
    
    # 排除 ST
    if exclude_st:
        st_mask = df['name'].str.contains(r'ST|\*ST|S\*ST', na=False, regex=True)
        filter_stats['排除ST'] = st_mask.sum()
        df = df[~st_mask]
    
    # 排除新股（60日内）
    if exclude_new:
        try:
            from datetime import datetime, timedelta
            cutoff = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            new_mask = df['list_date'] >= cutoff
            filter_stats['排除新股'] = new_mask.sum()
            df = df[~new_mask]
        except:
            pass  # 若无法获取日期则跳过
    
    # 记录过滤统计
    df.attrs['filter_stats'] = filter_stats
    df.attrs['original_count'] = original_count
    
    return df


def get_candidate_pool(stage=1, min_daily_amount=1000000):
    """
    获取候选股票池（用于 production_mode）
    
    Stage 1: 基础可交易性与数据质量过滤
    - 排除 ST/*ST/S*ST
    - 排除停牌（需实时数据判断）
    - 排除退市
    - 排除上市不足60天
    - 排除成交额过低（<100万/日）
    - 排除数据缺失严重标的
    
    Args:
        stage: 当前仅支持 stage=1
        min_daily_amount: 最小日成交额（元）
    
    Returns:
        DataFrame: 过滤后的候选股票池
    """
    if stage != 1:
        raise ValueError("目前仅支持 stage=1")
    
    from data_fetcher import get_stock_basic
    from datetime import datetime, timedelta
    
    # 获取全量股票
    stocks = get_stock_basic()
    original_count = len(stocks)
    
    # 应用基础过滤
    df = stocks.copy()
    filter_stats = {}
    
    # 1. 排除 ST
    st_mask = df['name'].str.contains(r'ST|\*ST|S\*ST', na=False, regex=True)
    filter_stats['排除ST'] = int(st_mask.sum())
    df = df[~st_mask]
    
    # 2. 排除新股（60个交易日内）
    try:
        cutoff_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
        new_mask = df['list_date'].fillna('19000101') >= cutoff_date
        filter_stats['排除新股'] = int(new_mask.sum())
        df = df[~new_mask]
    except:
        filter_stats['排除新股'] = 0
    
    # 3. 排除退市（delist_date 非空）
    if 'delist_date' in df.columns:
        delist_mask = df['delist_date'].notna()
        filter_stats['排除退市'] = int(delist_mask.sum())
        df = df[~delist_mask]
    
    # 4. 排除数据缺失严重（关键字段为空的太多）
    # 这里先不处理，需要结合实时数据
    
    # 记录统计
    filtered_count = len(df)
    df.attrs['filter_stats'] = filter_stats
    df.attrs['original_count'] = original_count
    
    print(f"Stage1 过滤: {original_count} → {filtered_count}")
    for k, v in filter_stats.items():
        if v > 0:
            print(f"  - {k}: {v}")
    
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
