"""
A股多因子量化交易系统 - 数据获取模块（增强版）
支持缓存、热数据、批量获取

============================================
动态过滤（基于行情数据的实时字段）
============================================

【可用字段】
- close: 收盘价 (可用于判断停牌: close=0)
- pct_chg: 涨跌幅
- vol: 成交量 (手)
- amount: 成交额 (千元)
- turnover_rate: 换手率
- volume_ratio: 量比

【动态过滤规则】
1. 排除停牌（close=0 或 close为NaN）
2. 排除低成交（amount<100万，即amount<1000千元）
3. 排除关键数据缺失（close/pct_chg/vol/amount任一为空）
"""
import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import config
from cache_manager import get_cache

# 设置tushare token
ts.set_token(config.TUSHARE_TOKEN)
pro = ts.pro_api()

# 全局缓存
_cache = get_cache()

# 动态过滤常量
MIN_DAILY_AMOUNT = 1_000_000  # 最小日成交额100万元


def apply_dynamic_filter(prices, min_amount=MIN_DAILY_AMOUNT):
    """
    动态过滤 - 基于行情数据过滤
    
    在获取行情数据后执行，排除：
    1. 停牌（close=0 或 close为NaN）
    2. 低成交（amount<100万）
    3. 关键数据缺失（close/pct_chg/vol/amount任一为空）
    
    Args:
        prices: 行情数据DataFrame
        min_amount: 最小日成交额（元），默认100万
    
    Returns:
        tuple: (过滤后的DataFrame, filter_stats统计字典)
    """
    if prices is None or len(prices) == 0:
        return pd.DataFrame(), {'原始数据': 0}
    
    df = prices.copy()
    original_count = len(df)
    filter_stats = {}
    
    # 1. 排除停牌（close=0 或 close为NaN）
    suspended_mask = (df['close'] == 0) | (df['close'].isna())
    filter_stats['排除停牌'] = int(suspended_mask.sum())
    df = df[~suspended_mask]
    
    # 2. 排除低成交（amount<min_amount）
    # amount单位是千元，所以100万=1000千元
    min_amount_thousand = min_amount / 1000
    low_amount_mask = (df['amount'] < min_amount_thousand) | (df['amount'].isna())
    filter_stats['排除低成交'] = int(low_amount_mask.sum())
    df = df[~low_amount_mask]
    
    # 3. 排除关键数据缺失
    key_fields = ['close', 'pct_chg', 'vol', 'amount']
    missing_mask = False
    for field in key_fields:
        if field in df.columns:
            missing_mask = missing_mask | df[field].isna()
    
    filter_stats['排除数据缺失'] = int(missing_mask.sum())
    df = df[~missing_mask]
    
    filtered_count = len(df)
    df.attrs['dynamic_filter_stats'] = filter_stats
    df.attrs['original_count'] = original_count
    
    return df, filter_stats


def get_trade_dates(start_date=None, end_date=None, n=5):
    """获取交易日历"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    df = df[df['is_open'] == 1]
    return df['cal_date'].tolist()


def get_latest_trade_date():
    """获取最近一个交易日"""
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
        df = pro.index_daily(ts_code='000001.SH', start_date=start_date, end_date=end_date)
        if len(df) > 0:
            return df['trade_date'].max()
    except:
        pass
    
    df = pro.trade_cal(exchange='SSE', 
                       start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
                       end_date=datetime.now().strftime('%Y%m%d'))
    df = df[df['is_open'] == 1]
    return df['cal_date'].iloc[-1]


def get_stock_basic(refresh=False):
    """获取股票基本信息（带缓存）"""
    cache_key = 'stock_basic_all'
    
    if not refresh:
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached
    
    df = pro.stock_basic(exchange='', list_status='L', 
                         fields='ts_code,symbol,name,area,industry,list_date,market,exchange')
    
    if df is not None:
        _cache.set(cache_key, df, ttl=86400)  # 缓存1天
    
    return df


def get_daily_price(ts_code, start_date=None, end_date=None):
    """获取个股日线数据"""
    if end_date is None:
        end_date = get_latest_trade_date()
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    
    # 转换日期格式 YYYY-MM-DD -> YYYYMMDD
    if start_date and '-' in str(start_date):
        start_date = start_date.replace('-', '')
    if end_date and '-' in str(end_date):
        end_date = end_date.replace('-', '')
    
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is not None and len(df) > 0 and 'trade_date' in df.columns:
        df = df.sort_values('trade_date')
    return df if df is not None else pd.DataFrame()


def get_recent_daily_data(n_days=60, batch_size=50):
    """获取最近n天的全部A股日线数据（分批获取）"""
    end_date = get_latest_trade_date()
    start_date = (datetime.now() - timedelta(days=n_days+20)).strftime('%Y%m%d')
    
    # 检查缓存
    cache_key = f'daily_all_{n_days}d'
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    
    # 获取所有股票
    stocks = get_stock_basic()
    
    all_data = []
    total = len(stocks)
    
    print(f"获取{total}只股票数据...")
    
    # 分批获取
    for i in range(0, len(stocks), batch_size):
        batch = stocks.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            try:
                df = get_daily_price(row['ts_code'], start_date, end_date)
                if len(df) > 0:
                    df['name'] = row['name']
                    df['industry'] = row['industry']
                    df['list_date'] = row['list_date']
                    all_data.append(df)
            except Exception as e:
                continue
        
        # 进度
        progress = min(i + batch_size, total) / total * 100
        print(f"进度: {progress:.1f}%", end='\r')
    
    print()
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        _cache.set(cache_key, result, ttl=3600)  # 缓存1小时
        return result
    
    return pd.DataFrame()


def get_financial_data(ts_code, refresh=False):
    """获取财务指标（带缓存）"""
    cache_key = f'financial_{ts_code}'
    
    if not refresh:
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached
    
    df = pro.fina_indicator(ts_code=ts_code, 
                           start_date=(datetime.now() - timedelta(days=180)).strftime('%Y%m%d'))
    
    if df is not None and len(df) > 0:
        _cache.set(cache_key, df, ttl=86400 * 7)  # 缓存7天
    
    return df


def get_alignment_factor(ts_code):
    """获取对齐因子"""
    df = pro.adj_factor(ts_code=ts_code, 
                        start_date='20200101', 
                        end_date=get_latest_trade_date())
    return df


def get_market_overview(refresh=False):
    """获取市场概览（带缓存优化）"""
    cache_key = f'market_overview_{get_latest_trade_date()}'
    
    if not refresh:
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached
    
    latest_date = get_latest_trade_date()
    print(f"获取日期: {latest_date}")
    
    # 获取当日数据，如果失败则获取前一天
    for _ in range(5):
        try:
            df = pro.daily_basic(
                trade_date=latest_date, 
                fields='ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pb,total_mv,circ_mv,ps'
            )
            if len(df) > 100:
                _cache.set(cache_key, df, ttl=3600 * 4)  # 缓存4小时
                return df
        except Exception as e:
            print(f"获取失败: {e}")
        
        # 尝试前一天
        dt = datetime.strptime(latest_date, '%Y%m%d') - timedelta(days=1)
        latest_date = dt.strftime('%Y%m%d')
        print(f"重试日期: {latest_date}")
    
    return pd.DataFrame()


def get_index_daily(ts_code='000300.SH', start_date=None, end_date=None):
    """获取指数日线"""
    if end_date is None:
        end_date = get_latest_trade_date()
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    
    df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return df


def get_index_weight(index_code='000300.SH', trade_date=None):
    """获取指数成分"""
    if trade_date is None:
        trade_date = get_latest_trade_date()
    
    df = pro.index_weight(index_code=index_code, trade_date=trade_date)
    return df


def get_concept_stock(concept):
    """获取概念股"""
    df = pro.concept_detail(concept)
    return df


def save_data(df, filename):
    """保存数据到文件"""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    path = os.path.join(config.DATA_DIR, f"{filename}.csv")
    df.to_csv(path, index=False)
    return path


def load_data(filename):
    """从文件加载数据"""
    path = os.path.join(config.DATA_DIR, f"{filename}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


def clear_cache():
    """清空所有缓存"""
    global _cache
    _cache.clear_all()
    print("缓存已清空")


def get_cache_stats():
    """获取缓存统计"""
    return _cache.get_stats()


def get_moneyflow_data(ts_code=None, trade_date=None, n_days=5):
    """获取资金流向数据（需要L2数据权限）
    返回：大单净流入、主力净流入占比等
    """
    if trade_date is None:
        trade_date = get_latest_trade_date()
    
    # 尝试使用moneyflow接口（需要权限）
    try:
        if ts_code:
            # 个股资金流向
            df = pro.moneyflow_hsgt(trade_date=trade_date)
            return df
    except:
        pass
    
    # 如果没有L2数据权限，返回模拟数据
    return pd.DataFrame()


def get_fund_flow_from_price(ts_code, n_days=10):
    """从价格数据推算资金流向（无权限时的备选方案）
    原理：价涨量增=资金流入，价跌量增=资金流出
    """
    df = get_daily_price(ts_code, 
                        start_date=(datetime.now() - timedelta(days=n_days+5)).strftime('%Y%m%d'),
                        end_date=get_latest_trade_date())
    
    if len(df) < 2:
        return {'big_order_net': 0, 'main_force_ratio': 0.5}
    
    df = df.sort_values('trade_date')
    
    # 计算涨跌幅
    df['pct_chg'] = df['close'].pct_change() * 100
    
    # 成交量变化
    df['vol_change'] = df['vol'].pct_change()
    
    # 资金流向估算
    # 涨且放量:资金流入，跌且放量:资金流出
    df['fund_flow'] = np.where(
        (df['pct_chg'] > 0) & (df['vol_change'] > 0), 1,
        np.where(
            (df['pct_chg'] < 0) & (df['vol_change'] > 0), -1,
            0
        )
    )
    
    # 最近5日的资金流向得分
    fund_score = df['fund_flow'].tail(n_days).sum()
    
    # 主力占比估算（使用大单比例）
    # 这里使用一个估算值，实际应该用moneyflow数据
    main_force_ratio = 0.5 + (fund_score / (n_days * 2)) * 0.3
    
    return {
        'big_order_net': fund_score,
        'main_force_ratio': max(0, min(1, main_force_ratio))
    }


def get_market_trend():
    """获取市场趋势（用于风控）
    返回：market_trend (up/down/neutral), index_change
    """
    try:
        # 获取上证指数最近20日数据
        df = get_index_daily('000001.SH', 
                            start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))
        
        if len(df) >= 20:
            df = df.sort_values('trade_date')
            # 20日均线
            ma20 = df['close'].tail(20).mean()
            # 当前价格
            current = df['close'].iloc[-1]
            # 近期涨跌幅
            change_5d = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100 if len(df) >= 6 else 0
            change_10d = (df['close'].iloc[-1] / df['close'].iloc[-11] - 1) * 100 if len(df) >= 11 else 0
            
            # 判断趋势
            if current > ma20 and change_5d > 0:
                trend = 'up'
            elif current < ma20 and change_5d < -1:
                trend = 'down'
            else:
                trend = 'neutral'
            
            return {
                'trend': trend,
                'index_change_5d': change_5d,
                'index_change_10d': change_10d,
                'index_price': current,
                'index_ma20': ma20
            }
    except Exception as e:
        print(f"获取市场趋势失败: {e}")
    
    return {'trend': 'neutral', 'index_change_5d': 0, 'index_change_10d': 0}


if __name__ == "__main__":
    # 测试
    print("获取交易日历...")
    dates = get_trade_dates()
    print(f"最近5个交易日: {dates[-5:]}")
    
    print("\n获取股票基本信息...")
    stocks = get_stock_basic()
    print(f"A股总数: {len(stocks)}")
    
    print("\n缓存统计:")
    print(get_cache_stats())


# ============ Production Mode 缓存和增量行情 ============
# 缓存设计说明:
# - 缓存目录: data/cache/realtime/
# - 文件命名: prices_YYYYMMDD.pkl
# - 失效时间: 4小时
# - 增量逻辑: 检查缓存是否命中，命中则使用，未命中则获取

import os
import pickle
import json
import time
from datetime import datetime, timedelta

CACHE_DIR = "data/cache"
REALTIME_DIR = f"{CACHE_DIR}/realtime"
CACHE_MAX_AGE_HOURS = 4  # 缓存失效时间


def _ensure_cache_dir():
    """确保缓存目录存在"""
    os.makedirs(REALTIME_DIR, exist_ok=True)


def get_cache_path(category, date_str=None):
    """获取缓存文件路径"""
    _ensure_cache_dir()
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    return f"{REALTIME_DIR}/{category}_{date_str}.pkl"


def is_cache_valid(cache_file, max_age_hours=4):
    """检查缓存是否有效"""
    if not os.path.exists(cache_file):
        return False
    mtime = os.path.getmtime(cache_file)
    age_hours = (datetime.now().timestamp() - mtime) / 3600
    return age_hours < max_age_hours


def get_cached_prices(codes=None, max_age_hours=CACHE_MAX_AGE_HOURS):
    """
    获取缓存的行情数据（Phase 1: 缓存优化）
    
    缓存设计:
    - 目录: data/cache/realtime/
    - 文件: prices_YYYYMMDD.pkl
    - 失效: 4小时
    
    逻辑:
    1. 检查缓存是否存在
    2. 检查是否在有效期内 (4小时)
    3. 命中则返回，失效则返回 None
    
    Args:
        codes: 股票代码列表，None 表示获取全部
        max_age_hours: 缓存有效时间
    
    Returns:
        DataFrame: 缓存的行情数据，None 表示未命中
    """
    cache_file = get_cache_path('prices')
    
    # 检查缓存是否有效
    if is_cache_valid(cache_file, max_age_hours):
        try:
            with open(cache_file, 'rb') as f:
                prices = pickle.load(f)
            print(f"使用缓存行情: {len(prices)} 条")
            if codes:
                prices = prices[prices['ts_code'].isin(codes)]
            return prices
        except:
            pass
    
    return None


def save_prices_cache(prices):
    """保存行情到缓存"""
    cache_file = get_cache_path('prices')
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(prices, f)
        print(f"已保存行情缓存: {len(prices)} 条")
    except Exception as e:
        print(f"保存缓存失败: {e}")


def get_realtime_prices(codes, batch_size=None, interval=None, max_workers=None):
    """
    获取实时行情（Phase 2: 并发优化）
    
    Args:
        codes: 股票代码列表
        batch_size: 每批数量（默认100）
        interval: 批次间隔秒（默认0.3）
        max_workers: 并发数（默认10）
    
    Returns:
        DataFrame: 行情数据
    """
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    # 使用默认配置或传入值
    if batch_size is None:
        batch_size = getattr(config, 'BATCH_SIZE', 100)
    if interval is None:
        interval = getattr(config, 'BATCH_INTERVAL', 0.3)
    if max_workers is None:
        max_workers = getattr(config, 'MAX_WORKERS', 10)
    
    print(f"[并发] batch={batch_size}, workers={max_workers}, interval={interval}s")
    
    results = []
    total = len(codes)
    failed = []  # 失败列表
    print(f"获取实时行情: {total} 只...")
    
    def get_one(code, retry_times=None):
        """获取单个股票行情（带容错）"""
        if retry_times is None:
            retry_times = getattr(config, 'RETRY_TIMES', 3)
        
        last_error = None
        for attempt in range(retry_times):
            try:
                df = pro.daily(ts_code=code, 
                             start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'),
                             end_date=datetime.now().strftime('%Y%m%d'))
                if len(df) > 0:
                    return df.iloc[-1]  # 取最新
            except Exception as e:
                last_error = str(e)
                # 限流退避
                if 'limit' in last_error.lower() or 'too many' in last_error.lower():
                    import time
                    time.sleep(getattr(config, 'RETRY_DELAY', 1) * (attempt + 1))
                continue
        
        # 所有重试都失败
        return None
    
    # 分批获取
    for i in range(0, total, batch_size):
        batch = codes[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(get_one, c): c for c in batch}
            for f in as_completed(futures):
                r = f.result()
                if r is not None:
                    results.append(r)
                elif r is None and code in failed:
                    pass  # 已记录
        
        # 进度
        done = min(i + batch_size, total)
        print(f"  进度: {done}/{total}")
        
        # 间隔避免限流
        if i + batch_size < total:
            time.sleep(interval)
    
    if results:
        df = pd.DataFrame(results)
        save_prices_cache(df)
        print(f"成功获取: {len(df)} 条, 失败: {len(failed)}")
        return df
    else:
        print(f"获取失败: 成功0, 失败{total}")
        return pd.DataFrame()


def get_prices_for_candidates(candidate_codes):
    """
    为候选股票池获取行情数据（Production Mode 专用）
    
    优先使用缓存，缓存失效时增量获取
    支持 Phase 4 增量更新机制
    """
    # 尝试从缓存获取
    cached = get_cached_prices(candidate_codes)
    
    if cached is not None and len(cached) > 0:
        # 筛选候选股票
        df = cached[cached['ts_code'].isin(candidate_codes)]
        if len(df) > len(candidate_codes) * 0.8:  # 缓存命中率 > 80%
            print(f"缓存命中: {len(df)}/{len(candidate_codes)}")
            return df
    
    # 缓存失效，尝试增量获取
    incremental_df = get_incremental_prices(candidate_codes)
    if incremental_df is not None and len(incremental_df) > 0:
        return incremental_df
    
    # 兜底：全量获取
    return get_realtime_prices(candidate_codes)


# ============ Phase 4: 增量更新机制 ============

PREHEAT_DIR = getattr(config, 'PREHEAT_CACHE_DIR', 'data/cache/preheat')

# 增量更新统计
_incremental_stats = {
    'cache_hit': 0,
    'incremental_update': 0,
    'full_fetch': 0
}


def get_incremental_stats():
    """获取增量更新统计"""
    return _incremental_stats.copy()


def reset_incremental_stats():
    """重置增量更新统计"""
    global _incremental_stats
    _incremental_stats = {'cache_hit': 0, 'incremental_update': 0, 'full_fetch': 0}


def load_preheat_cache(category):
    """加载预热缓存"""
    cache_file = os.path.join(PREHEAT_DIR, f"{category}.pkl")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except:
            pass
    return None


def get_preheat_prices(candidate_codes):
    """
    从预热缓存获取行情数据
    
    这是 Phase 4 的核心功能：production 主流程直接复用预热缓存
    """
    global _incremental_stats
    
    # 尝试加载预热缓存
    preheat_data = load_preheat_cache('daily_all')
    
    if preheat_data is not None and len(preheat_data) > 0:
        # 筛选候选股票
        df = preheat_data[preheat_data['ts_code'].isin(candidate_codes)]
        
        if len(df) > 0:
            # 获取最新交易日的数据
            latest_date = df['trade_date'].max()
            latest_data = df[df['trade_date'] == latest_date].copy()
            
            # 统计
            total_candidates = len(candidate_codes)
            hit_count = len(latest_data)
            hit_rate = hit_count / total_candidates if total_candidates > 0 else 0
            
            _incremental_stats['cache_hit'] = hit_count
            
            print(f"[Phase4] 预热缓存命中: {hit_count}/{total_candidates} ({hit_rate*100:.1f}%)")
            print(f"[Phase4] 交易日: {latest_date}, 数据量: {len(df)}")
            
            return latest_data
    
    return None


def get_incremental_prices(candidate_codes):
    """
    增量更新获取行情数据（Phase 4）
    
    逻辑：
    1. 先检查预热缓存（盘中也可复用）
    2. 再检查实时缓存
    3. 只获取缺失的股票或过期的数据
    4. 合并后返回
    
    Returns:
        DataFrame: 合并后的行情数据
    """
    global _incremental_stats
    
    print(f"\n[Phase4] 增量更新模式获取 {len(candidate_codes)} 只股票行情...")
    
    # 1. 尝试从预热缓存获取
    preheat_df = get_preheat_prices(candidate_codes)
    
    if preheat_df is not None and len(preheat_df) >= len(candidate_codes) * 0.9:
        # 预热缓存命中率足够高
        _incremental_stats['cache_hit'] = len(preheat_df)
        return preheat_df
    
    # 2. 预热缓存不够，获取缺失的股票
    if preheat_df is not None and len(preheat_df) > 0:
        cached_codes = set(preheat_df['ts_code'].unique())
        missing_codes = [c for c in candidate_codes if c not in cached_codes]
    else:
        missing_codes = list(candidate_codes)
    
    if missing_codes:
        print(f"[Phase4] 需要增量获取: {len(missing_codes)} 只")
        
        # 只获取最近5天的数据（增量）
        incremental_days = getattr(config, 'INCREMENTAL_DAYS', 5)
        start_date = (datetime.now() - timedelta(days=incremental_days+3)).strftime('%Y%m%d')
        end_date = get_latest_trade_date()
        
        incremental_data = []
        
        # 使用并发获取
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        batch_size = getattr(config, 'BATCH_SIZE', 100)
        interval = getattr(config, 'BATCH_INTERVAL', 0.3)
        
        for i in range(0, len(missing_codes), batch_size):
            batch = missing_codes[i:i+batch_size]
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(get_daily_price, c, start_date, end_date): c for c in batch}
                for f in as_completed(futures):
                    df = f.result()
                    if df is not None and len(df) > 0:
                        incremental_data.append(df)
            
            # 间隔防限流
            if i + batch_size < len(missing_codes):
                time.sleep(interval)
        
        if incremental_data:
            incremental_df = pd.concat(incremental_data, ignore_index=True)
            _incremental_stats['incremental_update'] = len(incremental_df)
            
            # 合并预热缓存和增量数据
            if preheat_df is not None and len(preheat_df) > 0:
                # 去重，优先用增量数据
                combined = pd.concat([preheat_df, incremental_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
            else:
                combined = incremental_df
            
            # 保存到实时缓存
            if len(combined) > 0:
                save_prices_cache(combined)
            
            return combined
    
    # 3. 没有缓存，返回空
    return pd.DataFrame()


def preheat_check():
    """
    检查预热缓存状态
    
    Returns:
        dict: 缓存状态信息
    """
    import os
    
    manifest_file = os.path.join(PREHEAT_DIR, '.manifest.json')
    manifest = {}
    if os.path.exists(manifest_file):
        try:
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
        except:
            pass
    
    # 检查缓存文件
    cache_status = {}
    for category in ['stock_basic', 'market_overview', 'daily_all']:
        cache_file = os.path.join(PREHEAT_DIR, f"{category}.pkl")
        if os.path.exists(cache_file):
            mtime = os.path.getmtime(cache_file)
            age_hours = (time.time() - mtime) / 3600
            cache_status[category] = {
                'exists': True,
                'age_hours': round(age_hours, 1),
                'valid': age_hours < getattr(config, 'PREHEAT_CACHE_TTL', 16)
            }
            if manifest.get(category):
                cache_status[category]['status'] = manifest[category].get('status', 'unknown')
        else:
            cache_status[category] = {'exists': False}
    
    return cache_status


# ============ 导出增量更新函数 ============

__all__ = [
    'get_cached_prices',
    'save_prices_cache',
    'get_realtime_prices',
    'get_prices_for_candidates',
    'get_incremental_prices',
    'get_preheat_prices',
    'get_incremental_stats',
    'reset_incremental_stats',
    'preheat_check',
]
