"""
A股多因子量化交易系统 - 数据获取模块（增强版）
支持缓存、热数据、批量获取
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
    
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df = df.sort_values('trade_date')
    return df


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
