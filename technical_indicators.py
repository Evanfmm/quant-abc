"""
A股量化交易系统 - 技术指标计算模块
包含：MACD、KDJ、RSI 等常用技术指标及买卖信号
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import data_fetcher
import config


def calculate_ema(series, period):
    """计算指数移动平均线 (EMA)"""
    return series.ewm(span=period, adjust=False).mean()


def calculate_ma(series, period):
    """计算简单移动平均线 (MA)"""
    return series.rolling(window=period).mean()


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标
    返回: DIF, DEA, MACD
    
    DIF = EMA(12) - EMA(26)
    DEA = EMA(DIF, 9)
    MACD = (DIF - DEA) * 2
    """
    if df is None or len(df) < slow:
        return None, None, None
    
    # 确保按日期排序
    df = df.sort_values('trade_date')
    
    # 计算快速和慢速EMA
    close = df['close']
    ema_fast = calculate_ema(close, fast)
    ema_slow = calculate_ema(close, slow)
    
    # DIF线
    dif = ema_fast - ema_slow
    
    # DEA线
    dea = calculate_ema(dif, signal)
    
    # MACD柱
    macd = (dif - dea) * 2
    
    return dif, dea, macd


def detect_macd_signal(df):
    """
    检测MACD买卖信号
    返回: signal (golden='金叉', dead='死叉', none='无信号')
    """
    if df is None or len(df) < 10:
        return 'none', '数据不足'
    
    dif, dea, macd = calculate_macd(df)
    
    if dif is None or dea is None:
        return 'none', '计算失败'
    
    # 获取最近两天的DIF和DEA
    dif_now = dif.iloc[-1]
    dif_prev = dif.iloc[-2]
    dea_now = dea.iloc[-1]
    dea_prev = dea.iloc[-2]
    
    # 金叉：DIF从下往上穿过DEA
    if dif_prev <= dea_prev and dif_now > dea_now:
        return 'golden', f'金叉(DIF:{dif_now:.3f}, DEA:{dea_now:.3f})'
    
    # 死叉：DIF从上往下穿过DEA
    elif dif_prev >= dea_prev and dif_now < dea_now:
        return 'dead', f'死叉(DIF:{dif_now:.3f}, DEA:{dea_now:.3f})'
    
    # 持续多头：DIF和DEA都在0轴上方且向上
    elif dif_now > 0 and dea_now > 0 and dif_now > dif_prev and dea_now > dea_prev:
        return 'bullish', f'多头(DIF:{dif_now:.3f}, DEA:{dea_now:.3f})'
    
    # 持续空头：DIF和DEA都在0轴下方且向下
    elif dif_now < 0 and dea_now < 0 and dif_now < dif_prev and dea_now < dea_prev:
        return 'bearish', f'空头(DIF:{dif_now:.3f}, DEA:{dea_now:.3f})'
    
    return 'none', f'等待(DIF:{dif_now:.3f}, DEA:{dea_now:.3f})'


def calculate_kdj(df, n=9, m1=3, m2=3):
    """
    计算KDJ指标
    参数:
        n: RSV计算周期 (默认9日)
        m1: K因子 (默认3)
        m2: D因子 (默认3)
    
    返回: K, D, J
    """
    if df is None or len(df) < n:
        return None, None, None
    
    # 确保按日期排序
    df = df.sort_values('trade_date')
    
    # 计算N日内最高价和最低价
    high_n = df['high'].rolling(window=n).max()
    low_n = df['low'].rolling(window=n).min()
    
    # 计算RSV
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    rsv = rsv.fillna(50)  # 缺失值用50填充
    
    # 计算K、D、J
    k = pd.Series(index=df.index, dtype=float)
    d = pd.Series(index=df.index, dtype=float)
    
    # 初始化
    k.iloc[0] = 50
    d.iloc[0] = 50
    
    # 递归计算
    for i in range(1, len(df)):
        k.iloc[i] = (2/3) * k.iloc[i-1] + (1/3) * rsv.iloc[i]
        d.iloc[i] = (2/3) * d.iloc[i-1] + (1/3) * k.iloc[i]
    
    # J = 3K - 2D
    j = 3 * k - 2 * d
    
    return k, d, j


def detect_kdj_signal(df, oversold=20, overbought=80):
    """
    检测KDJ买卖信号
    返回: signal, description
    """
    if df is None or len(df) < 20:
        return 'none', '数据不足'
    
    k, d, j = calculate_kdj(df)
    
    if k is None or d is None or j is None:
        return 'none', '计算失败'
    
    # 获取最近几天的值
    k_now, k_prev = k.iloc[-1], k.iloc[-2]
    d_now, d_prev = d.iloc[-1], d.iloc[-2]
    j_now = j.iloc[-1]
    
    # 金叉：K从下往上穿过D，且在超卖区（J<20）
    if k_prev <= d_prev and k_now > d_now and j_now < oversold:
        return 'golden', f'金叉(K:{k_now:.1f}, D:{d_now:.1f}, J:{j_now:.1f}超卖)'
    
    # 死叉：K从上往下穿过D，且在超买区（J>80）
    if k_prev >= d_prev and k_now < d_now and j_now > overbought:
        return 'dead', f'死叉(K:{k_now:.1f}, D:{d_now:.1f}, J:{j_now:.1f}超买)'
    
    # 超卖反弹：J<20
    if j_now < oversold:
        return 'oversold', f'超卖(J:{j_now:.1f})'
    
    # 超买回调：J>80
    if j_now > overbought:
        return 'overbought', f'超买(J:{j_now:.1f})'
    
    # 多头排列：K>D>J且都在50以上
    if k_now > d_now > 50 and j_now > k_now:
        return 'bullish', f'多头(K:{k_now:.1f}, D:{d_now:.1f}, J:{j_now:.1f})'
    
    # 空头排列：K<D<J且都在50以下
    if k_now < d_now < 50 and j_now < k_now:
        return 'bearish', f'空头(K:{k_now:.1f}, D:{d_now:.1f}, J:{j_now:.1f})'
    
    return 'none', f'等待(K:{k_now:.1f}, D:{d_now:.1f}, J:{j_now:.1f})'


def calculate_rsi(df, periods=[6, 12]):
    """
    计算RSI相对强弱指标
    RSI = N日内上涨幅度 / (N日内上涨幅度 + N日内下跌幅度) * 100
    """
    if df is None or len(df) < max(periods) + 1:
        return {p: None for p in periods}
    
    df = df.sort_values('trade_date')
    
    close = df['close']
    
    # 计算价格变化
    delta = close.diff()
    
    result = {}
    
    for period in periods:
        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)
        
        # 计算平均涨跌幅（使用EMA）
        avg_gain = gain.ewm(span=period, adjust=False).mean()
        avg_loss = loss.ewm(span=period, adjust=False).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        result[period] = rsi
    
    return result


def calculate_atr(df, period=14):
    """
    计算ATR (Average True Range) 平均真实波幅
    用于动态止损
    
    True Range = max(H-L, |H-PC|, |L-PC|)
    ATR = EMA(TR, period)
    
    返回: ATR值
    """
    if df is None or len(df) < period + 1:
        return None
    
    df = df.sort_values('trade_date')
    
    # 计算True Range
    high = df['high']
    low = df['low']
    close = df['close']
    
    # 前一天的收盘价
    prev_close = close.shift(1)
    
    # 三种情况取最大
    tr1 = high - low  # 当日最高-最低
    tr2 = (high - prev_close).abs()  # 当日最高-昨日收盘
    tr3 = (low - prev_close).abs()   # 当日最低-昨日收盘
    
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # 计算ATR (使用EMA)
    atr = true_range.ewm(span=period, adjust=False).mean()
    
    return atr


def calculate_dynamic_stop_loss(entry_price, atr, atr_multiplier=2.0):
    """
    计算动态止损价
    基于ATR计算，止损价 = 入场价 - ATR * 倍数
    
    参数:
        entry_price: 入场价格
        atr: ATR值
        atr_multiplier: ATR倍数 (默认2倍)
    
    返回: 止损价
    """
    if entry_price is None or atr is None:
        return None
    
    stop_loss_price = entry_price - (atr * atr_multiplier)
    return stop_loss_price


def detect_rsi_signal(df, oversold=30, overbought=70):
    """
    检测RSI买卖信号
    返回: signal, description
    """
    if df is None or len(df) < 20:
        return 'none', '数据不足'
    
    rsi_values = calculate_rsi(df)
    
    rsi_6 = rsi_values.get(6)
    rsi_12 = rsi_values.get(12)
    
    if rsi_6 is None or rsi_12 is None:
        return 'none', '计算失败'
    
    rsi6_now = rsi_6.iloc[-1]
    rsi12_now = rsi_12.iloc[-1]
    rsi6_prev = rsi_6.iloc[-2]
    rsi12_prev = rsi_12.iloc[-2]
    
    signals = []
    
    # RSI(6) 超卖
    if rsi6_now < oversold:
        signals.append(f'RSI(6)超卖:{rsi6_now:.1f}')
    
    # RSI(6) 超买
    if rsi6_now > overbought:
        signals.append(f'RSI(6)超买:{rsi6_now:.1f}')
    
    # RSI金叉：短期RSI从下往上穿过长期RSI
    if rsi6_prev <= rsi12_prev and rsi6_now > rsi12_now:
        return 'golden', f'RSI金叉(6:{rsi6_now:.1f}, 12:{rsi12_now:.1f})'
    
    # RSI死叉：短期RSI从上往下穿过长期RSI
    if rsi6_prev >= rsi12_prev and rsi6_now < rsi12_now:
        return 'dead', f'RSI死叉(6:{rsi6_now:.1f}, 12:{rsi12_now:.1f})'
    
    if signals:
        return signals[0], ','.join(signals)
    
    # 多头：RSI在50以上且上升趋势
    if rsi6_now > 50 and rsi12_now > 50 and rsi6_now > rsi6_prev:
        return 'bullish', f'RSI多头(6:{rsi6_now:.1f}, 12:{rsi12_now:.1f})'
    
    # 空头：RSI在50以下且下降趋势
    if rsi6_now < 50 and rsi12_now < 50 and rsi6_now < rsi6_prev:
        return 'bearish', f'RSI空头(6:{rsi6_now:.1f}, 12:{rsi12_now:.1f})'
    
    return 'none', f'RSI(6:{rsi6_now:.1f}, 12:{rsi12_now:.1f})'


def get_stock_technical_indicators(ts_code, n_days=60):
    """
    获取单只股票的所有技术指标
    """
    # 获取日线数据
    df = data_fetcher.get_daily_price(ts_code, 
                                      start_date=(datetime.now() - timedelta(days=n_days+20)).strftime('%Y%m%d'),
                                      end_date=data_fetcher.get_latest_trade_date())
    
    if df is None or len(df) < 30:
        return None
    
    df = df.sort_values('trade_date')
    
    # 计算MACD
    dif, dea, macd = calculate_macd(df)
    macd_signal, macd_desc = detect_macd_signal(df)
    
    # 计算KDJ
    k, d, j = calculate_kdj(df)
    kdj_signal, kdj_desc = detect_kdj_signal(df)
    
    # 计算RSI
    rsi_signal, rsi_desc = detect_rsi_signal(df)
    rsi_values = calculate_rsi(df)
    
    # 计算ATR (用于动态止损)
    atr = calculate_atr(df)
    
    # 获取最新值
    result = {
        'ts_code': ts_code,
        # MACD
        'macd_dif': round(dif.iloc[-1], 4) if dif is not None else None,
        'macd_dea': round(dea.iloc[-1], 4) if dea is not None else None,
        'macd_hist': round(macd.iloc[-1], 4) if macd is not None else None,
        'macd_signal': macd_signal,
        'macd_desc': macd_desc,
        # KDJ
        'kdj_k': round(k.iloc[-1], 2) if k is not None else None,
        'kdj_d': round(d.iloc[-1], 2) if d is not None else None,
        'kdj_j': round(j.iloc[-1], 2) if j is not None else None,
        'kdj_signal': kdj_signal,
        'kdj_desc': kdj_desc,
        # RSI
        'rsi_6': round(rsi_values[6].iloc[-1], 2) if rsi_values.get(6) is not None else None,
        'rsi_12': round(rsi_values[12].iloc[-1], 2) if rsi_values.get(12) is not None else None,
        'rsi_signal': rsi_signal,
        'rsi_desc': rsi_desc,
        # ATR (新增，用于动态止损)
        'atr': round(atr.iloc[-1], 4) if atr is not None else None,
        'atr_14': round(atr.iloc[-1], 4) if atr is not None else None,
    }
    
    return result


def generate_trading_signal(indicators):
    """
    根据技术指标生成综合买卖信号
    返回: action (buy/sell/hold), reason
    """
    if indicators is None:
        return 'hold', '数据不足'
    
    buy_signals = []
    sell_signals = []
    hold_reasons = []
    
    # MACD信号
    macd = indicators.get('macd_signal', 'none')
    if macd == 'golden':
        buy_signals.append('MACD金叉')
    elif macd == 'dead':
        sell_signals.append('MACD死叉')
    elif macd == 'bearish':
        sell_signals.append('MACD空头')
    
    # KDJ信号
    kdj = indicators.get('kdj_signal', 'none')
    if kdj == 'golden':
        buy_signals.append('KDJ金叉超卖')
    elif kdj == 'dead':
        sell_signals.append('KDJ死叉超买')
    elif kdj == 'oversold':
        buy_signals.append('KDJ超卖')
    elif kdj == 'overbought':
        sell_signals.append('KDJ超买')
    
    # RSI信号
    rsi = indicators.get('rsi_signal', 'none')
    if rsi == 'golden':
        buy_signals.append('RSI金叉')
    elif rsi == 'dead':
        sell_signals.append('RSI死叉')
    elif '超卖' in str(rsi):
        buy_signals.append('RSI超卖')
    elif '超买' in str(rsi):
        sell_signals.append('RSI超买')
    
    # 综合判断
    buy_count = len(buy_signals)
    sell_count = len(sell_signals)
    
    if buy_count >= 2:
        return 'buy', f"买入信号({', '.join(buy_signals)})"
    elif sell_count >= 2:
        return 'sell', f"卖出信号({', '.join(sell_signals)})"
    elif buy_count == 1 and sell_count == 0:
        return 'watch', f"关注({buy_signals[0]})"
    elif sell_count == 1 and buy_count == 0:
        return 'caution', f"谨慎({sell_signals[0]})"
    else:
        return 'hold', '技术面中性'


def get_batch_technical_indicators(ts_codes, n_days=60):
    """
    批量获取多只股票的技术指标
    """
    results = []
    for code in ts_codes:
        try:
            indicators = get_stock_technical_indicators(code, n_days)
            if indicators:
                action, reason = generate_trading_signal(indicators)
                indicators['action'] = action
                indicators['reason'] = reason
                results.append(indicators)
        except Exception as e:
            print(f"计算{code}技术指标失败: {e}")
            continue
    
    return results


# 测试
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    print("="*60)
    print("测试技术指标计算模块")
    print("="*60)
    
    # 测试获取一只股票的技术指标
    test_code = '000001.SZ'  # 平安银行
    
    print(f"\n获取 {test_code} 技术指标...")
    indicators = get_stock_technical_indicators(test_code)
    
    if indicators:
        print("\n【MACD指标】")
        print(f"  DIF: {indicators.get('macd_dif')}")
        print(f"  DEA: {indicators.get('macd_dea')}")
        print(f"  MACD柱: {indicators.get('macd_hist')}")
        print(f"  信号: {indicators.get('macd_signal')} - {indicators.get('macd_desc')}")
        
        print("\n【KDJ指标】")
        print(f"  K: {indicators.get('kdj_k')}")
        print(f"  D: {indicators.get('kdj_d')}")
        print(f"  J: {indicators.get('kdj_j')}")
        print(f"  信号: {indicators.get('kdj_signal')} - {indicators.get('kdj_desc')}")
        
        print("\n【RSI指标】")
        print(f"  RSI(6): {indicators.get('rsi_6')}")
        print(f"  RSI(12): {indicators.get('rsi_12')}")
        print(f"  信号: {indicators.get('rsi_signal')} - {indicators.get('rsi_desc')}")
        
        print("\n【综合买卖建议】")
        action, reason = generate_trading_signal(indicators)
        print(f"  操作: {action}")
        print(f"  理由: {reason}")
    else:
        print("❌ 获取数据失败")
