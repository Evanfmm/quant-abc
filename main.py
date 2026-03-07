#!/usr/bin/env python3
"""
A股多因子量化交易系统 - 主程序 v3
技术指标集成版 - 带具体买卖点位
"""
import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import data_fetcher
from factor_model_v3 import ShortTermFactorModelV3


def run_daily_report():
    """运行每日报告"""
    print("\n" + "="*70)
    print("A股量化交易系统 v3 - 具体买卖点位版")
    print("="*70)
    
    # 使用v3模型
    model = ShortTermFactorModelV3()
    recommendations = model.get_recommendations(config.MAX_POSITION)
    
    if not recommendations:
        print("\n⚠️ 未能获取到推荐股票")
        return
    
    # 获取市场情况
    print("\n【市场情况】")
    try:
        sh_index = data_fetcher.get_index_daily('000001.SH')
        if len(sh_index) > 0:
            sh_index = sh_index.sort_values('trade_date')
            latest = sh_index.iloc[-1]
            prev = sh_index.iloc[-2] if len(sh_index) > 1 else latest
            change = (latest['close'] - prev['close']) / prev['close'] * 100
            print(f"  上证指数: {latest['close']:.2f} ({change:+.2f}%)")
            print(f"  数据日期: {latest['trade_date']}")
    except Exception as e:
        print(f"  获取市场数据失败: {e}")
    
    # 打印推荐（带买卖点位）
    print("\n" + "="*70)
    print("【推荐股票 - 具体买卖点位】")
    print("="*70)
    
    for i, r in enumerate(recommendations, 1):
        try:
            current_price = float(r['现价'])
        except:
            current_price = 0
        
        # 计算止损止盈价
        stop_loss = current_price * (1 - config.STOP_LOSS_PCT)
        take_profit = current_price * (1 + config.TAKE_PROFIT_PCT)
        
        # 信号标记
        action = r.get('建议', 'N/A')
        action_mark = action
        if 'BUY' in str(action).upper():
            action_mark = "📈买入"
        elif 'SELL' in str(action).upper():
            action_mark = "📉卖出"
        elif 'WATCH' in str(action).upper():
            action_mark = "👀关注"
        
        print(f"\n{i}. {r['代码']} {r['名称']}")
        print(f"   ├─ 现价: {current_price:.2f}元")
        print(f"   ├─ 📈 建议买入价: {current_price:.2f}元 (今日收盘价)")
        print(f"   ├─ 🛡️ 止损价: {stop_loss:.2f}元 (下跌{int(config.STOP_LOSS_PCT*100)}%)")
        print(f"   ├─ 🎯 止盈价: {take_profit:.2f}元 (上涨{int(config.TAKE_PROFIT_PCT*100)}%)")
        print(f"   ├─ ⏰ 建议持有: 1-5个交易日")
        print(f"   ├─ 📊 技术信号: MACD={r.get('MACD','N/A')}, KDJ={r.get('KDJ','N/A')}, RSI={r.get('RSI','N/A')}")
        print(f"   └─ 💡 建议: {action_mark}")
    
    print("\n" + "="*70)
    
    # 买卖信号统计
    buy_count = sum(1 for r in recommendations if 'BUY' in str(r.get('建议', '')).upper())
    sell_count = sum(1 for r in recommendations if 'SELL' in str(r.get('建议', '')).upper())
    watch_count = sum(1 for r in recommendations if 'WATCH' in str(r.get('建议', '')).upper())
    
    print(f"\n【信号统计】")
    print(f"  📈 买入信号: {buy_count}/4")
    print(f"  📉 卖出信号: {sell_count}/4")
    print(f"  👀 关注信号: {watch_count}/4")
    
    # 风控说明
    print("\n【风控规则】")
    print(f"  🛡️ 止损: 买入后下跌{int(config.STOP_LOSS_PCT*100)}%止损")
    print(f"  🎯 止盈: 买入后上涨{int(config.TAKE_PROFIT_PCT*100)}%止盈")
    print(f"  ⏰ 持仓: 建议1-5个交易日")
    print(f"  📦 仓位: 单票不超30%，总持仓不超4只")
    
    # 因子配置
    print("\n【因子配置】")
    print("  资金流向: 30% | 动量因子: 30% | 情绪因子: 15%")
    print("  风险因子: 15% | 价值因子: 10%")
    print("  + 技术指标: MACD/KDJ/RSI")
    
    # 保存报告
    date_str = datetime.now().strftime('%Y-%m-%d')
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    
    # 添加买卖点位到推荐
    for r in recommendations:
        try:
            current_price = float(r['现价'])
            r['止损价'] = round(current_price * (1 - config.STOP_LOSS_PCT), 2)
            r['止盈价'] = round(current_price * (1 + config.TAKE_PROFIT_PCT), 2)
            r['建议买入价'] = current_price
        except:
            pass
    
    report_path = os.path.join(config.OUTPUT_DIR, f"report_v3_{date_str}.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump({
            'date': date_str,
            'model': 'v3买卖点位版',
            'recommendations': recommendations,
            'signals': {
                'buy': buy_count,
                'sell': sell_count,
                'watch': watch_count
            },
            'risk': {
                'stop_loss': config.STOP_LOSS_PCT,
                'take_profit': config.TAKE_PROFIT_PCT,
                'max_drawdown': config.MAX_DRAWDOWN
            }
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 报告已保存到: {report_path}")
    
    return recommendations


def main():
    run_daily_report()


if __name__ == "__main__":
    main()
