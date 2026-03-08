#!/usr/bin/env python3
"""
A股多因子量化交易系统 - 增强版主程序
使用方法:
    python main_enhanced.py              # 生成每日报告
    python main_enhanced.py --backtest  # 运行回测
    python main_enhanced.py --enhanced  # 使用增强因子模型
    python main_enhanced.py --broker    # 模拟真实券商交易
"""
import sys
import argparse
import daily_report
import trading_signal
from factor_model_enhanced import EnhancedMultiFactorModel
from broker_api import create_broker
from risk_manager import RiskManager, PositionSizer
import config


def main():
    parser = argparse.ArgumentParser(description='A股多因子量化交易系统-增强版')
    parser.add_argument('--report', action='store_true', help='生成每日报告')
    parser.add_argument('--backtest', action='store_true', help='运行回测')
    parser.add_argument('--enhanced', action='store_true', help='使用增强因子模型')
    parser.add_argument('--broker', action='store_true', help='使用模拟券商')
    parser.add_argument('--clear-cache', action='store_true', help='清空缓存')
    args = parser.parse_args()
    
    # 清空缓存
    if args.clear_cache:
        import data_fetcher
        data_fetcher.clear_cache()
        print("缓存已清空")
        return
    
    # 回测模式
    if args.backtest:
        run_backtest()
        return
    
    # 券商模拟模式
    if args.broker:
        run_with_broker()
        return
    
    # 每日报告模式
    if args.enhanced:
        run_enhanced_report()
    else:
        # 默认使用原有模式
        daily_report.run_daily_report()


def run_enhanced_report():
    """运行增强版报告"""
    print("\n" + "="*60)
    print("A股量化交易系统 - 增强因子版")
    print("="*60)
    
    # 使用增强因子模型
    model = EnhancedMultiFactorModel()
    recommendations = model.get_recommendations(config.MAX_POSITION)
    
    if not recommendations:
        print("\n⚠️ 未能获取到推荐股票")
        return
    
    # 生成报告
    print("\n【推荐股票】")
    print("-"*70)
    print(f"{'代码':<10}{'名称':<10}{'行业':<10}{'现价':<8}{'PE':<8}{'ROE':<8}{'换手率':<8}{'得分':<8}")
    print("-"*70)
    for r in recommendations:
        print(f"{r['代码']:<10}{r['名称']:<10}{r['行业']:<10}{r['现价']:<8}"
              f"{r['PE']:<8}{r.get('ROE', 'N/A'):<8}{r.get('换手率', 'N/A'):<8}{r['综合得分']:<8}")
    print("-"*70)
    
    # 生成交易信号（含风控）
    signal_gen = trading_signal.TradingSignal()
    import pandas as pd
    signals = signal_gen.generate_signal(pd.DataFrame(recommendations))
    
    print("\n【交易信号】")
    if signals:
        for sig in signals:
            if sig['action'] == 'BUY':
                print(f"  ✅ 买入 {sig['ts_code']} {sig['name']}")
                print(f"     数量:{sig['shares']}股 金额:{sig['amount']:.0f}元 行业:{sig.get('industry', 'N/A')}")
            else:
                print(f"  ❌ 卖出 {sig['ts_code']} 原因: {sig['reason']}")
    else:
        print("  ⏸️ 无交易信号")
    
    # 风控报告
    print("\n【风险提示】")
    print(f"  最大回撤限制: {config.MAX_DRAWDOWN*100:.0f}%")
    print(f"  止损线: {config.STOP_LOSS_PCT*100:.0f}%")
    print(f"  止盈线: {config.TAKE_PROFIT_PCT*100:.0f}%")
    print(f"  单行业最大权重: {config.MAX_INDUSTRY_WEIGHT*100:.0f}%")
    print("\n" + "="*60)


def run_backtest():
    """运行回测"""
    print("\n" + "="*60)
    print("开始回测...")
    print("="*60)
    
    bt = trading_signal.Backtest(config.INITIAL_CAPITAL)
    
    # 简化回测
    perf = bt.get_performance()
    
    print(f"\n【回测结果】")
    print(f"  初始资金: ¥{perf['initial_capital']:,.2f}")
    print(f"  最终资金: ¥{perf['final_capital']:,.2f}")
    print(f"  总收益: {perf['total_return_pct']:+.2f}%")
    print(f"  最大回撤: {perf['max_drawdown_pct']:.2f}%")
    print(f"  交易次数: {perf['num_trades']}")
    if 'sharpe_ratio' in perf:
        print(f"  夏普比率: {perf['sharpe_ratio']:.2f}")
    print("="*60)


def run_with_broker():
    """使用模拟券商运行"""
    print("\n" + "="*60)
    print("券商模拟交易模式")
    print("="*60)
    
    # 创建Broker
    broker = create_broker('tushare')
    broker.connect()
    
    # 获取账户信息
    account = broker.get_account_info()
    print(f"\n账户资金: ¥{account['cash']:,.2f}")
    
    # 获取推荐
    model = EnhancedMultiFactorModel()
    recommendations = model.get_recommendations(config.MAX_POSITION)
    
    if recommendations:
        print(f"\n推荐股票: {[r['代码'] for r in recommendations]}")
        
        # 生成信号
        signal_gen = trading_signal.TradingSignal(broker)
        import pandas as pd
        signals = signal_gen.generate_signal(pd.DataFrame(recommendations))
        
        # 执行信号
        print("\n执行交易:")
        for sig in signals:
            order = broker.place_order(
                sig['ts_code'],
                sig['action'],
                sig.get('shares', 0),
                sig.get('price', 0)
            )
            print(f"  {sig['action']} {sig['ts_code']}: {order}")
    
    # 最终账户状态
    account = broker.get_account_info()
    print(f"\n【最终账户状态】")
    print(f"  可用资金: ¥{account['available_cash']:,.2f}")
    print(f"  持仓市值: ¥{account.get('position_value', 0):,.2f}")
    print(f"  总资产: ¥{account['total_assets']:,.2f}")
    print(f"  持仓: {broker.get_positions()}")
    print("="*60)


if __name__ == "__main__":
    main()
