"""
A股多因子量化交易系统 - 每日报告生成
"""
import pandas as pd
from datetime import datetime
import json
import os
import config
import data_fetcher
from factor_model import MultiFactorModel
from trading_signal import TradingSignal

class DailyReport:
    """每日报告生成器"""
    
    def __init__(self):
        self.model = MultiFactorModel()
        self.signal_gen = TradingSignal()
        self.date = datetime.now().strftime('%Y-%m-%d')
        
    def generate(self, positions=None):
        """生成每日报告"""
        print(f"\n{'='*60}")
        print(f"A股量化交易系统 - 每日报告")
        print(f"日期: {self.date}")
        print(f"{'='*60}\n")
        
        # 1. 获取推荐股票
        print("正在分析市场...")
        recommendations = self.model.get_recommendations(config.MAX_POSITION)
        
        if not recommendations:
            print("\n⚠️ 未能获取到推荐股票，请检查数据源或稍后重试")
            return None
        
        # 2. 生成交易信号
        signals = self.signal_gen.generate_signal(
            pd.DataFrame(recommendations), 
            positions or {}
        )
        
        # 3. 生成报告
        report = {
            'date': self.date,
            'recommendations': recommendations,
            'signals': signals,
            'market_overview': self.get_market_brief()
        }
        
        # 4. 输出报告
        self.print_report(report)
        
        # 5. 保存报告
        self.save_report(report)
        
        return report
    
    def get_market_brief(self):
        """获取市场简况"""
        try:
            # 获取上证指数最近10天数据
            sh_index = data_fetcher.get_index_daily('000001.SH')
            if len(sh_index) > 0:
                # 按日期排序确保最新在最后
                sh_index = sh_index.sort_values('trade_date')
                latest = sh_index.iloc[-1]
                prev = sh_index.iloc[-2] if len(sh_index) > 1 else latest
                
                change = (latest['close'] - prev['close']) / prev['close'] * 100
                
                return {
                    'sh_index': round(latest['close'], 2),
                    'sh_change': round(change, 2),
                    'sh_volume': latest.get('vol', 0),
                    'trade_date': latest['trade_date']
                }
        except Exception as e:
            print(f"获取市场概况出错: {e}")
            pass
        return {}
    
    def print_report(self, report):
        """打印报告"""
        print("【市场情况】")
        if report['market_overview']:
            m = report['market_overview']
            print(f"  上证指数: {m.get('sh_index', 'N/A')} ({m.get('sh_change', 'N/A'):+.2f}%)")
        print()
        
        print("【推荐股票】")
        print("-" * 70)
        print(f"{'代码':<10}{'名称':<10}{'行业':<12}{'现价':<8}{'PE':<8}{'得分':<8}")
        print("-" * 70)
        for r in report['recommendations']:
            print(f"{r['代码']:<10}{r['名称']:<10}{r['行业']:<12}"
                  f"{r['现价']:<8}{str(r['PE']):<8}{r['综合得分']:<8}")
        print("-" * 70)
        print()
        
        print("【交易信号】")
        if report['signals']:
            for sig in report['signals']:
                if sig['action'] == 'BUY':
                    print(f"  买入 {sig['ts_code']} {sig['name']} "
                          f"数量:{sig['shares']}股 金额:{sig['amount']:.2f}元")
                else:
                    print(f"  卖出 {sig['ts_code']} 原因: {sig['reason']}")
        else:
            print("  无交易信号")
        print()
        
        print("【操作建议】")
        self.print_advice(report)
    
    def print_advice(self, report):
        """打印操作建议"""
        signals = report['signals']
        buy_signals = [s for s in signals if s['action'] == 'BUY']
        sell_signals = [s for s in signals if s['action'] == 'SELL']
        
        if buy_signals:
            print("  📈 建议买入:")
            for sig in buy_signals:
                print(f"     - {sig['ts_code']} {sig['name']}")
                print(f"       买入{sg['shares']}股，约{sig['amount']:.0f}元")
        
        if sell_signals:
            print("  📉 建议卖出:")
            for sig in sell_signals:
                print(f"     - {sig['ts_code']}")
        
        if not signals:
            print("  ⏸️ 继续持有，等待信号")
        
        print()
        print(f"⚠️ 风险提示: 系统最大回撤{config.MAX_DRAWDOWN*100}%，请谨慎操作")
    
    def save_report(self, report):
        """保存报告"""
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        
        # JSON格式
        json_path = os.path.join(config.OUTPUT_DIR, f"report_{self.date}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # Markdown格式
        md_path = os.path.join(config.OUTPUT_DIR, f"report_{self.date}.md")
        self.save_markdown(md_path, report)
        
        print(f"报告已保存到: {json_path}")
    
    def save_markdown(self, path, report):
        """保存markdown格式报告"""
        md = f"""# A股量化交易系统 - 每日报告

**日期**: {report['date']}

## 市场情况

| 指标 | 数值 |
|------|------|
| 上证指数 | {report['market_overview'].get('sh_index', 'N/A')} |
| 涨跌幅 | {report['market_overview'].get('sh_change', 'N/A'):+.2f}% |

## 推荐股票

| 代码 | 名称 | 行业 | 现价 | PE | 综合得分 |
|------|------|------|------|-----|----------|
"""
        for r in report['recommendations']:
            md += f"| {r['代码']} | {r['名称']} | {r['行业']} | {r['现价']} | {r['PE']} | {r['综合得分']} |\n"
        
        md += """
## 交易信号

"""
        for sig in report['signals']:
            if sig['action'] == 'BUY':
                md += f"- **买入** {sig['ts_code']} {sig['name']} - {sig['shares']}股\n"
            else:
                md += f"- **卖出** {sig['ts_code']} - {sig['reason']}\n"
        
        if not report['signals']:
            md += "- 无交易信号\n"
        
        md += f"""
## 操作建议

{"📈 建议买入以上推荐股票" if report['signals'] else "⏸️ 继续持有"}

⚠️ **风险提示**: 系统最大回撤{config.MAX_DRAWDOWN*100}%，本报告仅供参考，不构成投资建议

---
*由AI量化交易系统自动生成*
"""
        
        with open(path, 'w', encoding='utf-8') as f:
            f.write(md)


def run_daily_report(positions=None):
    """运行每日报告"""
    report_gen = DailyReport()
    return report_gen.generate(positions)


if __name__ == "__main__":
    run_daily_report()
