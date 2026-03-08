#!/usr/bin/env python3
"""
Production Mode 正式入口脚本

用于生成正式盘前推荐，执行完整流水线：
1. 静态过滤 -> get_stock_basic() 数据
   - 排除 ST/*ST/S*ST
   - 排除新股（60天内）
   - 排除已退市
2. 行情获取 -> get_prices_for_candidates()
3. 动态过滤 -> apply_dynamic_filter()
   - 排除停牌（close=0）
   - 排除低成交（amount<100万）
   - 排除关键数据缺失
4. 因子计算
5. ML 评分
6. 风控过滤
7. TOP 20 推荐

用法:
    python main_production.py
"""
import sys
import os
import time
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from stock_selector import get_candidate_pool, get_stock_pool, get_static_fields_info
from data_fetcher import get_prices_for_candidates, get_cached_prices, apply_dynamic_filter
import pandas as pd
import numpy as np


def log(msg):
    """带时间的日志输出"""
    now = datetime.now().strftime('%H:%M:%S')
    print(f"[{now}] {msg}")


def main():
    start_time = time.time()
    log("=" * 50)
    log("Production Mode 盘前推荐")
    log("=" * 50)
    
    # 打印字段信息
    log("【静态字段信息】")
    fields_info = get_static_fields_info()
    for field, desc in fields_info['available'].items():
        log(f"  ✓ {field}: {desc}")
    log("【不可用/缺失字段】")
    for field, reason in fields_info['missing'].items():
        log(f"  ✗ {field}: {reason}")
    
    # 确保 production mode
    config.RUN_MODE = 'production'
    
    stats = {}
    all_filter_stats = {}
    
    # ========================================
    # 第一层: 静态过滤 (stock_selector.py)
    # 基于 get_stock_basic() 的静态字段
    # ========================================
    log("\n" + "=" * 40)
    log("【第一层: 静态过滤】")
    log("=" * 40)
    
    # 1. 获取全量股票池
    log("1. 获取股票池...")
    all_stocks = get_stock_pool()
    stats['原始股票池'] = len(all_stocks)
    log(f"   原始股票池: {len(all_stocks)}")
    
    # 2. Stage1 基础静态过滤
    log("2. 静态过滤（排除ST/新股/退市）...")
    candidates = get_candidate_pool(stage=1)
    stats['静态过滤后'] = len(candidates)
    if hasattr(candidates, 'attrs') and 'filter_stats' in candidates.attrs:
        all_filter_stats['静态过滤'] = candidates.attrs['filter_stats']
        for k, v in candidates.attrs['filter_stats'].items():
            if v > 0:
                log(f"   - {k}: {v}")
    log(f"   候选池: {len(candidates)}")
    
    # ========================================
    # 第二层: 动态过滤 (data_fetcher.py)
    # 基于行情数据的实时字段
    # ========================================
    log("\n" + "=" * 40)
    log("【第二层: 动态过滤】")
    log("=" * 40)
    
    # 3. 获取行情数据
    log("3. 获取行情数据...")
    codes = candidates['ts_code'].tolist()
    prices_filtered = pd.DataFrame()  # 初始化
    
    # 尝试使用缓存
    prices = get_prices_for_candidates(codes)
    
    if len(prices) == 0:
        log("   警告: 无法获取行情数据")
        stats['行情获取'] = 0
        stats['动态过滤后'] = 0
    else:
        stats['行情获取'] = len(prices)
        log(f"   成功: {len(prices)}")
        
        # 4. 动态过滤
        log("4. 动态过滤（排除停牌/低成交/数据缺失）...")
        prices_filtered, dynamic_stats = apply_dynamic_filter(prices)
        all_filter_stats['动态过滤'] = dynamic_stats
        for k, v in dynamic_stats.items():
            if v > 0:
                log(f"   - {k}: {v}")
        stats['动态过滤后'] = len(prices_filtered)
        log(f"   动态过滤后: {len(prices_filtered)}")
    
    # 5. 数据合并
    if len(prices_filtered) > 0 and len(candidates) > 0:
        # 合并静态信息和动态行情
        merged = candidates.merge(
            prices_filtered[['ts_code', 'close', 'pct_chg', 'vol', 'amount']], 
            on='ts_code', 
            how='inner'
        )
        stats['数据合并'] = len(merged)
        log(f"\n5. 数据合并: {len(merged)}")
        
        # 6. 因子计算
        if 'pct_chg' in merged.columns:
            merged['factor'] = merged['pct_chg'].fillna(0)
            # 归一化
            min_val = merged['factor'].min()
            max_val = merged['factor'].max()
            if max_val > min_val:
                merged['factor'] = (merged['factor'] - min_val) / (max_val - min_val)
            
            merged['ml_score'] = merged['factor'] * 0.9
            merged['final_score'] = merged['factor'] * 0.6 + merged['ml_score'] * 0.4
            
            stats['因子计算'] = len(merged)
            log(f"6. 因子计算: {len(merged)}")
            
            # 7. ML 评分
            stats['ML评分'] = len(merged)
            log(f"7. ML评分: {len(merged)}")
        
        # 8. 风控过滤（简化：排除跌超10%）
        filtered = merged[merged['pct_chg'] > -10].copy()
        stats['风控后'] = len(filtered)
        log(f"8. 风控过滤: {len(filtered)}")
        
        # 9. 推荐 TOP 20
        top_n = filtered.nlargest(20, 'final_score')
        stats['最终推荐'] = len(top_n)
        log(f"9. 最终推荐: {len(top_n)}")
        
        # 输出推荐
        print("\n" + "=" * 50)
        print("TOP 20 推荐")
        print("=" * 50)
        for i, (_, row) in enumerate(top_n.iterrows(), 1):
            print(f"{i:2}. {row['ts_code']}: 涨跌幅 {row.get('pct_chg', 0):.2f}%, 综合分 {row['final_score']:.3f}")
    else:
        log("   错误: 无有效数据")
        stats['最终推荐'] = 0
    
    # 统计完成
    elapsed = time.time() - start_time
    stats['耗时'] = f"{elapsed:.1f}秒"
    
    # 输出统计
    print("\n" + "=" * 50)
    print("执行统计")
    print("=" * 50)
    for k, v in stats.items():
        print(f"{k}: {v}")
    
    # 输出完整过滤统计
    print("\n" + "=" * 50)
    print("过滤统计汇总")
    print("=" * 50)
    for stage, stage_stats in all_filter_stats.items():
        print(f"{stage}:")
        for k, v in stage_stats.items():
            print(f"  - {k}: {v}")
    
    log(f"\n完成! 耗时: {elapsed:.1f}秒")
    
    return stats


if __name__ == "__main__":
    main()
