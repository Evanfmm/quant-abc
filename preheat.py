#!/usr/bin/env python3
"""
Phase 4: 盘前预热脚本

功能：
1. 盘前预热缓存 - 在开盘前获取并缓存全量数据
2. 增量更新检查 - 只更新缺失或过期的数据
3. 缓存状态管理 - 记录缓存更新时间、状态

用法：
    python preheat.py              # 完整预热
    python preheat.py --check     # 只检查缓存状态
    python preheat.py --force     # 强制重新预热
    python preheat.py --stats     # 显示缓存统计
"""
import sys
import os
import time
import json
import pickle
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import tushare as ts
from data_fetcher import (
    get_stock_basic, get_market_overview, get_latest_trade_date,
    get_daily_price, get_prices_for_candidates, apply_dynamic_filter
)
from stock_selector import get_candidate_pool
import pandas as pd

# 设置tushare
ts.set_token(config.TUSHARE_TOKEN)
pro = ts.pro_api()

# 预热缓存目录
PREHEAT_DIR = config.PREHEAT_CACHE_DIR
os.makedirs(PREHEAT_DIR, exist_ok=True)

# 缓存清单文件
MANIFEST_FILE = os.path.join(PREHEAT_DIR, '.manifest.json')


def log(msg, level='INFO'):
    """带时间的日志"""
    now = datetime.now().strftime('%H:%M:%S')
    print(f"[{now}] [{level}] {msg}")


# ============ 缓存清单管理 ============

def load_manifest():
    """加载缓存清单"""
    if os.path.exists(MANIFEST_FILE):
        try:
            with open(MANIFEST_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}


def save_manifest(manifest):
    """保存缓存清单"""
    try:
        with open(MANIFEST_FILE, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
    except Exception as e:
        log(f"保存清单失败: {e}", 'WARN')


def update_manifest(category, status, details=None):
    """更新缓存清单"""
    manifest = load_manifest()
    manifest[category] = {
        'status': status,
        'updated': datetime.now().isoformat(),
        'details': details or {}
    }
    save_manifest(manifest)


# ============ 预热数据获取 ============

def preheat_stock_basic():
    """预热股票基本信息"""
    log("预热股票基本信息...")
    start = time.time()
    
    cache_file = os.path.join(PREHEAT_DIR, f"stock_basic.pkl")
    
    # 检查是否需要更新
    manifest = load_manifest()
    need_update = True
    
    if os.path.exists(cache_file) and 'stock_basic' in manifest:
        mtime = os.path.getmtime(cache_file)
        age_hours = (time.time() - mtime) / 3600
        # 如果缓存不足24小时，且是同一天更新的，则跳过
        if age_hours < 24:
            last_date = manifest['stock_basic'].get('updated', '')[:10]
            today = datetime.now().strftime('%Y-%m-%d')
            if last_date == today:
                log(f"  股票基本信息缓存有效，跳过 ({age_hours:.1f}h)")
                need_update = False
    
    if need_update:
        df = get_stock_basic(refresh=True)
        if df is not None and len(df) > 0:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
            update_manifest('stock_basic', 'ok', {'count': len(df), 'cache_file': cache_file})
            log(f"  完成: {len(df)} 只股票，耗时 {time.time()-start:.1f}s")
            return True
        else:
            update_manifest('stock_basic', 'failed', {'error': '无数据'})
            log("  失败: 无法获取数据", 'ERROR')
            return False
    
    return True


def preheat_market_overview():
    """预热市场概览"""
    log("预热市场概览...")
    start = time.time()
    
    cache_file = os.path.join(PREHEAT_DIR, f"market_overview.pkl")
    latest_date = get_latest_trade_date()
    
    manifest = load_manifest()
    need_update = True
    
    if os.path.exists(cache_file) and 'market_overview' in manifest:
        details = manifest['market_overview'].get('details', {})
        cached_date = details.get('trade_date', '')
        if cached_date == latest_date:
            log(f"  市场概览已是最新 ({latest_date})")
            need_update = False
    
    if need_update:
        df = get_market_overview(refresh=True)
        if df is not None and len(df) > 0:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
            update_manifest('market_overview', 'ok', {
                'count': len(df),
                'trade_date': latest_date,
                'cache_file': cache_file
            })
            log(f"  完成: {len(df)} 条记录，耗时 {time.time()-start:.1f}s")
            return True
        else:
            update_manifest('market_overview', 'failed', {'error': '无数据'})
            log("  失败: 无法获取数据", 'ERROR')
            return False
    
    return True


def preheat_daily_all(force=False):
    """预热全量日线数据（核心功能）- 使用并发优化"""
    log("预热全量日线数据...")
    start = time.time()
    
    cache_file = os.path.join(PREHEAT_DIR, f"daily_all.pkl")
    n_days = getattr(config, 'PREHEAT_DAYS_LOOKBACK', 60)
    
    # 检查是否需要更新
    manifest = load_manifest()
    need_update = True
    
    if os.path.exists(cache_file) and not force:
        if 'daily_all' in manifest:
            details = manifest['daily_all'].get('details', {})
            cached_date = details.get('trade_date', '')
            cached_days = details.get('days', 0)
            latest_date = get_latest_trade_date()
            
            if cached_date == latest_date and cached_days >= n_days:
                log(f"  日线数据已是最新 ({cached_date}, {cached_days}天)")
                need_update = False
    
    if need_update:
        # 获取候选股票池
        log("  获取候选股票池...")
        candidates = get_candidate_pool(stage=1)
        codes = candidates['ts_code'].tolist()
        log(f"  候选股票: {len(codes)} 只")
        
        # 使用并发获取
        all_data = []
        batch_size = getattr(config, 'BATCH_SIZE', 100)
        interval = getattr(config, 'BATCH_INTERVAL', 0.3)
        max_workers = getattr(config, 'MAX_WORKERS', 10)
        
        total = len(codes)
        log(f"  开始并发获取 {total} 只股票日线数据...")
        log(f"  [并发] batch={batch_size}, workers={max_workers}, interval={interval}s")
        
        # 并发获取
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def get_one(code):
            try:
                df = get_daily_price(code, 
                    start_date=(datetime.now() - timedelta(days=n_days+10)).strftime('%Y%m%d'),
                    end_date=get_latest_trade_date())
                if df is not None and len(df) > 0:
                    # 添加股票名称和行业
                    stock_info = candidates[candidates['ts_code'] == code]
                    if len(stock_info) > 0:
                        df['name'] = stock_info.iloc[0]['name']
                        df['industry'] = stock_info.iloc[0]['industry']
                return df
            except:
                return None
        
        for i in range(0, total, batch_size):
            batch = codes[i:i+batch_size]
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(get_one, c): c for c in batch}
                for f in as_completed(futures):
                    df = f.result()
                    if df is not None and len(df) > 0:
                        all_data.append(df)
            
            # 进度
            done = min(i + batch_size, total)
            progress = done / total * 100
            log(f"  进度: {done}/{total} ({progress:.1f}%)", 'PROGRESS')
            
            # 间隔防限流
            if i + batch_size < total:
                time.sleep(interval)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            
            # 保存缓存
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            
            latest_date = get_latest_trade_date()
            unique_dates = result['trade_date'].nunique() if 'trade_date' in result.columns else 0
            
            update_manifest('daily_all', 'ok', {
                'count': len(result),
                'stocks': total,
                'trade_date': latest_date,
                'days': unique_dates,
                'cache_file': cache_file
            })
            
            elapsed = time.time() - start
            log(f"  完成: {len(result)} 条记录，{unique_dates} 个交易日，耗时 {elapsed:.1f}s")
            return True
        else:
            update_manifest('daily_all', 'failed', {'error': '无数据'})
            log("  失败: 无法获取数据", 'ERROR')
            return False
    
    return True


# ============ 增量更新 ============

def incremental_update():
    """增量更新检查"""
    log("=" * 50)
    log("检查增量更新...")
    log("=" * 50)
    
    manifest = load_manifest()
    results = {
        'cache_hit': 0,
        'updated': 0,
        'failed': 0
    }
    
    # 检查每种缓存
    cache_types = ['stock_basic', 'market_overview', 'daily_all']
    
    for cache_type in cache_types:
        cache_file = os.path.join(PREHEAT_DIR, f"{cache_type}.pkl")
        
        if os.path.exists(cache_file):
            mtime = os.path.getmtime(cache_file)
            age_hours = (time.time() - mtime) / 3600
            ttl = getattr(config, 'PREHEAT_CACHE_TTL', 16)
            
            if age_hours < ttl:
                results['cache_hit'] += 1
                log(f"  {cache_type}: 缓存命中 ({age_hours:.1f}h < {ttl}h)")
            else:
                log(f"  {cache_type}: 缓存过期 ({age_hours:.1f}h > {ttl}h)，需要更新")
                results['updated'] += 1
        else:
            log(f"  {cache_type}: 缓存不存在，需要获取")
            results['updated'] += 1
    
    return results


# ============ 缓存统计 ============

def show_stats():
    """显示缓存统计"""
    log("=" * 50)
    log("缓存状态统计")
    log("=" * 50)
    
    manifest = load_manifest()
    
    if not manifest:
        log("暂无缓存记录")
        return
    
    for category, info in manifest.items():
        status = info.get('status', 'unknown')
        updated = info.get('updated', 'unknown')
        details = info.get('details', {})
        
        log(f"\n{category}:")
        log(f"  状态: {status}")
        log(f"  更新时间: {updated}")
        
        for k, v in details.items():
            log(f"  {k}: {v}")
    
    # 计算总缓存大小
    total_size = 0
    for f in os.listdir(PREHEAT_DIR):
        if f.endswith('.pkl'):
            total_size += os.path.getsize(os.path.join(PREHEAT_DIR, f))
    
    log(f"\n总缓存大小: {total_size / 1024 / 1024:.1f} MB")


# ============ 主函数 ============

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='盘前预热脚本')
    parser.add_argument('--check', action='store_true', help='只检查缓存状态')
    parser.add_argument('--force', action='store_true', help='强制重新预热')
    parser.add_argument('--stats', action='store_true', help='显示缓存统计')
    parser.add_argument('--target', type=str, default='all', 
                        choices=['all', 'stock_basic', 'market_overview', 'daily_all'],
                        help='指定预热目标')
    
    args = parser.parse_args()
    
    log("=" * 50)
    log("Phase 4: 盘前预热")
    log(f"缓存目录: {PREHEAT_DIR}")
    log("=" * 50)
    
    if args.stats:
        show_stats()
        return
    
    if args.check:
        results = incremental_update()
        log(f"\n缓存命中: {results['cache_hit']}")
        log(f"需要更新: {results['updated']}")
        return
    
    # 完整预热流程
    target = args.target
    force = args.force
    
    overall_start = time.time()
    success_count = 0
    total_count = 0
    
    # 按优先级预热
    if target == 'all':
        targets = config.PREHEAT_PRIORITY
    else:
        targets = [target]
    
    for t in targets:
        total_count += 1
        log(f"\n--- 预热: {t} ---")
        
        if t == 'stock_basic':
            ok = preheat_stock_basic()
        elif t == 'market_overview':
            ok = preheat_market_overview()
        elif t == 'daily_all':
            ok = preheat_daily_all(force=force)
        else:
            log(f"  未知目标: {t}", 'WARN')
            continue
        
        if ok:
            success_count += 1
    
    elapsed = time.time() - overall_start
    
    log("\n" + "=" * 50)
    log("预热完成")
    log("=" * 50)
    log(f"成功: {success_count}/{total_count}")
    log(f"总耗时: {elapsed:.1f}s")
    
    # 显示最终统计
    show_stats()


if __name__ == "__main__":
    main()
