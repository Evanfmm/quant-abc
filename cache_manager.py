"""
A股量化交易系统 - 数据缓存管理模块
分层缓存策略，支持内存缓存、文件缓存、API缓存
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import hashlib
import pickle
import time
from collections import OrderedDict
import config


class CacheEntry:
    """缓存条目"""
    
    def __init__(self, key, value, ttl=3600):
        self.key = key
        self.value = value
        self.created_at = time.time()
        self.ttl = ttl
        self.access_count = 0
        self.last_access = self.created_at
    
    def is_expired(self):
        """检查是否过期"""
        return time.time() - self.created_at > self.ttl
    
    def access(self):
        """访问缓存"""
        self.access_count += 1
        self.last_access = time.time()


class LRUCache:
    """LRU内存缓存"""
    
    def __init__(self, max_size=100):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get(self, key):
        """获取缓存"""
        if key in self.cache:
            entry = self.cache[key]
            if not entry.is_expired():
                # 移到末尾
                self.cache.move_to_end(key)
                entry.access()
                self.hits += 1
                return entry.value
            else:
                # 过期删除
                del self.cache[key]
        self.misses += 1
        return None
    
    def set(self, key, value, ttl=3600):
        """设置缓存"""
        # 如果存在，先删除
        if key in self.cache:
            del self.cache[key]
        
        # 如果满了，删除最旧的
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        
        self.cache[key] = CacheEntry(key, value, ttl)
    
    def delete(self, key):
        """删除缓存"""
        if key in self.cache:
            del self.cache[key]
    
    def clear(self):
        """清空缓存"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self):
        """获取统计信息"""
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': hit_rate
        }
    
    def cleanup_expired(self):
        """清理过期缓存"""
        expired_keys = [k for k, v in self.cache.items() if v.is_expired()]
        for key in expired_keys:
            del self.cache[key]
        return len(expired_keys)


class DataCache:
    """数据缓存管理器"""
    
    def __init__(self):
        # 内存缓存
        self.memory_cache = LRUCache(max_size=50)
        
        # 文件缓存目录
        self.cache_dir = config.CACHE_CONFIG.get('cache_dir', 'data/cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 缓存统计
        self.stats = {
            'memory_hits': 0,
            'memory_misses': 0,
            'disk_hits': 0,
            'disk_misses': 0,
            'api_calls': 0
        }
    
    def _get_cache_key(self, prefix, *args, **kwargs):
        """生成缓存键"""
        key_str = f"{prefix}:{args}:{sorted(kwargs.items())}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, key, use_disk=True):
        """
        获取缓存
        先查内存，再查磁盘
        """
        # 查内存
        value = self.memory_cache.get(key)
        if value is not None:
            self.stats['memory_hits'] += 1
            return value
        
        self.stats['memory_misses'] += 1
        
        # 查磁盘
        if use_disk:
            disk_path = os.path.join(self.cache_dir, f"{key}.pkl")
            if os.path.exists(disk_path):
                # 检查过期
                mtime = os.path.getmtime(disk_path)
                if time.time() - mtime < config.CACHE_CONFIG.get('ttl_seconds', 14400):
                    try:
                        with open(disk_path, 'rb') as f:
                            value = pickle.load(f)
                        # 加载到内存
                        self.memory_cache.set(key, value)
                        self.stats['disk_hits'] += 1
                        return value
                    except:
                        pass
                else:
                    # 过期删除
                    os.remove(disk_path)
        
        self.stats['disk_misses'] += 1
        return None
    
    def set(self, key, value, ttl=None, use_disk=True):
        """
        设置缓存
        同时写入内存和磁盘
        """
        if ttl is None:
            ttl = config.CACHE_CONFIG.get('ttl_seconds', 14400)
        
        # 写入内存
        self.memory_cache.set(key, value, ttl)
        
        # 写入磁盘
        if use_disk:
            disk_path = os.path.join(self.cache_dir, f"{key}.pkl")
            try:
                with open(disk_path, 'wb') as f:
                    pickle.dump(value, f)
            except:
                pass
    
    def delete(self, key):
        """删除缓存"""
        self.memory_cache.delete(key)
        disk_path = os.path.join(self.cache_dir, f"{key}.pkl")
        if os.path.exists(disk_path):
            os.remove(disk_path)
    
    def clear_all(self):
        """清空所有缓存"""
        self.memory_cache.clear()
        for f in os.listdir(self.cache_dir):
            if f.endswith('.pkl'):
                os.remove(os.path.join(self.cache_dir, f))
    
    def get_stats(self):
        """获取缓存统计"""
        return {
            **self.stats,
            'memory': self.memory_cache.get_stats(),
            'disk_cache_size': self._get_disk_cache_size()
        }
    
    def _get_disk_cache_size(self):
        """获取磁盘缓存大小"""
        total = 0
        for f in os.listdir(self.cache_dir):
            if f.endswith('.pkl'):
                total += os.path.getsize(os.path.join(self.cache_dir, f))
        return total / (1024 * 1024)  # MB


class DataFetcherWithCache:
    """带缓存的数据获取器"""
    
    def __init__(self):
        self.cache = DataCache()
        self.data_fetcher = None  # 实际的数据获取器
    
    def set_fetcher(self, fetcher):
        """设置实际的数据获取器"""
        self.data_fetcher = fetcher
    
    def get_market_data(self, trade_date=None, use_cache=True):
        """
        获取市场数据（带缓存）
        """
        cache_key = self.cache._get_cache_key('market', trade_date)
        
        if use_cache:
            data = self.cache.get(cache_key)
            if data is not None:
                print(f"使用缓存数据: market_{trade_date}")
                return data
        
        # 实际获取
        self.cache.stats['api_calls'] += 1
        if self.data_fetcher:
            data = self.data_fetcher.get_market_overview()
        else:
            import data_fetcher
            data = data_fetcher.get_market_overview()
        
        # 缓存
        if data is not None and len(data) > 0:
            self.cache.set(cache_key, data)
        
        return data
    
    def get_stock_daily(self, ts_code, start_date=None, end_date=None, use_cache=True):
        """
        获取个股日线（带缓存）
        """
        cache_key = self.cache._get_cache_key('daily', ts_code, start_date, end_date)
        
        if use_cache:
            data = self.cache.get(cache_key)
            if data is not None:
                return data
        
        # 实际获取
        self.cache.stats['api_calls'] += 1
        if self.data_fetcher:
            data = self.data_fetcher.get_daily_price(ts_code, start_date, end_date)
        else:
            import data_fetcher
            data = data_fetcher.get_daily_price(ts_code, start_date, end_date)
        
        if data is not None:
            self.cache.set(cache_key, data)
        
        return data
    
    def get_financial_data(self, ts_code, use_cache=True):
        """获取财务数据（带缓存）"""
        cache_key = self.cache._get_cache_key('financial', ts_code)
        
        if use_cache:
            data = self.cache.get(cache_key)
            if data is not None:
                return data
        
        self.cache.stats['api_calls'] += 1
        if self.data_fetcher:
            data = self.data_fetcher.get_financial_data(ts_code)
        else:
            import data_fetcher
            data = data_fetcher.get_financial_data(ts_code)
        
        if data is not None:
            self.cache.set(cache_key, data, ttl=86400 * 7)  # 财务数据缓存7天
        
        return data
    
    def invalidate(self, pattern=None):
        """
        失效缓存
        pattern: 可选，失效匹配的缓存
        """
        if pattern is None:
            self.cache.clear_all()
        else:
            # 简化处理：清空所有
            self.cache.clear_all()
    
    def get_cache_stats(self):
        """获取缓存统计"""
        return self.cache.get_stats()


# 全局缓存实例
_global_cache = DataCache()


def get_cache():
    """获取全局缓存实例"""
    return _global_cache


def cached(key_prefix, ttl=None):
    """
    缓存装饰器
    用法:
        @cached('my_data', ttl=3600)
        def get_data():
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            cache_key = _global_cache._get_cache_key(key_prefix, *args, **kwargs)
            
            result = _global_cache.get(cache_key)
            if result is not None:
                return result
            
            result = func(*args, **kwargs)
            
            if result is not None:
                _global_cache.set(cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator


if __name__ == "__main__":
    # 测试
    print("测试缓存系统")
    
    cache = DataCache()
    
    # 测试写入
    cache.set('test_key', {'data': [1, 2, 3]})
    
    # 测试读取
    result = cache.get('test_key')
    print(f"读取结果: {result}")
    
    # 统计
    print(f"\n缓存统计: {cache.get_stats()}")
    
    # 测试LRU
    print("\n测试LRU缓存")
    lru = LRUCache(max_size=3)
    lru.set('a', 1)
    lru.set('b', 2)
    lru.set('c', 3)
    print(f"a: {lru.get('a')}")  # 命中
    lru.set('d', 4)  # 淘汰a
    print(f"a after d: {lru.get('a')}")  # 未命中
    print(f"stats: {lru.get_stats()}")
