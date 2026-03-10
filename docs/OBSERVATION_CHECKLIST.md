# Quant-ABC 自动化运行观察清单

## 每日观察项目

### 1. 检查预热任务 (preheat.py)

| 检查项 | 命令 | 预期结果 |
|--------|------|----------|
| 执行时间 | `tail -5 logs/preheat.log` | 08:15 左右 |
| 执行状态 | `grep "完成" logs/preheat.log` | 退出码: 0 |
| 错误日志 | `cat logs/preheat.error.log` | 空或无新内容 |

### 2. 检查 Production 任务 (main_production.py)

| 检查项 | 命令 | 预期结果 |
|--------|------|----------|
| 执行时间 | `tail -5 logs/production.log` | 08:45 左右 |
| 执行状态 | `grep "完成" logs/production.log` | 耗时 < 1秒 |
| 推荐结果 | `tail -25 logs/production.log` | TOP 20 推荐 |

### 3. 检查缓存状态

| 检查项 | 命令 | 预期结果 |
|--------|------|----------|
| 预热缓存 | `python preheat.py --stats` | daily_all 存在且有效 |
| 缓存命中率 | `grep "缓存命中" logs/production.log` | > 90% |

### 4. 检查日志文件

```bash
# 每日必查命令
cd /Users/evan/.openclaw/workspace-butler/quant-abc

# 1. 预热日志
echo "=== 预热日志 ==="
tail -10 logs/preheat.log

# 2. Production 日志
echo "=== Production 日志 ==="
tail -30 logs/production.log

# 3. 错误日志
echo "=== 错误日志 ==="
cat logs/preheat.error.log
cat logs/production.error.log
```

---

## 问题记录模板

如果发现问题，按以下格式记录：

```
## 问题记录 - [日期]

### 问题描述
[描述问题现象]

### 原因分析
[分析可能原因]

### 影响范围
[影响哪些功能]

### 修复建议
[建议的修复方案]
```

---

## 预期运行结果

### 正常情况

1. **preheat.log**
```
========================================
2026-03-10 08:15:03: 预热任务开始
...
2026-03-10 08:15:20: 预热任务完成 (退出码: 0)
```

2. **production.log**
```
============================================================
Stage1 过滤: 5489 → 5290
...
[Phase4] 预热缓存命中: 5200/5290 (98.3%)
...
完成! 耗时: 0.1秒
```

3. **缓存状态**
```
daily_all:
  状态: ok
  股票数: 5290
```

---

## 快速检查命令

```bash
# 一键检查所有状态
cd /Users/evan/.openclaw/workspace-butler/quant-abc

echo "=== 1. 预热任务 ===" && tail -3 logs/preheat.log
echo ""
echo "=== 2. Production 任务 ===" && tail -5 logs/production.log
echo ""
echo "=== 3. 缓存状态 ===" && python3 preheat.py --stats 2>&1 | grep -A3 "daily_all"
echo ""
echo "=== 4. 错误日志 ===" && wc -l logs/*.error.log
```

---

*最后更新: 2026-03-09*
