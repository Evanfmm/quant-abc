# A股量化交易系统 (quant-abc)

> A股多因子量化交易系统，支持因子选股、风险控制、仓位管理

## 项目简介

quant-abc 是一个基于多因子模型的A股量化交易系统，采用Python开发，支持因子选股、风险控制、仓位管理和回测功能。系统采用模块化设计，包含因子模型、风控模块、数据缓存、券商API对接等核心组件。

**核心特性：**
- 🎯 多因子选股模型（价值、成长、动量、质量、风险、情绪）
- 🛡️ 完善的风控体系（仓位、回撤、止损、行业限制）
- ⚡ 高效数据缓存（分层缓存 + 自动过期清理）
- 🔌 券商API对接（支持Tushare、富途、聚宽）

## 功能特点

### 1. 因子模型

| 因子类别 | 权重 | 因子列表 |
|---------|------|----------|
| 价值因子 | 20% | PE、PB、PS（市销率） |
| 成长因子 | 20% | 营收增速、利润增速 |
| 动量因子 | 15% | 5日/20日动量、动量反转 |
| 质量因子 | 25% | ROE、ROA、毛利率 |
| 风险因子 | 10% | 小市值、低波动 |
| 情绪因子 | 10% | 换手率、量比 |

**短线增强版 (v3)** 采用资金流向+动量为核心：
- 资金流向 30%（大单净流入、主力占比）
- 动量/趋势 25%（5日涨幅、突破强度）
- 情绪/量能 25%（换手率、量比、振幅）
- 风险 15%（波动率、市值）

### 2. 风控模块

- **仓位限制**：最多持仓4只，单只不超过30%
- **回撤控制**：最大回撤15%预警
- **行业限制**：单行业不超过30%
- **止损机制**：
  - 8%固定止损
  - 5%跟踪止损（盈利>15%后启用）
  - 20%止盈
- **时间止损**：持有超过5天无盈利强制卖出
- **VaR计算**：风险价值评估

### 3. 仓位管理

支持多种仓位算法：
- 等权重仓位
- Kelly公式仓位
- 风险平价仓位
- 波动率加权仓位

### 4. 数据缓存

- **分层缓存**：内存缓存(LRU) + 磁盘缓存
- **过期机制**：默认4小时过期，财务数据7天
- **自动清理**：过期数据自动清理
- **统计监控**：缓存命中率统计

### 5. 券商API对接

| Broker类型 | 说明 | 状态 |
|-----------|------|------|
| Tushare | 数据源模拟 | ✅ 可用 |
| Backtest | 回测专用 | ✅ 可用 |
| 富途证券 | 需安装futu-api | 🔧 预留 |
| 聚宽 | 需安装jqdata | 🔧 预留 |

## 文件结构

```
quant-abc/
├── config.py                  # 配置文件
├── data_fetcher.py            # 数据获取（缓存优化）
├── factor_model.py            # 原始因子模型
├── factor_model_enhanced.py   # 增强因子模型
├── factor_model_v2.py         # 因子模型v2
├── factor_model_v3.py         # 因子模型v3（短线增强）
├── factor_model_shortterm.py  # 短线因子模型
├── risk_manager.py            # 风控模块
├── broker_api.py              # 券商API
├── cache_manager.py           # 缓存管理
├── trading_signal.py          # 交易信号（含风控）
├── technical_indicators.py    # 技术指标
├── daily_report.py            # 每日报告
├── main.py                    # 主程序
└── main_enhanced.py           # 增强版主程序
```

## 快速开始

### 环境要求

```bash
pip install pandas numpy tushare
```

### 配置

在 `config.py` 中设置：

```python
TUSHARE_TOKEN = "你的token"
INITIAL_CAPITAL = 50000  # 初始资金
MAX_POSITION = 4         # 最多持仓数
```

### 运行

**基础版（原有功能）**
```bash
python main.py --report
```

**增强版（推荐）**
```bash
python main_enhanced.py --enhanced
```

**回测模式**
```bash
python main_enhanced.py --backtest
```

**券商模拟**
```bash
python main_enhanced.py --broker
```

**清空缓存**
```bash
python main_enhanced.py --clear-cache
```

## 待办事项

- [ ] 实盘券商对接：完成富途/聚宽API对接
- [ ] 因子回测：对新因子进行历史回测验证
- [ ] 机器学习因子：引入ML预测模型
- [ ] 组合优化：引入Mean-Variance优化
- [ ] 实时行情：接入实时行情websocket

---

*A股量化交易系统 - quant-abc*
