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

> **当前唯一正式版本**: `factor_model_v3.py` (短线增强版)

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

> 历史版本已移至 `archive/` 目录

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

### 5. 机器学习预测模块

- **模型支持**：LightGBM（默认）、RandomForest备选
- **预测目标**：预测股票未来N日收益率
- **特征工程**：
  - 技术特征（收盘价、成交量等的滞后、移动平均、波动率）
  - 动量特征（5日/10日动量）
  - 量价特征（量价比）
  - 基本面特征（PE、PB、ROE等）
- **融合方式**：支持加权融合和排名融合与因子分数结合
- **评估指标**：IC（信息系数）、RMSE、回测收益

### 6. 券商API对接

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
├── data_fetcher.py           # 数据获取（含缓存优化）
├── stock_selector.py         # 股票池选择器
├── factor_model_v3.py        # 因子模型（当前唯一正式版本）
├── risk_manager.py           # 风控模块
├── broker_api.py             # 券商API
├── cache_manager.py          # 缓存管理
├── trading_signal.py        # 交易信号（含风控）
├── technical_indicators.py   # 技术指标
├── ml_predictor.py          # 机器学习预测模块
├── daily_report.py          # 每日报告
├── main_production.py       # Production 正式入口
├── preheat.py              # 盘前预热脚本
├── archive/                 # 历史版本（已归档）
└── requirements.txt         # 项目依赖
```

## 快速开始

### 环境要求

```bash
pip install pandas numpy tushare lightgbm scikit-learn
```

### 配置

1. 创建 `.env` 文件（推荐）或设置环境变量：

```bash
# 方式1: 创建 .env 文件
echo "TUSHARE_TOKEN=你的token" > .env

# 方式2: 设置环境变量
export TUSHARE_TOKEN=你的token
```

2. 在 `config.py` 中可调整以下参数：

```python
INITIAL_CAPITAL = 50000  # 初始资金
MAX_POSITION = 4         # 最多持仓数
RUN_MODE = "production"  # 运行模式: test/dev/production
```

### 运行

**Production 模式（推荐 - 正式盘前推荐）**
```bash
python main_production.py
```

**盘前预热（可选，推荐每日开盘前执行）**
```bash
python preheat.py              # 完整预热
python preheat.py --check      # 检查缓存状态
python preheat.py --stats      # 显示缓存统计
```

**开发/测试模式**
```bash
# 修改 config.py 中的 RUN_MODE
RUN_MODE = "test"   # 冒烟测试 (50只)
RUN_MODE = "dev"    # 开发验证 (SH/SZ各25只)
```

### 历史版本

旧版入口已移至 `archive/` 目录：
- `archive/main.py`
- `archive/main_enhanced.py`

当前唯一正式入口: `main_production.py`

## 机器学习模块使用

### 安装依赖

```bash
pip install -r requirements.txt
```

### 依赖说明

| 依赖 | 版本 | 说明 |
|------|------|------|
| lightgbm | ≥3.0.0 | 机器学习模型（GBDT） |
| scikit-learn | ≥1.0.0 | 数据预处理、模型评估 |
| pandas | ≥1.3.0 | 数据处理 |
| numpy | ≥1.20.0 | 数值计算 |
| tushare | ≥1.2.0 | 量化数据接口 |

### 使用示例

**1. 训练模型**

```python
from ml_predictor import MLPredictor, train_and_evaluate

# 训练 LightGBM 模型（默认）
predictor = train_and_evaluate(n_days=60, model_type='lightgbm')

# 或使用 RandomForest
predictor = train_and_evaluate(n_days=60, model_type='randomforest')
```

**2. 预测股票收益**

```python
from ml_predictor import MLPredictor
import data_fetcher

# 获取当前股票数据
current_data = data_fetcher.get_recent_daily_data(n_days=30)

# 初始化并训练模型
predictor = MLPredictor(model_type='lightgbm')
predictor.fit(current_data, target_days=5)

# 预测未来收益
predictions = predictor.predict(current_data)
print(predictions.head(10))
```

**3. 融合因子分数**

```python
from ml_predictor import MLPredictor, predict_with_fusion

# 假设已有因子分数 DataFrame
factor_scores = pd.DataFrame({
    'ts_code': ['000001.SZ', '000002.SZ'],
    'factor_score': [0.8, 0.6]
})

# 融合预测
result = predict_with_fusion(
    predictor, 
    current_data, 
    factor_scores, 
    fusion_method='weighted'  # 或 'rank'
)
print(result.head(10))
```

**4. 查看特征重要性**

```python
importance = predictor.get_feature_importance()
print(importance.head(10))
```

### 命令行运行

```bash
# 直接运行 ML 模块测试
python ml_predictor.py
```

## 待办事项

- [ ] 实盘券商对接：完成富途/聚宽API对接
- [ ] 因子回测：对新因子进行历史回测验证
- [x] 机器学习因子：引入ML预测模型 ✅ 已完成 (ml_predictor.py)
- [ ] 组合优化：引入Mean-Variance优化
- [ ] 实时行情：接入实时行情websocket

---

*A股量化交易系统 - quant-abc*
