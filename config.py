# A股多因子量化交易系统配置 - 短线增强版v2
# 根据策略专家建议修改

# ==================== Tushare API配置 ====================
# 从环境变量或 .env 文件读取 token，安全方式
import os

# 尝试从 .env 文件加载（如果存在）
env_file = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line.startswith('TUSHARE_TOKEN='):
                _, token = line.split('=', 1)
                os.environ['TUSHARE_TOKEN'] = token.strip('"\'')

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")

# ==================== 账户配置 ====================
INITIAL_CAPITAL = 50000  # 初始资金5万
MAX_POSITION = 4  # 最多持仓4只股票
MAX_SINGLE_POSITION = 0.30  # 单只股票最大仓位30%
MAX_DRAWDOWN = 0.15  # 最大回撤15%（从20%下调）
MAX_SINGLE_LOSS = 0.08  # 单只股票最大亏损8%（从15%统一）

# ==================== 交易配置 ====================
MIN_TRADE_AMOUNT = 100  # 最小交易金额100元
COMMISSION_RATE = 0.0003  # 手续费万三
STAMP_TAX = 0.001  # 印花税千一（卖出）

# ==================== 风控配置 - 专家建议加强 ====================
STOP_LOSS_PCT = 0.08  # 止损线 8%（从10%下调）
TAKE_PROFIT_PCT = 0.20  # 止盈线 20%（从15%上调）
TRAILING_STOP_PCT = 0.05  # 跟踪止损 5%（盈利>15%后启用）
TRAILING_STOP_ACTIVATION = 0.15  # 移动止损激活点 15%
MAX_INDUSTRY_WEIGHT = 0.30  # 单行业最大权重30%
DAILY_LOSS_CIRCUIT = 0.05  # 日内熔断：单日亏损>5%暂停交易
MAX_HOLD_DAYS = 5  # 时间止损：持有超过5天无盈利强制卖出

# ==================== 短线增强因子配置 - v3 ====================
# 策略专家建议：资金流向+动量为核心，质量/价值因子大幅降低
SHORT_TERM_FACTORS = {
    # --- 资金流向（核心）30% ---
    "big_order_net": {"weight": 0.20, "column": "big_order_net", "ascending": False},   # 大单净流入
    "main_force_ratio": {"weight": 0.10, "column": "main_force_ratio", "ascending": False},  # 主力占比
    
    # --- 动量/趋势 25% ---
    "momentum_5d": {"weight": 0.15, "column": "momentum_5d", "ascending": False},    # 5日涨幅
    "breakout_strength": {"weight": 0.10, "column": "breakout_strength", "ascending": False},  # 突破强度
    
    # --- 情绪/量能 25% ---
    "turnover_rate": {"weight": 0.10, "column": "turnover_rate", "ascending": False},   # 换手率
    "volume_ratio": {"weight": 0.10, "column": "volume_ratio", "ascending": False},     # 量比
    "amplitude": {"weight": 0.05, "column": "amplitude", "ascending": False},          # 振幅
    
    # --- 风险 15% ---
    "volatility": {"weight": 0.10, "column": "volatility", "ascending": False},  # 高波动（短线需要波动来赚钱）
    "size": {"weight": 0.05, "column": "total_mv", "ascending": True},                 # 小市值溢价
    
    # --- 质量因子（大幅降低至5%）---
    "roe": {"weight": 0.05, "column": "roe", "ascending": False},       # ROE
}

# 原始因子配置（兼容）
FACTORS = {
    "pe_ttm": {"weight": 0.20, "ascending": True},
    "pb": {"weight": 0.15, "ascending": True},
    "roe": {"weight": 0.25, "ascending": False},
    "revenue_growth": {"weight": 0.20, "ascending": False},
    "turnover_rate": {"weight": 0.10, "ascending": False},
    "momentum_5d": {"weight": 0.10, "ascending": False},
}

# 增强版因子配置（兼容）
ENHANCED_FACTORS = SHORT_TERM_FACTORS

# ==================== 选股池过滤条件 ====================
FILTERS = {
    "market_cap_min": 200000,      # 市值最小20亿 (万元)
    "market_cap_max": 5000000,     # 市值最大500亿 (万元)
    "pe_min": 0,               
    "pe_max": 80,               # PE不超80（过滤高估值）
    "pb_max": 10,               # PB不超10
    "price_min": 2,             # 最低股价2元
    "price_max": 150,           # 最高股价150元
    "turnover_min": 1.0,        # 最低换手率1%（短线需要活跃度）
    "exclude_st": True,         # 排除ST股
    "exclude_new": True,        # 排除新股（60日内）
    "roe_min": 0,               # ROE大于0（盈利公司）
    "exclude_loss": True,       # 排除亏损公司
}

# 增强版过滤条件（引用上方定义）
ENHANCED_FILTERS = FILTERS

# ==================== 行业配置 ====================
INDUSTRY_NEUTRAL = True  
MAX_INDUSTRY_WEIGHT = 0.30  

# ==================== 缓存配置 ====================
CACHE_CONFIG = {
    "enabled": True,
    "ttl_seconds": 14400,  # 4小时
}

# ==================== 输出路径 ====================
DATA_DIR = "data"
LOG_DIR = "logs"
OUTPUT_DIR = "output"
BACKTEST_DIR = "backtest"

# ==================== 运行模式配置 ====================
# 可选值: test / dev / production
# - test: 冒烟测试，随机小样本 (50只)
# - dev: 开发验证，SH/SZ分层采样 (各25只)
# - production: 正式推荐，全量候选池 (默认)
RUN_MODE = "production"

# test_mode 配置
TEST_SAMPLE_SIZE = 50

# dev_mode 配置
DEV_SH_SAMPLE = 25
DEV_SZ_SAMPLE = 25
