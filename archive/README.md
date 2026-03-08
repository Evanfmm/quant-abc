# Archive - 历史版本目录

本目录包含 quant-abc 项目的历史版本和实验版本，**不再维护**。

## 历史入口文件

| 文件 | 说明 | 状态 |
|------|------|------|
| `main.py` | 旧版入口，使用 ShortTermFactorModelV3 | 已归档 |
| `main_enhanced.py` | 增强版入口，支持多模式 | 已归档 |

## 历史因子模型

| 文件 | 说明 | 状态 |
|------|------|------|
| `factor_model.py` | 原始多因子模型 | **仍在使用**（被 daily_report.py 引用）|
| `factor_model_enhanced.py` | 增强版因子模型 | 已归档 |
| `factor_model_v2.py` | 短线因子模型 v2 | 已归档 |
| `factor_model_shortterm.py` | 短线因子模型 v1 | 已归档 |

## 当前正式版本

- **正式入口**: `../main_production.py`
- **正式因子模型**: `../factor_model_v3.py`

---
*本文件最后更新: 2026-03-08*
