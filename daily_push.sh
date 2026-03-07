#!/bin/bash
# 每日盘前量化策略推送脚本
# 运行时间: 每个交易日 8:30

cd /home/evanfan/.openclaw/workspace-team/quant-abc
TOKEN="8330779132:AAHWnqLYqc0YWAx9fhYgObCUHBDlWZf1vNg"
CHAT_ID="-5145500882"

# 运行量化系统
python3 main.py > /tmp/quant_output.txt 2>&1

# 提取推荐股票部分
RECOMMENDATIONS=$(sed -n '/【推荐股票/,/【信号统计/p' /tmp/quant_output.txt)

# 清理格式，发送到Telegram
MESSAGE=$(echo "$RECOMMENDATIONS" | sed 's/├─/•/g' | sed 's/└─/•/g' | sed 's/│/ /g' | sed 's/📈/📈/g' | sed 's/📉/📉/g' | sed 's/📊/📊/g' | sed 's/🛡️/🛡️/g' | sed 's/🎯/🎯/g' | sed 's/⏰/⏰/g' | sed 's/💡/💡/g')

curl -s -X POST "https://api.telegram.org/bot$TOKEN/sendMessage" \
  -d "chat_id=$CHAT_ID" \
  -d "text=📊 *每日盘前量化策略推送*

$MESSAGE" \
  -d "parse_mode=Markdown" > /tmp/telegram_send.log 2>&1

echo "推送完成 $(date)" >> /home/evanfan/.openclaw/workspace-team/quant-abc/cron.log
