#!/bin/bash

# ログ出力用
LOGFILE="/home/ec2-user/batch/logs/upload_s3_$(date '+%Y%m%d_%H%M%S').log"

# S3アップロード処理
echo "=== S3アップロード開始 $(date) ===" > "$LOGFILE"

/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/batter/ s3://hirota-work/databricks/batting/2025/ --recursive >> "$LOGFILE" 2>&1
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/pitcher/ s3://hirota-work/databricks/pitcher/2025/ --recursive >> "$LOGFILE" 2>&1
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/matches/ s3://hirota-work/databricks/games/ --recursive >> "$LOGFILE" 2>&1

echo "=== S3アップロード完了 $(date) ===" >> "$LOGFILE"

