#!/bin/bash

# ログ出力用
LOGFILE="/home/ec2-user/batch/logs/upload_s3_$(date '+%Y%m%d_%H%M%S').log"

# S3 アップロード処理開始
echo "=== S3 アップロード開始 $(date) ===" > "$LOGFILE"

# バッター成績
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/batter/ \
  s3://hirota-work/databricks/batting/2025/ --recursive >> "$LOGFILE" 2>&1

# ピッチャー成績
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/pitcher/ \
  s3://hirota-work/databricks/pitcher/2025/ --recursive >> "$LOGFILE" 2>&1

# 試合日程
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/matches/ \
  s3://hirota-work/databricks/games/ --recursive >> "$LOGFILE" 2>&1

# チーム打撃成績
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/team_batting/ \
  s3://hirota-work/databricks/team_batting/ --recursive >> "$LOGFILE" 2>&1

# チーム投手成績
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/team_pitcher/ \
  s3://hirota-work/databricks/team_pitcher/ --recursive >> "$LOGFILE" 2>&1

# チーム守備成績
/usr/local/bin/aws s3 cp /home/ec2-user/batch/data/team_defense/ \
  s3://hirota-work/databricks/team_defense/ --recursive >> "$LOGFILE" 2>&1
