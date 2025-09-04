#!/bin/bash

# .env を読み込み
set -a
source /home/ec2-user/batch/.env
set +a

# ログ出力用
LOGFILE="${LOG_DIR}/upload_s3_$(date '+%Y%m%d_%H%M%S').log"

echo "=== S3 アップロード開始 $(date) ===" > "$LOGFILE"

# バッター成績
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/batter/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/batting/${S3_YEAR}/" --recursive >> "$LOGFILE" 2>&1

# ピッチャー成績
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/pitcher/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/pitcher/${S3_YEAR}/" --recursive >> "$LOGFILE" 2>&1

# 試合日程
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/matches/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/games/" --recursive >> "$LOGFILE" 2>&1

# チーム打撃成績
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/team_batting/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/team_batting/" --recursive >> "$LOGFILE" 2>&1

# チーム投手成績
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/team_pitcher/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/team_pitcher/" --recursive >> "$LOGFILE" 2>&1

# チーム守備成績
/usr/local/bin/aws s3 cp "${LOCAL_DATA_DIR}/team_defense/" \
  "s3://${S3_BUCKET}/${S3_PREFIX}/team_defense/" --recursive >> "$LOGFILE" 2>&1

