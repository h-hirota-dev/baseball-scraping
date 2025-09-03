import os
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

# === 保存先の設定 ===
url = "https://baseball-data.com/team/pitcher.html"
save_dir = "/home/ec2-user/batch/data/team_pitcher"
os.makedirs(save_dir, exist_ok=True)
save_path = os.path.join(save_dir, "team_pitcher_stats_2025.csv")

# ✅ ログ保存先（修正箇所）
log_dir = "/home/ec2-user/batch/logs"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, f"log_team_pitcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# === ログ関数 ===
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

# === 実行処理 ===
try:
    log("チーム投手成績の取得開始")

    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    res = requests.get(url, headers=headers)
    res.encoding = res.apparent_encoding

    # ⛳ HTMLのテーブル読み込み（pandas推奨形式）
    tables = pd.read_html(StringIO(res.text))

    df_central = tables[0].copy()
    df_pacific = tables[1].copy()

    df_central["リーグ"] = "セ・リーグ"
    df_pacific["リーグ"] = "パ・リーグ"

    df_all = pd.concat([df_central, df_pacific], ignore_index=True)

    df_all.to_csv(save_path, index=False, encoding="utf-8-sig")
    log(f"保存完了: {save_path}")
    log("処理正常終了")

except Exception as e:
    log(f"エラー発生: {e}")

