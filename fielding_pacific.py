import pandas as pd
import requests
from datetime import datetime
import os
from io import StringIO
import re

# === 保存先設定 ===
save_dir = "/home/ec2-user/batch/data/team_defense"
os.makedirs(save_dir, exist_ok=True)
save_path = os.path.join(save_dir, "team_fielding_stats_pacific_2025.csv")  # パ・リーグと分かるように命名

# === ログ設定 ===
log_dir = "/home/ec2-user/batch/logs"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, f"log_fielding_pacific_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# === ログ出力関数 ===
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

# === 重複カラム名を1つに変換 ===
def clean_column_name(name):
    match = re.match(r"^(.+?)\1$", name)
    return match.group(1) if match else name

# === 実行処理 ===
try:
    log("パ・リーグ チーム守備成績の取得開始")

    url = "https://npb.jp/bis/2025/stats/tmf_p.html"  # ← パ・リーグ用URL
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    response.encoding = response.apparent_encoding

    tables = pd.read_html(StringIO(response.text))
    df = tables[0]  # 最初のテーブルが守備成績

    # カラム名を整形
    df.columns = [''.join(col).strip().replace(" ", "") for col in df.columns]
    df.columns = [clean_column_name(col) for col in df.columns]
    log(f"カラム一覧: {df.columns.tolist()}")

    # チーム名の空白除去
    df["チーム"] = df["チーム"].str.replace(r"\s+", "", regex=True)

    # リーグ名追加
    df["リーグ"] = "パ・リーグ"

    # CSV保存
    df.to_csv(save_path, index=False, encoding="utf-8-sig")
    log(f"保存完了: {save_path}")
    log("処理正常終了")

except Exception as e:
    log(f"エラー発生: {e}")

