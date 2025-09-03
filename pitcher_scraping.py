import pandas as pd
import os
from datetime import datetime

# === ログ設定 ===
log_dir = "/home/ec2-user/batch/logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"pitcher_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")

log("=== ピッチャーデータ取得処理 開始 ===")

# 球団とURLの辞書
teams = {
    "fighters": "https://baseball-data.com/stats/pitcher-f/",
    "hawks": "https://baseball-data.com/stats/pitcher-h/",
    "buffaloes": "https://baseball-data.com/stats/pitcher-bs/",
    "marines": "https://baseball-data.com/stats/pitcher-m/",
    "lions": "https://baseball-data.com/stats/pitcher-l/",
    "eagles": "https://baseball-data.com/stats/pitcher-e/",
    "giants": "https://baseball-data.com/stats/pitcher-g/",
    "tigers": "https://baseball-data.com/stats/pitcher-t/",
    "swallows": "https://baseball-data.com/stats/pitcher-s/",
    "dragons": "https://baseball-data.com/stats/pitcher-d/",
    "carp": "https://baseball-data.com/stats/pitcher-c/",
    "baystars": "https://baseball-data.com/stats/pitcher-yb/"
}

# 出力先ディレクトリ
output_dir = "/home/ec2-user/batch/data/pitcher"
os.makedirs(output_dir, exist_ok=True)

# ピッチャーのカラム定義
columns = [
    "背番号", "選手名", "防御率", "試合", "勝利", "敗北", "セーブ", "ホールド",
    "勝率", "打者", "投球回", "被安打", "被本塁打", "与四球", "与死球", "奪三振",
    "失点", "自責点", "WHIP", "DIPS"
]

# 各チームのデータ取得処理
for team_key, url in teams.items():
    try:
        log(f"[{team_key}] URL取得開始：{url}")
        tables = pd.read_html(url)
        df = tables[0]

        if df.iloc[0].equals(df.iloc[1]):
            df = df.drop(1).reset_index(drop=True)

        df.columns = columns
        output_path = os.path.join(output_dir, f"{team_key}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"[{team_key}] データ保存成功：{output_path}")

    except Exception as e:
        log(f"[{team_key}] エラー発生：{e}")

log("=== ピッチャーデータ取得処理 完了 ===")

