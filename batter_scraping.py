import pandas as pd
import os
from datetime import datetime

# === ログ設定 ===
log_dir = "/home/ec2-user/batch/logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"batter_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")

log("=== バッターデータ取得処理 開始 ===")

# 球団名とURLのマッピング
teams = {
    "fighters": "https://baseball-data.com/stats/hitter-f/",
    "hawks": "https://baseball-data.com/stats/hitter-h/",
    "buffaloes": "https://baseball-data.com/stats/hitter-bs/",
    "marines": "https://baseball-data.com/stats/hitter-m/",
    "lions": "https://baseball-data.com/stats/hitter-l/",
    "eagles": "https://baseball-data.com/stats/hitter-e/",
    "giants": "https://baseball-data.com/stats/hitter-g/",
    "tigers": "https://baseball-data.com/stats/hitter-t/",
    "swallows": "https://baseball-data.com/stats/hitter-s/",
    "dragons": "https://baseball-data.com/stats/hitter-d/",
    "carp": "https://baseball-data.com/stats/hitter-c/",
    "baystars": "https://baseball-data.com/stats/hitter-yb/"
}

# 出力先ディレクトリ
output_dir = "/home/ec2-user/batch/data/batter"
os.makedirs(output_dir, exist_ok=True)

# カラム名定義
columns = [
    "背番号", "選手名", "打率", "試合", "打席数", "打数", "安打", "本塁打",
    "打点", "盗塁", "四球", "死球", "三振", "犠打", "併殺打",
    "出塁率", "長打率", "OPS", "RC27", "XR27"
]

# 各球団のデータ取得＆保存
for team_key, url in teams.items():
    try:
        log(f"[{team_key}] URL取得開始：{url}")
        tables = pd.read_html(url)
        df = tables[0]

        # 1行目と2行目が同じなら2行目削除
        if df.iloc[0].equals(df.iloc[1]):
            df = df.drop(1).reset_index(drop=True)

        df.columns = columns
        output_path = os.path.join(output_dir, f"{team_key}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"[{team_key}] データ保存成功：{output_path}")

    except Exception as e:
        log(f"[{team_key}] エラー発生：{e}")

log("=== バッターデータ取得処理 完了 ===")

