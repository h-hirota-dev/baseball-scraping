import pandas as pd
import requests
import os
from datetime import datetime

# === ログ設定 ===
log_dir = "/home/ec2-user/batch/logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"games_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")

log("=== チーム試合数取得処理 開始 ===")

# URL
url = "https://baseball-data.com/team/standings.html"

try:
    tables = pd.read_html(url)
    log(f"{len(tables)} tables found")
    log(f"セ・リーグ columns: {list(tables[0].columns)}")
    log(f"パ・リーグ columns: {list(tables[1].columns)}")

    central_df = tables[0][["チーム", "試 合"]].copy()
    pacific_df = tables[1][["チーム", "試 合"]].copy()

    # チーム名マッピング
    team_mapping = {
        "阪神": "tigers",
        "広島": "carp",
        "DeNA": "baystars",
        "巨人": "giants",
        "中日": "dragons",
        "ヤクルト": "swallows",
        "ソフトバンク": "hawks",
        "ロッテ": "marines",
        "西武": "lions",
        "楽天": "eagles",
        "オリックス": "buffaloes",
        "日本ハム": "fighters"
    }

    def add_english_team_name(df):
        df["team"] = df["チーム"].map(team_mapping)
        df["games"] = df["試 合"].astype(int)
        return df[["team", "games"]]

    central_df = add_english_team_name(central_df)
    pacific_df = add_english_team_name(pacific_df)

    team_games_df = pd.concat([central_df, pacific_df], ignore_index=True)
    log("取得したチーム試合数データ：")
    log(f"\n{team_games_df.to_string(index=False)}")

    # 保存処理
    output_dir = "/home/ec2-user/batch/data/matches"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "team_games.csv")
    team_games_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    log(f"チーム試合数データを {output_path} に保存しました。")

except Exception as e:
    log(f"エラー発生：{e}")

log("=== チーム試合数取得処理 完了 ===")

