import pandas as pd
import os
from datetime import datetime

# ログ出力用
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

log("=== チーム打撃成績の取得処理 開始 ===")

# URL
url = "https://baseball-data.com/team/hitter.html"

try:
    # pandasでテーブルを一括取得
    tables = pd.read_html(url)
    log(f"取得テーブル数: {len(tables)}")

    # 通常、セリーグ・パリーグの順番で取得される
    central_df = tables[0].copy()
    pacific_df = tables[1].copy()

    # データ整形：チーム列の名前統一や余分な行の削除（必要に応じて）
    central_df["リーグ"] = "セ・リーグ"
    pacific_df["リーグ"] = "パ・リーグ"
    df = pd.concat([central_df, pacific_df], ignore_index=True)

    # 保存先ディレクトリとファイル名
    output_dir = "/home/ec2-user/batch/data/team_batting"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "team_batting_stats_2025.csv")

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    log(f"打撃成績をCSVとして保存しました：{output_path}")

except Exception as e:
    log(f"エラー発生：{e}")

log("=== チーム打撃成績の取得処理 完了 ===")

