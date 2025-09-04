# Baseball Scraping Batch

NPB（日本プロ野球）の各種成績を毎朝スクレイピングし、CSV を S3 に配置するバッチ一式です。  
EC2 上で動作し、cron により自動実行します。

## 📁 ディレクトリ構成

```bash
/home/ec2-user/batch
├── README.md # 本ドキュメント
├── data/ # 取得CSVの格納先（※Git管理外推奨）
├── logs/ # 実行ログ（※Git管理外推奨）
├── rotate_logs.py # ログローテーション実行スクリプト
├── upload_to_s3.sh # S3 へ一括アップロードするシェル
│
├── batter_scraping.py # 個人打者 成績取得（全体）
├── pitcher_scraping.py # 個人投手 成績取得（全体）
├── games_scraping.py # 各チームの消化試合数取得
├── scrape_nf3_schedule_all_teams.py # 各チームの残試合（対戦予定）取得
│
├── team_batting.py # チーム打撃 成績取得
├── team_pitcher.py # チーム投手 成績取得
├── fielding_central.py # セ・リーグ 守備成績取得
├── fielding_pacific.py # パ・リーグ 守備成績取得
│
├── scrape_hitters_vs_team_all.py # 個人打者 vsチーム 成績取得
├── scrape_pitchers_vs_team_all.py # 個人投手 vsチーム 成績取得
├── scrape_hitters_vs_stadium_all.py # 個人打者 球場別 成績取得
└── scrape_pitchers_vs_stadium_all.py # 個人投手 球場別 成績取得

```

## 🔧 前提・セットアップ

### 1) Python & 依存パッケージ
EC2 に Python3 がインストール済みとします。必要に応じて仮想環境を利用してください。

### 2) 実行権限
```bash
chmod +x /home/ec2-user/batch/upload_to_s3.sh
```

### ▶️ 手動実行の例
```bash
# 個人打者（全体）
python3 /home/ec2-user/batch/batter_scraping.py

# 個人投手（vs 球場）
python3 /home/ec2-user/batch/scrape_pitchers_vs_stadium_all.py

# S3へアップロード
/home/ec2-user/batch/upload_to_s3.sh
```

実行ログは /home/ec2-user/batch/logs/ に出力される想定です（各スクリプトで logging 設定してください）。

### ⏰ 自動実行（cron）
現在の crontab 設定（毎日 08:00 台に各種取得、10:00 に S3 送信、01:10 にログローテーション）：

```bash

# 個人打者・投手_成績取得
00 8 * * * python3 /home/ec2-user/batch/batter_scraping.py
05 8 * * * python3 /home/ec2-user/batch/pitcher_scraping.py

# 消化した試合数の取得
10 8 * * * python3 /home/ec2-user/batch/games_scraping.py

# 残試合数の取得
15 8 * * * python3 /home/ec2-user/batch/scrape_nf3_schedule_all_teams.py

# チーム打撃・投手・守備_成績取得
20 8 * * * python3 /home/ec2-user/batch/team_batting.py
25 8 * * * python3 /home/ec2-user/batch/team_pitcher.py
30 8 * * * python3 /home/ec2-user/batch/fielding_central.py
35 8 * * * python3 /home/ec2-user/batch/fielding_pacific.py

# 個人打撃・投手_対チームの成績取得
40 8 * * * python3 /home/ec2-user/batch/scrape_hitters_vs_team_all.py
45 8 * * * python3 /home/ec2-user/batch/scrape_pitchers_vs_team_all.py

# 個人打撃・投手_球場別の成績取得
50 8 * * * python3 /home/ec2-user/batch/scrape_hitters_vs_stadium_all.py
55 8 * * * python3 /home/ec2-user/batch/scrape_pitchers_vs_stadium_all.py

# S3にcsvデータを配置
00 10 * * * /home/ec2-user/batch/upload_to_s3.sh

# Log rotation for batch logs
10 1 * * * /home/ec2-user/batch/rotate_logs.py >> /home/ec2-user/batch/logs/rotate_logs_runner.log 2>&1

```

編集は crontab -e、確認は crontab -l

### ☁️ S3 への配置

 - upload_to_s3.sh で data/ 配下の CSV をバケットへ同期します。

 - 例：

```bash
aws s3 sync "/home/ec2-user/batch/data" "s3://${S3_BUCKET}/${S3_PREFIX}/" \
  --exclude "*.tmp" --delete
```

注意：S3 のリージョン、暗号化、ライフサイクル設定は要件に合わせて設定してください。

🧹 ログローテーション

 - rotate_logs.py は /home/ec2-user/batch/logs 配下のログを日付ベースで圧縮・削除する想定です。

 - 例：7日以上前のログを削除、当日ログは残す等（スクリプト内の保持日数を調整）。

### 🔐 セキュリティ / 運用の注意

- APIキー・秘密情報 は .env や EC2 の IAM ロールで管理し、リポジトリに含めない。

- data/ と logs/ は Git 管理外 を推奨（.gitignore 参照）。

- 大量アクセスとならないよう、スクレイピング間隔やアクセス先サイトの利用規約を遵守してください。

### 📝 Git 運用（超要約）

```bash
# 変更前に最新化
git pull --rebase origin main

# 変更 → コミット → プッシュ
git add <変更ファイル>
git commit -m "update: README / fix scraping"
git push origin main

```

### 🧪 動作確認チェックリスト

 - python3 -V でバージョン確認（3.9+ 推奨）

 - logs/ に出力が出るか

 - data/ に CSV が生成されるか

 - upload_to_s3.sh 実行で S3 に同期されるか

 - cron 実行後に logs/ に追記されているか

### ❓トラブルシューティング

- Permission denied (publickey)：GitHub への SSH 鍵設定を再確認

- S3 への書き込み失敗：IAM 権限／リージョン／バケット名を再確認

- cron が動かない：Python のフルパス指定、実行権限、環境変数の引き継ぎ（/etc/crontabやラッパースクリプトで source ~/.bash_profile）を確認
