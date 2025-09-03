# Baseball Scraping

NPBの打者/投手のスタッツをスクレイピングしてS3に保存するためのスクリプト群。

## ディレクトリ構成
- `src/` …… スクリプト本体（例：`scrape_hitters_vs_stadium_all.py` など）
- `conf/` …… 設定ファイル（`.env` は **コミットしない**）
- `logs/` …… 実行ログ（**コミットしない**）
- `samples/` …… サンプル小サイズデータ（共有したい最小限のみ）

## 事前準備
- Python 3.10+
- `pip install -r requirements.txt`
- `.env` に S3 バケット等の設定（**このファイルは管理対象外**）

## 使い方（例）
```bash
python src/scrape_hitters_vs_stadium_all.py --year 2025
