import requests
from bs4 import BeautifulSoup
import csv
import boto3
from datetime import datetime

# 取得対象のURL
url = "https://baseball-data.com/stats/hitter-f/"
response = requests.get(url)
response.encoding = response.apparent_encoding

# HTMLを解析
soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table", {"id": "tblTableSorter_stats"})

# エラーチェック
if table is None:
    print("⚠️ テーブルが見つかりません")
    exit()

# ヘッダー抽出
headers = [th.text.strip() for th in table.find_all("tr")[0].find_all("th")]

# データ行の抽出
rows = []
for tr in table.find_all("tr")[1:]:
    cols = [td.text.strip() for td in tr.find_all("td")]
    if cols:
        rows.append(cols)

# CSVファイルに書き出し
csv_filename = f"baseball_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
with open(csv_filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"CSVファイル作成: {csv_filename}")
