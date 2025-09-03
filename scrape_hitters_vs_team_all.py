# -*- coding: utf-8 -*-
import os
import re
import time
import unicodedata
import hashlib
import pandas as pd
from datetime import datetime
from pathlib import Path

# ===== ログ設定 =====
LOG_DIR = "/home/ec2-user/batch/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# 実行中のスクリプト名を取得して、そのままログファイル名にする
SCRIPT_NAME = Path(__file__).name
LOG_FILE_PATH = os.path.join(LOG_DIR, SCRIPT_NAME)  # 例: /home/ec2-user/batch/logs/scrape_hitters_vs_team_all.py

# ※ 拡張子を .log にしたい場合はこちらを使用（上の行はコメントアウト）
# LOG_FILE_PATH = os.path.join(LOG_DIR, Path(SCRIPT_NAME).with_suffix(".log").name)

def log(msg: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ===== 設定 =====
OUTPUT_ROOT = "/home/ec2-user/batch/data/team_splits/hitters/vs_team"
CSV_ENCODING = "utf-8-sig"
SLEEP_SEC = 1.0
TOTAL_LABEL = "通算"

TEAM_CODE_CANDIDATES = {
    "Pacific": {
        "Fighters": ["F"],
        "Softbank": ["H"],
        "Lotte":    ["M"],
        "Rakuten":  ["E"],
        "Orix":     ["B"],
        "Seibu":    ["L"],
    },
    "Central": {
        "Giants":   ["G"],
        "Tigers":   ["T"],
        "BayStars": ["DB", "YB"],
        "Carp":     ["C"],
        "Dragons":  ["D"],
        "Swallows": ["S"],
    }
}

OPPONENT_NAME_MAP = {
    "日本ハム": "Fighters", "北海道日本ハム": "Fighters",
    "ソフトバンク": "Softbank", "福岡ソフトバンク": "Softbank",
    "ロッテ": "Lotte", "千葉ロッテ": "Lotte",
    "楽天": "Rakuten", "東北楽天": "Rakuten",
    "オリックス": "Orix",
    "西武": "Seibu", "埼玉西武": "Seibu",
    "巨人": "Giants", "読売": "Giants", "読売巨人": "Giants",
    "阪神": "Tigers",
    "DeNA": "BayStars", "ＤｅＮＡ": "BayStars", "横浜DeNA": "BayStars",
    "ベイスターズ": "BayStars", "横浜": "BayStars",
    "広島": "Carp", "広島東洋": "Carp",
    "中日": "Dragons",
    "ヤクルト": "Swallows", "東京ヤクルト": "Swallows",
}

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def ascii_slug(s: str) -> str:
    n = re.sub(r'[\\/:*?"<>|]+', '_', str(s))
    n = re.sub(r'[^0-9A-Za-z_.-]+', '_', n)
    n = n.strip("._")
    return n or "unknown"

def opponent_ascii(name: str) -> str:
    s = unicodedata.normalize("NFKC", str(name)).strip()
    for jp, en in OPPONENT_NAME_MAP.items():
        if jp in s:
            return en
    slug = ascii_slug(s)
    if re.search(r'[A-Za-z0-9]', slug):
        return slug
    return "u" + hashlib.md5(s.encode("utf-8")).hexdigest()[:8]

def read_hitters_vs_team_table(url: str) -> pd.DataFrame:
    tables = pd.read_html(url, flavor="lxml", encoding="cp932", header=[0, 1, 2])
    if not tables:
        raise RuntimeError("テーブルが見つかりませんでした")
    df = tables[0]

    def norm3(col):
        a, b, c = [unicodedata.normalize("NFKC", str(x)).strip() for x in col]
        b = b.replace("Ｈ", "H").replace("Ｓ", "S").replace("本塁", "本塁")
        return (a, b, c)
    df.columns = pd.MultiIndex.from_tuples([norm3(col) for col in df.columns])
    return df

def extract_id_cols(df: pd.DataFrame):
    id_cols = []
    for col in df.columns:
        a, b, c = col
        if a == b and c == "合計" and a in ("背番", "名前", "席"):
            id_cols.append(col)
    if not id_cols:
        id_cols = [df.columns[i] for i in range(min(3, len(df.columns)))]
    return id_cols

def save_one_team(df: pd.DataFrame, league: str, team_en: str):
    id_cols = extract_id_cols(df)
    level0_vals = list(dict.fromkeys([col[0] for col in df.columns]))
    id_level0 = set([c[0] for c in id_cols])
    targets = [v for v in level0_vals if v not in id_level0]

    base_out_dir = os.path.join(OUTPUT_ROOT, league, team_en)
    ensure_dir(base_out_dir)

    saved = 0
    for tgt in targets:
        tgt_cols = [col for col in df.columns if col[0] == tgt]
        if not tgt_cols:
            continue

        sub = df[id_cols + tgt_cols].copy()

        new_cols = []
        for a, b, c in sub.columns:
            if (a, b, c) in id_cols:
                new_cols.append(a)
            else:
                new_cols.append(b)
        sub.columns = new_cols

        for col in sub.columns:
            if col in ("背番", "名前", "席"):
                continue
            sub[col] = pd.to_numeric(sub[col], errors="coerce")

        fname = "Total.csv" if tgt == TOTAL_LABEL else f"{opponent_ascii(tgt)}.csv"
        out_path = os.path.join(base_out_dir, fname)
        sub.to_csv(out_path, index=False, encoding=CSV_ENCODING)
        saved += 1
        log(f"保存: {out_path} ({len(sub)}行)")
    return saved

def scrape_all():
    ensure_dir(OUTPUT_ROOT)
    grand_total = 0
    for league, team_map in TEAM_CODE_CANDIDATES.items():
        log(f"=== {league}（打者×対チーム）===")
        for team_en, code_candidates in team_map.items():
            df = None
            last_err = None
            for code in code_candidates:
                url = f"https://nf3.sakura.ne.jp/{league}/{code}/t/fp_all_data_vsT.htm"
                try:
                    log(f"取得: {league} / {team_en} -> {url}")
                    df = read_hitters_vs_team_table(url)
                    break
                except Exception as e:
                    last_err = e
                    log(f"失敗: {url} ({e})")
                    time.sleep(0.3)
            if df is None:
                log(f"× 断念: {league} / {team_en}（全候補失敗）: {last_err}")
                continue

            saved = save_one_team(df, league, team_en)
            grand_total += saved
            time.sleep(SLEEP_SEC)
    log(f"=== 完了: 総ファイル数 {grand_total} ===")

if __name__ == "__main__":
    scrape_all()

