# -*- coding: utf-8 -*-
import os
import re
import time
import pandas as pd

URL = "https://nf3.sakura.ne.jp/Pacific/F/t/pc_all_data_vsT.htm"

# ===== 設定 =====
OUTPUT_DIR = "/home/ec2-user/batch/data/team_splits/pitchers/vs_team"
CSV_ENCODING = "utf-8-sig"   # Excel互換。端末表示だけ重視なら "utf-8"
USE_ASCII_FILENAME = True    # 文字化け防止のため英字ファイル名を推奨
TEAM_NAME_MAP = {
    "通算": "Total",
    "ソフトバンク": "Softbank",
    "ロッテ": "Lotte",
    "楽天": "Rakuten",
    "オリックス": "Orix",
    "西武": "Seibu",
    "日本ハム": "Fighters",
}

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def ascii_name(name: str) -> str:
    if name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name]
    n = re.sub(r'[\\/:*?"<>|]+', '_', str(name))
    n = re.sub(r'[^0-9A-Za-z_.-]+', '_', n)
    n = n.strip("._")
    return n or "unknown"

def sanitize_filename(name: str) -> str:
    return ascii_name(name) if USE_ASCII_FILENAME else (str(name) or "unknown")

def scrape_pitchers_vs_team(url: str, out_dir: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 取得開始: {url}")

    # ★ ここがポイント：encoding="cp932" を read_html に直接渡す
    tables = pd.read_html(url, flavor="lxml", encoding="cp932", header=[0,1,2])
    if not tables:
        raise RuntimeError("テーブルが見つかりませんでした。")
    df = tables[0]

    # 列名のトリム＆全角→半角（H/S）だけ軽く正規化
    def norm3(col):
        a, b, c = [str(x).strip() for x in col]
        b = b.replace("Ｈ", "H").replace("Ｓ", "S")
        return (a, b, c)
    df.columns = pd.MultiIndex.from_tuples([norm3(col) for col in df.columns])

    # （デバッグ）ここで日本語に見えるか一度確認
    print("== level0 (先頭10) ==", list(dict.fromkeys([c[0] for c in df.columns]))[:10])
    print("== sample cols ==", df.columns[:10].tolist())

    # ID列（背番/名前/腕）
    id_cols = []
    for col in df.columns:
        a, b, c = col
        if a == b and c == "合計" and a in ("背番", "名前", "腕"):
            id_cols.append(col)
    if not id_cols:
        id_cols = [df.columns[i] for i in range(min(3, len(df.columns)))]

    # 第1階層の「通算／各チーム名」
    level0_vals = list(dict.fromkeys([col[0] for col in df.columns]))
    id_level0 = set([c[0] for c in id_cols])
    targets = [v for v in level0_vals if v not in id_level0]

    ensure_dir(out_dir)

    saved = 0
    for tgt in targets:
        tgt_cols = [col for col in df.columns if col[0] == tgt]
        if not tgt_cols:
            continue

        sub = df[id_cols + tgt_cols].copy()

        # 列をフラット化：IDは '背番','名前','腕'、チーム側は第2階層（先/リ/防/勝/敗/H/S）
        new_cols = []
        for a, b, c in sub.columns:
            if (a, b, c) in id_cols:
                new_cols.append(a)
            else:
                new_cols.append(b)
        sub.columns = new_cols

        # 数値化
        for col in sub.columns:
            if col in ("背番", "名前", "腕"):
                continue
            sub[col] = pd.to_numeric(sub[col], errors="coerce")

        # 保存（中身は UTF-8-SIG）
        fname = sanitize_filename(tgt) + ".csv"
        path = os.path.join(out_dir, fname)
        sub.to_csv(path, index=False, encoding=CSV_ENCODING)
        print(f"  - 保存: {path} ({len(sub)}行, enc={CSV_ENCODING})")
        saved += 1

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 完了: {saved}ファイル")
    return saved

if __name__ == "__main__":
    scrape_pitchers_vs_team(URL, OUTPUT_DIR)

