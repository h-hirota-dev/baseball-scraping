# -*- coding: utf-8 -*-
import os
import re
import time
import unicodedata
import hashlib
import pandas as pd

# ===== 設定 =====
OUTPUT_ROOT = "/home/ec2-user/batch/data/team_splits/hitters/vs_team"
CSV_ENCODING = "utf-8-sig"   # Excel互換（端末表示だけなら "utf-8" でもOK）
SLEEP_SEC = 1.0
TOTAL_LABEL = "通算"

# リーグ → {英字チーム名: [サイト内チーム記号候補]}
TEAM_CODE_CANDIDATES = {
    "Pacific": {
        "Fighters": ["F"],        # 日本ハム
        "Softbank": ["H"],        # ソフトバンク
        "Lotte":    ["M"],        # ロッテ
        "Rakuten":  ["E"],        # 楽天
        "Orix":     ["B"],        # オリックス
        "Seibu":    ["L"],        # 西武
    },
    "Central": {
        "Giants":   ["G"],        # 巨人
        "Tigers":   ["T"],        # 阪神
        "BayStars": ["DB", "YB"], # DeNA は DB/YB 両対応
        "Carp":     ["C"],        # 広島
        "Dragons":  ["D"],        # 中日
        "Swallows": ["S"],        # ヤクルト
    }
}

# 相手チーム名の日本語→英字（unknown回避）
OPPONENT_NAME_MAP = {
    # パ
    "日本ハム": "Fighters", "北海道日本ハム": "Fighters",
    "ソフトバンク": "Softbank", "福岡ソフトバンク": "Softbank",
    "ロッテ": "Lotte", "千葉ロッテ": "Lotte",
    "楽天": "Rakuten", "東北楽天": "Rakuten",
    "オリックス": "Orix",
    "西武": "Seibu", "埼玉西武": "Seibu",
    # セ
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
    """相手名を英字に（辞書→ASCII→ハッシュ）"""
    s = unicodedata.normalize("NFKC", str(name)).strip()
    for jp, en in OPPONENT_NAME_MAP.items():
        if jp in s:
            return en
    slug = ascii_slug(s)
    if re.search(r'[A-Za-z0-9]', slug):
        return slug
    return "u" + hashlib.md5(s.encode("utf-8")).hexdigest()[:8]

def read_hitters_vs_team_table(url: str) -> pd.DataFrame:
    """3段ヘッダの表を取得（cp932）。"""
    tables = pd.read_html(url, flavor="lxml", encoding="cp932", header=[0, 1, 2])
    if not tables:
        raise RuntimeError("テーブルが見つかりませんでした")
    df = tables[0]

    # ヘッダ正規化：前後空白除去・「 本塁」のような先頭空白・全角を吸収
    def norm3(col):
        a, b, c = [unicodedata.normalize("NFKC", str(x)).strip() for x in col]
        b = b.replace("Ｈ", "H").replace("Ｓ", "S").replace("本塁", "本塁")  # 念のためNFKC後にtrim
        return (a, b, c)
    df.columns = pd.MultiIndex.from_tuples([norm3(col) for col in df.columns])
    return df

def extract_id_cols(df: pd.DataFrame):
    """ID列（背番・名前・席）を特定。なければ左3列フォールバック。"""
    id_cols = []
    for col in df.columns:
        a, b, c = col
        if a == b and c == "合計" and a in ("背番", "名前", "席"):
            id_cols.append(col)
    if not id_cols:
        id_cols = [df.columns[i] for i in range(min(3, len(df.columns)))]
    return id_cols

def save_one_team(df: pd.DataFrame, league: str, team_en: str):
    """1球団分を相手チームごとにCSV保存。"""
    id_cols = extract_id_cols(df)

    # 第1階層（通算/相手チーム名）
    level0_vals = list(dict.fromkeys([col[0] for col in df.columns]))
    id_level0 = set([c[0] for c in id_cols])
    targets = [v for v in level0_vals if v not in id_level0]  # '通算' と各チーム

    base_out_dir = os.path.join(OUTPUT_ROOT, league, team_en)
    ensure_dir(base_out_dir)

    saved = 0
    for tgt in targets:
        tgt_cols = [col for col in df.columns if col[0] == tgt]
        if not tgt_cols:
            continue

        sub = df[id_cols + tgt_cols].copy()

        # 列をフラット化：IDは '背番','名前','席'、相手側は第2階層（試合/打率/本塁/打点/盗塁）
        new_cols = []
        for a, b, c in sub.columns:
            if (a, b, c) in id_cols:
                new_cols.append(a)
            else:
                new_cols.append(b)
        sub.columns = new_cols

        # 数値化（打率は .247 のような文字列でも to_numeric でOK）
        for col in sub.columns:
            if col in ("背番", "名前", "席"):
                continue
            sub[col] = pd.to_numeric(sub[col], errors="coerce")

        # ファイル名：通算は Total.csv、それ以外は相手チーム英字名.csv
        fname = "Total.csv" if tgt == TOTAL_LABEL else f"{opponent_ascii(tgt)}.csv"
        out_path = os.path.join(base_out_dir, fname)
        sub.to_csv(out_path, index=False, encoding=CSV_ENCODING)
        saved += 1
        print(f"    - 保存: {out_path} ({len(sub)}行)")
    return saved

def scrape_all():
    ensure_dir(OUTPUT_ROOT)
    grand_total = 0
    for league, team_map in TEAM_CODE_CANDIDATES.items():
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] === {league}（打者×対チーム）===")
        for team_en, code_candidates in team_map.items():
            df = None
            last_err = None
            for code in code_candidates:
                url = f"https://nf3.sakura.ne.jp/{league}/{code}/t/fp_all_data_vsT.htm"
                try:
                    print(f"[{time.strftime('%H:%M:%S')}] 取得: {league} / {team_en} -> {url}")
                    df = read_hitters_vs_team_table(url)
                    break
                except Exception as e:
                    last_err = e
                    print(f"  失敗: {url} ({e})")
                    time.sleep(0.3)
            if df is None:
                print(f"  × 断念: {league} / {team_en}（全候補失敗）: {last_err}")
                continue

            saved = save_one_team(df, league, team_en)
            grand_total += saved
            time.sleep(SLEEP_SEC)
    print(f"=== 完了: 総ファイル数 {grand_total} ===")

if __name__ == "__main__":
    scrape_all()

