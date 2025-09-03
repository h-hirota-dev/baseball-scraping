# -*- coding: utf-8 -*-
import os
import re
import time
import unicodedata
import hashlib
import pandas as pd

# ===== 設定 =====
OUTPUT_ROOT = "/home/ec2-user/batch/data/team_splits/hitters/vs_stadium"
CSV_ENCODING = "utf-8-sig"   # Excel互換（端末表示優先なら "utf-8" でもOK）
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
        "BayStars": ["DB", "YB"], # DeNA（表記ゆれ両対応）
        "Carp":     ["C"],        # 広島
        "Dragons":  ["D"],        # 中日
        "Swallows": ["S"],        # ヤクルト
    }
}

# よく出る球場名 → 英字スラッグ（表記ゆれも吸収）
STADIUM_NAME_MAP = {
    # パ主要
    "エスコンＦ": "escon_field_hokkaido",
    "みずほPayPay": "mizuhopaypay_dome",
    "ZOZOマリン": "zozo_marine",
    "楽天モバイル": "rakuten_mobile_park",
    "京セラD大阪": "kyocera_dome_osaka",
    "ベルーナドーム": "belluna_dome",
    # セ主要
    "東京ドーム": "tokyo_dome",
    "神宮": "jinguu_stadium",
    "横浜": "yokohama_stadium",
    "バンテリンドーム": "banterin_dome_nagoya",
    "マツダ": "mazda_stadium",
    "甲子園": "koshien_stadium",
    # その他
    "札幌ドーム": "sapporo_dome",
    "ほっと神戸": "hotto_motto_kobe",
    "北九州": "kitakyushu",
    "PayPay": "paypay_dome",
    "その他": "others",
}

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def ascii_slug(s: str) -> str:
    n = re.sub(r'[\\/:*?"<>|]+', '_', str(s))
    n = re.sub(r'[^0-9A-Za-z_.-]+', '_', n)
    n = n.strip("._")
    return n or "unknown"

def stadium_ascii(name: str) -> str:
    """
    球場名を英字スラッグ化：辞書 → ASCII整形 → ハッシュの順でunknown回避。
    全角・空白の表記ゆれ（例: '京セラD大 阪', '楽天モ バイル'）も正規化。
    """
    s = unicodedata.normalize("NFKC", str(name))
    s = s.replace(" ", "").replace("\u3000", "")  # 半角/全角スペース除去
    for jp, slug in STADIUM_NAME_MAP.items():
        if jp in s:
            return slug
    slug = ascii_slug(s)
    if re.search(r'[A-Za-z0-9]', slug):
        return slug
    return "u" + hashlib.md5(s.encode("utf-8")).hexdigest()[:8]

def read_hitters_vs_stadium_table(url: str) -> pd.DataFrame:
    """3段ヘッダを取得（cp932）。"""
    tables = pd.read_html(url, flavor="lxml", encoding="cp932", header=[0, 1, 2])
    if not tables:
        raise RuntimeError("テーブルが見つかりませんでした")
    df = tables[0]

    # ヘッダ正規化：前後空白除去・全角→半角・球場名の空白除去
    def norm3(col):
        a, b, c = [unicodedata.normalize("NFKC", str(x)).strip() for x in col]
        a = a.replace(" ", "").replace("\u3000", "")  # 球場名
        return (a, b, c)
    df.columns = pd.MultiIndex.from_tuples([norm3(col) for col in df.columns])
    return df

def extract_id_cols(df: pd.DataFrame):
    """ID列（背番・名前・席）。無ければ左3列フォールバック。"""
    id_cols = []
    for col in df.columns:
        a, b, c = col
        if a == b and c == "合計" and a in ("背番", "名前", "席"):
            id_cols.append(col)
    if not id_cols:
        id_cols = [df.columns[i] for i in range(min(3, len(df.columns)))]
    return id_cols

def save_one_team(df: pd.DataFrame, league: str, team_en: str):
    """1球団分を球場ごとにCSV保存。"""
    id_cols = extract_id_cols(df)

    # 第1階層（通算/各球場）
    level0_vals = list(dict.fromkeys([col[0] for col in df.columns]))
    id_level0 = set([c[0] for c in id_cols])
    targets = [v for v in level0_vals if v not in id_level0]  # '通算' と各球場

    base_out_dir = os.path.join(OUTPUT_ROOT, league, team_en)
    ensure_dir(base_out_dir)

    saved = 0
    for tgt in targets:
        tgt_cols = [col for col in df.columns if col[0] == tgt]
        if not tgt_cols:
            continue

        sub = df[id_cols + tgt_cols].copy()

        # 列フラット化：IDは『背番・名前・席』、球場側は第2階層（試合/打率/本塁/打点/盗塁）
        new_cols = []
        for a, b, c in sub.columns:
            if (a, b, c) in id_cols:
                new_cols.append(a)
            else:
                new_cols.append(b)
        sub.columns = new_cols

        # 数値化（打率は '.247' でもOK、エラーは NaN）
        for col in sub.columns:
            if col in ("背番", "名前", "席"):
                continue
            sub[col] = pd.to_numeric(sub[col], errors="coerce")

        # 保存：通算は Total.csv、それ以外は球場スラッグ
        fname = "Total.csv" if tgt == TOTAL_LABEL else f"{stadium_ascii(tgt)}.csv"
        out_path = os.path.join(base_out_dir, fname)
        sub.to_csv(out_path, index=False, encoding=CSV_ENCODING)
        saved += 1
        print(f"    - 保存: {out_path} ({len(sub)}行)")
    return saved

def scrape_all():
    ensure_dir(OUTPUT_ROOT)
    grand_total = 0
    for league, team_map in TEAM_CODE_CANDIDATES.items():
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] === {league}（打者×球場）===")
        for team_en, code_candidates in team_map.items():
            df = None
            last_err = None
            for code in code_candidates:
                url = f"https://nf3.sakura.ne.jp/{league}/{code}/t/fp_all_data_vsS.htm"
                try:
                    print(f"[{time.strftime('%H:%M:%S')}] 取得: {league} / {team_en} -> {url}")
                    df = read_hitters_vs_stadium_table(url)
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

