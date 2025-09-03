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

SCRIPT_NAME = Path(__file__).name
LOG_FILE_PATH = os.path.join(LOG_DIR, SCRIPT_NAME)  # .py のまま保存（必要なら .log に変更可）

def log(msg: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ===== 設定 =====
OUTPUT_ROOT = "/home/ec2-user/batch/data/team_splits/pitchers/vs_stadium"
CSV_ENCODING = "utf-8-sig"
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

# 球場名の日本語 → 英字スラッグ（表記ゆれも吸収）
STADIUM_NAME_MAP = {
    "エスコンＦ": "escon_field_hokkaido",
    "エスコンF": "escon_field_hokkaido",  # 半角Fの揺れも明示的に吸収
    "みずほPayPay": "mizuhopaypay_dome",
    "ZOZOマリン": "zozo_marine",
    "楽天モバイル": "rakuten_mobile_park",
    "京セラD大阪": "kyocera_dome_osaka",
    "ベルーナドーム": "belluna_dome",
    "東京ドーム": "tokyo_dome",
    "神宮": "jinguu_stadium",
    "横浜": "yokohama_stadium",
    "バンテリンドーム": "banterin_dome_nagoya",
    "マツダ": "mazda_stadium",
    "甲子園": "koshien_stadium",
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
    球場名を英字スラッグ化：辞書（キーも正規化して比較）→ ASCII 整形 → ハッシュ。
    全角/半角スペース・全角英字も吸収。『エスコンＦ』『エスコンF』なども同一扱い。
    """
    def norm(s: str) -> str:
        s = unicodedata.normalize("NFKC", str(s))
        return s.replace(" ", "").replace("\u3000", "")

    s_norm = norm(name)

    # ★ キー側も正規化して部分一致で比較
    for jp, slug in STADIUM_NAME_MAP.items():
        if norm(jp) in s_norm:
            return slug

    # 英数字が含まれるならASCII整形
    slug = ascii_slug(s_norm)
    if re.search(r'[A-Za-z0-9]', slug):
        if len(slug) <= 2:
            log(f"[WARN] stadium slug suspicious: original='{name}', slug='{slug}'")
        return slug

    # それ以外はハッシュ化（unknown回避）
    h = "u" + hashlib.md5(s_norm.encode("utf-8")).hexdigest()[:8]
    log(f"[WARN] stadium fallback to hash: original='{name}', slug='{h}'")
    return h

def read_pitchers_vs_stadium_table(url: str) -> pd.DataFrame:
    """3段ヘッダの表を取得（cp932）。"""
    tables = pd.read_html(url, flavor="lxml", encoding="cp932", header=[0, 1, 2])
    if not tables:
        raise RuntimeError("テーブルが見つかりませんでした")
    df = tables[0]

    # ヘッダ正規化：前後空白・全角→半角、球場名の空白除去
    def norm3(col):
        a, b, c = [unicodedata.normalize("NFKC", str(x)).strip() for x in col]
        a = a.replace(" ", "").replace("\u3000", "")  # 球場名（第1階層）
        # 投手側は第2階層が 先/リ/防/勝/敗/H/S など
        b = b.replace("Ｈ", "H").replace("Ｓ", "S")
        return (a, b, c)

    df.columns = pd.MultiIndex.from_tuples([norm3(col) for col in df.columns])
    return df

def extract_id_cols(df: pd.DataFrame):
    """ID列（背番・名前・腕）。無ければ左3列フォールバック。"""
    id_cols = []
    for col in df.columns:
        a, b, c = col
        if a == b and c == "合計" and a in ("背番", "名前", "腕"):
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

        # 列フラット化：IDは『背番・名前・腕』、球場側は第2階層（先/リ/防/勝/敗/H/S）
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

        # 保存：通算は Total.csv、それ以外は球場スラッグ
        fname = "Total.csv" if tgt == TOTAL_LABEL else f"{stadium_ascii(tgt)}.csv"
        out_path = os.path.join(base_out_dir, fname)
        sub.to_csv(out_path, index=False, encoding=CSV_ENCODING)
        saved += 1
        log(f"保存: {out_path} ({len(sub)}行)")
    return saved

def scrape_all():
    ensure_dir(OUTPUT_ROOT)
    grand_total = 0
    for league, team_map in TEAM_CODE_CANDIDATES.items():
        log(f"=== {league}（投手×球場）===")
        for team_en, code_candidates in team_map.items():
            df = None
            last_err = None
            for code in code_candidates:
                url = f"https://nf3.sakura.ne.jp/{league}/{code}/t/pc_all_data_vsS.htm"
                try:
                    log(f"取得: {league} / {team_en} -> {url}")
                    df = read_pitchers_vs_stadium_table(url)
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

