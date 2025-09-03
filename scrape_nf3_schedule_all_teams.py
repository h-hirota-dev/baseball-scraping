# -*- coding: utf-8 -*-
"""
scrape_nf3_schedule_all_teams.py
- nf3.sakura.ne.jp から試合日程（指定カラム）を取得
- セ・パ全12球団、月3〜11
- チームごとに CSV を保存（UTF-8 BOM）
- ログは /home/ec2-user/batch/logs/scrape_nf3_schedule_all_teams.log に追記
- 列は MultiIndex 対応（下段優先）
- 月ごとに空データはスキップ
- leg は優先値で試行、ダメなら 0/1/2 をフォールバックで自動再試行
"""

import os
import time
import logging
from urllib.parse import urlencode
import pandas as pd

# ====== 設定 ======
BASE_URL = "https://nf3.sakura.ne.jp/php/stat_disp/stat_disp.php"

# 保存先
SAVE_DIR = "/home/ec2-user/batch/data/matches"
LOG_DIR  = "/home/ec2-user/batch/logs"
LOG_FILE = os.path.join(LOG_DIR, "scrape_nf3_schedule_all_teams.log")

# コンソールにもログを出したい場合は True
CONSOLE_LOG = True

# 対象チーム（tm はサイトのクエリ、leg は優先的に使いたい値）
TEAMS = {
    # パ・リーグ
    "Fighters":  {"tm": "F",  "leg": 1},
    "Buffaloes": {"tm": "B",  "leg": 1},
    "Lions":     {"tm": "L",  "leg": 1},
    "Hawks":     {"tm": "H",  "leg": 1},
    "Marines":   {"tm": "M",  "leg": 1},
    "Eagles":    {"tm": "E",  "leg": 1},
    # セ・リーグ
    "Giants":    {"tm": "G",  "leg": 0},
    "Tigers":    {"tm": "T",  "leg": 0},
    "BayStars":  {"tm": "DB", "leg": 0},
    "Dragons":   {"tm": "D",  "leg": 0},
    "Carp":      {"tm": "C",  "leg": 0},
    "Swallows":  {"tm": "S",  "leg": 0},
}

# 取得する月（3〜11月）
MONTHS = list(range(3, 12))

# 取りたい列
NEEDED_COLS = ["日付", "曜", "対戦T", "球場", "H/V", "開始"]

# 連続アクセスの間隔（秒）
REQUEST_INTERVAL = 1.0
# ====== 設定ここまで ======


def setup_logging():
    os.makedirs(SAVE_DIR, exist_ok=True)
    os.makedirs(LOG_DIR,  exist_ok=True)

    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if CONSOLE_LOG:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger("").addHandler(console)


def build_url(params: dict) -> str:
    return f"{BASE_URL}?{urlencode(params)}"


def read_table(url: str):
    """
    HTMLの最初のテーブルを返す。文字コードは cp932 を優先し、失敗時はデフォルト。
    見つからなければ None。
    """
    try:
        tables = pd.read_html(url, encoding="cp932")
    except Exception:
        tables = pd.read_html(url)
    if not tables:
        return None
    return tables[0]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    複数行ヘッダ(MultiIndex) → 下段優先でフラット化。単一行なら strip のみ。
    """
    if isinstance(df.columns, pd.MultiIndex):
        flat = []
        for col in df.columns:
            lower = (col[1] if len(col) > 1 else "")
            upper = col[0]
            name = str(lower).strip() if str(lower).strip() else str(upper).strip()
            flat.append(name)
        df.columns = flat
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def fetch_table_any_leg(team_code: str, mon: int, leg_preferred: int) -> pd.DataFrame:
    """
    指定 leg で取得を試み、ダメなら他の leg(0/1/2)も順に再試行。
    取得できた最初の DataFrame を返す。空表はエラー扱い。
    """
    legs_to_try = [leg_preferred] + [l for l in (0, 1, 2) if l != leg_preferred]
    last_err = None

    for leg in legs_to_try:
        params = {"y": 0, "leg": leg, "tm": team_code, "mon": mon, "vst": "all"}
        url = build_url(params)
        try:
            df = read_table(url)
            if df is None:
                raise ValueError("テーブルが見つかりませんでした")
            df = normalize_columns(df)

            exist = [c for c in NEEDED_COLS if c in df.columns]
            if not exist:
                raise KeyError(f"必要列が見つからない: {df.columns.tolist()}")

            df = df[exist].copy()

            # 不要行除去
            if "日付" in df.columns:
                df = df[df["日付"].notna()]
                df = df[~df["日付"].astype(str).str.contains("合計|計|日付", na=False)]

            # 行が0ならスキップ（空月）
            if df.empty:
                raise ValueError("データ行が0件（その月は試合なしor掲載なし）")

            # メタ情報
            df["month"] = mon
            df["team_code"] = team_code
            df["leg_used"] = leg
            df["source_url"] = url

            return df.reset_index(drop=True)

        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"取得失敗 tm={team_code} mon={mon}: {last_err}")


def main():
    setup_logging()
    logging.info("=== スクレイピング開始 ===")

    for team_name, meta in TEAMS.items():
        team_code = meta["tm"]
        leg_pref  = meta["leg"]
        all_months = []

        for mon in MONTHS:
            logging.info(f"Fetching {team_name} (tm={team_code}) mon={mon} pref_leg={leg_pref}")
            try:
                df = fetch_table_any_leg(team_code, mon, leg_pref)

                # 安全なログ（空対策：uniqueを使う）
                leg_vals = sorted(set(df.get("leg_used", [])))
                logging.info(f" -> rows={len(df)} leg_used={leg_vals}")

                all_months.append(df)
            except Exception as e:
                logging.warning(f"取得失敗: team={team_name} mon={mon} reason={e}")

            time.sleep(REQUEST_INTERVAL)  # アクセス間隔

        if all_months:
            try:
                result = pd.concat(all_months, ignore_index=True)

                # 列順整える（リスト結合で列選択。DataFrameの「+」はNG）
                ordered = [c for c in NEEDED_COLS if c in result.columns]
                extra   = [c for c in ["month", "team_code", "leg_used", "source_url"] if c in result.columns]
                result  = result[ordered + extra]

                out_csv = os.path.join(SAVE_DIR, f"{team_name}.csv")
                result.to_csv(out_csv, index=False, encoding="utf-8-sig")
                logging.info(f"[DONE] {team_name}: {len(result)} rows saved -> {out_csv}")
            except Exception as e:
                logging.error(f"[ERROR] 保存処理で失敗: team={team_name} reason={e}")
        else:
            logging.warning(f"データなし: {team_name}")

    logging.info("=== スクレイピング終了 ===")


if __name__ == "__main__":
    main()

