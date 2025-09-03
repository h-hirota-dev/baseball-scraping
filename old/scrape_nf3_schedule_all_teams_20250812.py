import time
import pandas as pd
from urllib.parse import urlencode
import os
import logging

BASE_URL = "https://nf3.sakura.ne.jp/php/stat_disp/stat_disp.php"
SAVE_DIR = "/home/ec2-user/batch/data/matches"
LOG_DIR = "/home/ec2-user/batch/logs"
LOG_FILE = os.path.join(LOG_DIR, "scrape_nf3_schedule_all_teams.log")

# ディレクトリ作成
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ログ設定（追記モード）
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

TEAMS = {
    # パ・リーグ
    "Fighters":  {"tm": "F",  "leg": 1},
    "Buffaloes": {"tm": "Bs", "leg": 1},
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

MONTHS = list(range(3, 12))
NEEDED_COLS = ["日付", "曜", "対戦T", "球場", "H/V", "開始"]

def build_url(params: dict) -> str:
    return f"{BASE_URL}?{urlencode(params)}"

def read_table(url: str):
    try:
        tables = pd.read_html(url, encoding="cp932")
    except Exception:
        tables = pd.read_html(url)
    if not tables:
        return None
    return tables[0]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
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
    legs_to_try = [leg_preferred] + [l for l in [0, 1, 2] if l != leg_preferred]
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

            if "日付" in df.columns:
                df = df[df["日付"].notna()]
                df = df[~df["日付"].astype(str).str.contains("合計|計|日付", na=False)]

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
    logging.info("=== スクレイピング開始 ===")
    for team_name, meta in TEAMS.items():
        team_code = meta["tm"]
        leg_pref = meta["leg"]
        all_months = []

        for mon in MONTHS:
            logging.info(f"Fetching {team_name} (tm={team_code}) mon={mon} pref_leg={leg_pref}")
            try:
                df = fetch_table_any_leg(team_code, mon, leg_pref)
                all_months.append(df)
                logging.info(f" -> rows={len(df)} leg_used={df['leg_used'].iloc[0]}")
            except Exception as e:
                logging.warning(f"取得失敗: team={team_name} mon={mon} reason={e}")
            time.sleep(1)

    if all_months:
        result = pd.concat(all_months, ignore_index=True)

        # 並べ替え（←ここを修正）
        ordered = [c for c in NEEDED_COLS if c in result.columns]
        extra = [c for c in ["month", "team_code", "leg_used", "source_url"] if c in result.columns]
        result = result[ordered + extra]   # ← 修正ポイント

        out_csv = os.path.join(SAVE_DIR, f"{team_name}.csv")
        result.to_csv(out_csv, index=False, encoding="utf-8-sig")
        logging.info(f"[DONE] {team_name}: {len(result)} rows saved -> {out_csv}")
    else:
        logging.warning(f"データなし: {team_name}")

    logging.info("=== スクレイピング終了 ===")

if __name__ == "__main__":
    main()

