import time
import pandas as pd
from urllib.parse import urlencode
import os

BASE_URL = "https://nf3.sakura.ne.jp/php/stat_disp/stat_disp.php"

# 保存先ディレクトリ
SAVE_DIR = "/home/ec2-user/batch/data/matches"

# ディレクトリが存在しない場合は作成
os.makedirs(SAVE_DIR, exist_ok=True)

# 取得対象チーム（必要に応じて追加）
TEAMS = {
    "Fighters": "F",
    "Buffaloes": "B",
    "Lions": "L",
    "Hawks": "H",
    "Marines": "M",
    "Eagles": "E",
}

# 月
MONTHS = [8]  # 複数月なら [7,8,9] や list(range(3,11)) に変更

# 欲しいカラム
NEEDED_COLS = ["日付", "曜", "対戦T", "球場", "H/V", "開始"]

def build_url(params: dict) -> str:
    return f"{BASE_URL}?{urlencode(params)}"

def fetch_table(team_code: str, mon: int) -> pd.DataFrame:
    params = {
        "y": 0,  # 当年
        "leg": 1,
        "tm": team_code,
        "mon": mon,
        "vst": "all",
    }
    url = build_url(params)

    try:
        tables = pd.read_html(url, encoding="cp932")
    except Exception:
        tables = pd.read_html(url)

    if not tables:
        raise ValueError(f"表が見つかりません: {url}")

    df = tables[0]

    # MultiIndex対応
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            lower = (col[1] if len(col) > 1 else "")
            upper = col[0]
            name = str(lower).strip() if str(lower).strip() else str(upper).strip()
            new_cols.append(name)
        df.columns = new_cols
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # 欲しい列だけ
    exist = [c for c in NEEDED_COLS if c in df.columns]
    df = df[exist].copy()

    # 日付がNaNや見出し行っぽい行を除外
    if "日付" in df.columns:
        df = df[df["日付"].notna()]
        df = df[~df["日付"].astype(str).str.contains("合計|計|日付", na=False)]

    # メタ情報追加
    df["month"] = mon
    df["team_code"] = team_code
    df["source_url"] = url

    return df.reset_index(drop=True)

def main():
    for team_name, team_code in TEAMS.items():
        all_months = []
        for mon in MONTHS:
            print(f"[INFO] Fetching {team_name} month={mon}")
            try:
                df = fetch_table(team_code, mon)
                all_months.append(df)
            except Exception as e:
                print(f"[WARN] 取得失敗: {e}")
            time.sleep(1)  # アクセス間隔

        if all_months:
            result = pd.concat(all_months, ignore_index=True)

            # ファイルパス
            out_csv = os.path.join(SAVE_DIR, f"{team_name}.csv")
            result.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"[DONE] {len(result)} rows saved -> {out_csv}")
        else:
            print(f"[WARN] データなし: {team_name}")

if __name__ == "__main__":
    main()

