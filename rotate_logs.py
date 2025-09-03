#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# ===== 設定 =====
LOG_DIR = Path("/home/ec2-user/batch/logs")

# 基本ポリシー
UNCOMPRESSED_KEEP_DAYS = 2     # 2日より古い未圧縮ログは圧縮
COMPRESSED_KEEP_DAYS   = 45    # 45日より古い圧縮済みログは削除
ACTIVE_GRACE_MINUTES   = 10    # 直近10分内に更新のファイルは触らない
MAX_TOTAL_MB           = 500   # ログ全体の上限MB（超えたら古い圧縮ログから削除）
# ===== ここまで設定 =====

def human(n):
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:3.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

def iter_log_files(root: Path):
    # 隠しファイルを除く、ファイルのみ
    for p in root.iterdir():
        if p.is_file() and not p.name.startswith("."):
            yield p

def is_recently_modified(path: Path, minutes: int) -> bool:
    mtime = path.stat().st_mtime
    return (time.time() - mtime) < (minutes * 60)

def compress_file(path: Path):
    gz_path = path.with_suffix(path.suffix + ".gz") if path.suffix != ".gz" else path
    if gz_path.suffix == ".gz":
        # すでに .gz は圧縮不要
        return False
    try:
        with open(path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        orig_size = path.stat().st_size
        os.remove(path)
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] COMPRESS  {path.name} -> {gz_path.name} ({human(orig_size)})")
        return True
    except Exception as e:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] WARN      圧縮失敗: {path.name} ({e})")
        return False

def delete_file(path: Path, reason: str):
    try:
        size = path.stat().st_size
        os.remove(path)
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] DELETE    {path.name} ({human(size)}) reason={reason}")
        return size
    except Exception as e:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] WARN      削除失敗: {path.name} ({e})")
        return 0

def total_size_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in iter_log_files(root))

def main():
    if not LOG_DIR.exists():
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] INFO      作成: {LOG_DIR}")
        LOG_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    compressed_cutoff  = now - timedelta(days=COMPRESSED_KEEP_DAYS)
    uncompressed_cutoff = now - timedelta(days=UNCOMPRESSED_KEEP_DAYS)

    # 1) 未圧縮の古いログを圧縮
    for p in iter_log_files(LOG_DIR):
        if p.suffix == ".gz":
            continue
        if is_recently_modified(p, ACTIVE_GRACE_MINUTES):
            continue
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
        except FileNotFoundError:
            continue
        if mtime < uncompressed_cutoff:
            compress_file(p)

    # 2) 圧縮済みで古すぎるログ（期限切れ）を削除
    for p in iter_log_files(LOG_DIR):
        if p.suffix != ".gz":
            continue
        if is_recently_modified(p, ACTIVE_GRACE_MINUTES):
            continue
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
        except FileNotFoundError:
            continue
        if mtime < compressed_cutoff:
            delete_file(p, reason=f"older_than_{COMPRESSED_KEEP_DAYS}d")

    # 3) 容量制限（MAX_TOTAL_MB）を超える場合、古い圧縮ログから削除
    max_total_bytes = MAX_TOTAL_MB * 1024 * 1024
    cur_total = total_size_bytes(LOG_DIR)
    if cur_total > max_total_bytes:
        # 古い順（圧縮済みを優先的に削除、足りなければ未圧縮も）
        files = sorted(iter_log_files(LOG_DIR), key=lambda x: x.stat().st_mtime)
        # 圧縮ファイルを先に、未圧縮を後に並べ替え
        files.sort(key=lambda x: (0 if x.suffix == ".gz" else 1, x.stat().st_mtime))
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] INFO      容量超過 {human(cur_total)} > {MAX_TOTAL_MB}MB。古いものから削除します。")
        for p in files:
            if cur_total <= max_total_bytes:
                break
            if is_recently_modified(p, ACTIVE_GRACE_MINUTES):
                continue
            freed = delete_file(p, reason="disk_cap")
            cur_total -= freed

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] DONE      現在の総容量: {human(total_size_bytes(LOG_DIR))}")

if __name__ == "__main__":
    main()

