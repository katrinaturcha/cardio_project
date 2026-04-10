import glob
import hashlib
import os
import re
import time
from pathlib import Path
import pandas as pd


def wait_for_downloads(download_dir: str | Path, timeout: int = 300) -> bool:
    start = time.time()
    download_dir = str(download_dir)
    while True:
        temp_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
        if not temp_files:
            return True
        if time.time() - start > timeout:
            raise TimeoutError("Не удалось дождаться завершения скачивания файлов")
        time.sleep(2)


def read_csv_with_fallback(file_path: str | Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "cp1251", "utf-8"]
    for enc in encodings:
        try:
            return pd.read_csv(file_path, sep=";", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(file_path)


def clean_text(value):
    if pd.isna(value):
        return None
    value = str(value)
    value = value.replace('"', '').replace("'", '').replace('«', '').replace('»', '')
    value = value.replace(" ", " ")
    value = re.sub(r"\s+", " ", value).strip()
    if value == "" or value.lower() == "nan":
        return None
    return value


def clean_float(value):
    if pd.isna(value):
        return None
    value = clean_text(value)
    if value is None:
        return None
    value = value.replace(" ", "").replace(",", ".")
    value = re.sub(r"[^0-9.\-]", "", value)
    if value in {"", ".", "-", "nan"}:
        return None
    try:
        return float(value)
    except Exception:
        return None


def clean_int(value):
    num = clean_float(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


import hashlib
import pandas as pd


def add_hash_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    hash_source = df.apply(
        lambda row: "||".join("" if pd.isna(value) else str(value) for value in row.tolist()),
        axis=1,
    )

    df["hash_key"] = hash_source.apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )

    return df
