# helper/cleaning.py

import pandas as pd
import re
import logging


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip()
    return df


def clean_no_body_var(value: str) -> str:
    if not value or pd.isna(value):
        return None
    letters = "".join(re.findall(r"[A-Za-z]", str(value)))[:3].upper()
    numbers = "".join(re.findall(r"\d", str(value)))[:3].rjust(3, "0")
    return f"{letters}-{numbers}"


def convert_status_var(value: str) -> str:
    if not value or pd.isna(value):
        return None
    v = str(value).strip().upper()
    return "pelanggan" if v == "S" else "nonpelanggan"


def clean_datetime_columns(df: pd.DataFrame, cols=None) -> pd.DataFrame:
    df = df.copy()
    if cols:
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def dedupe_dataframe(df: pd.DataFrame, exclude_cols=None):
    df = df.copy()
    exclude_cols = exclude_cols or []
    dedupe_cols = [c for c in df.columns if c not in exclude_cols]

    before = len(df)
    df = df.drop_duplicates(subset=dedupe_cols, keep="last")
    after = len(df)

    logging.info(f"Deduped: {before - after} duplicates removed")

    return df
