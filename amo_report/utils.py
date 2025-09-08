from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional, Set
import re

import numpy as np
import pandas as pd


def normalize_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("ё", "е", regex=False)
    )


def only_date(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return dt.dt.date


def last_wednesday_on_or_before(d: date) -> date:
    return d - timedelta(days=(d.weekday() - 2) % 7)


def mask_stage_in(df: pd.DataFrame, stage_list: List[str]) -> pd.Series:
    lowered = [x.lower() for x in stage_list]
    if "__stage" in df.columns:
        return df["__stage"].isin(lowered)
    return normalize_series(df["Этап сделки"]).isin(lowered)


def budget_to_float(s: pd.Series) -> pd.Series:
    x = (
        s.astype(str)
        .replace({",": "."}, regex=True)
        .replace(r"[^\d\.\-]", "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )
    return x


def sum_budget(df: pd.DataFrame) -> float:
    if "__budget_float" in df.columns:
        return float(np.nansum(df["__budget_float"]))
    return float(np.nansum(budget_to_float(df["Бюджет"])))


def mask_no_wazzap(df: pd.DataFrame, stage_list: List[str]) -> pd.Series:
    """Return mask for 'no wazzap/whatsapp' stages using exact list OR common patterns.

    - Uses precomputed __stage when available
    - Adds pattern-based fallback to catch variants like 'no whatsapp'
    """
    base = mask_stage_in(df, stage_list)
    stage_norm = df["__stage"] if "__stage" in df.columns else normalize_series(df["Этап сделки"]) 
    # Avoid capture groups to prevent pandas UserWarning
    pattern = r"wazzap|wazzup|whats\s*app"
    contains = stage_norm.str.contains(pattern, na=False, regex=True)
    return base | contains


def parse_tags(s: str) -> list[str]:
    if pd.isna(s) or str(s).strip() == "":
        return []
    s = str(s)
    for sep in [";", "|", "/", "\\"]:
        s = s.replace(sep, ",")
    tags = [t.strip() for t in s.split(",") if t.strip()]
    seen = set()
    out: list[str] = []
    for t in tags:
        key = t.lower()
        if key not in seen:
            seen.add(key)
            out.append(t)
    return out


def explode_by_tags(df: pd.DataFrame, include_norm_tags: Optional[Set[str]] = None) -> pd.DataFrame:
    include = {t.strip().lower() for t in include_norm_tags} if include_norm_tags else None
    def _filter_tags(raw: str) -> list[str]:
        lst = parse_tags(raw)
        if include is None:
            return lst
        out: list[str] = []
        for t in lst:
            if t.strip().lower() in include:
                out.append(t)
        return out

    tags_series = df["Теги сделки"].apply(_filter_tags)
    df = df.copy()
    df["__tags_list"] = tags_series
    exploded = df.explode("__tags_list", ignore_index=True)
    # Fast normalization using vectorized ops
    exploded["__tag_display"] = exploded["__tags_list"].fillna("")
    exploded["__tag_norm"] = exploded["__tag_display"].astype(str).str.strip().str.lower()
    return exploded[exploded["__tag_norm"] != ""]


def collect_unique_norm_tags(df: pd.DataFrame) -> list[str]:
    seen: Set[str] = set()
    order: list[str] = []
    for raw in df["Теги сделки"].dropna().astype(str):
        for t in parse_tags(raw):
            key = t.strip().lower()
            if key and key not in seen:
                seen.add(key)
                order.append(key)
    return order


