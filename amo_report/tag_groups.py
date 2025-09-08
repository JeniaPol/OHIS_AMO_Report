from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict
import io
import pandas as pd


@dataclass
class TagGroup:
    name: str
    tags: List[str]
    desc_by_norm: Dict[str, str]


def _norm_tag(s: str) -> str:
    return str(s).strip().lower().replace("ё", "е")


def parse_tag_groups_excel(file_bytes: bytes) -> List[TagGroup]:
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, dtype=str, header=None)
    if df.empty:
        return []
    col = df.iloc[:, 0].astype(str)
    col2 = df.iloc[:, 1].astype(str) if df.shape[1] > 1 else pd.Series([None] * len(df))

    groups: List[TagGroup] = []
    current_name: str | None = None
    current_tags: List[str] = []
    current_desc: Dict[str, str] = {}

    def _flush():
        nonlocal current_name, current_tags, current_desc
        if current_name and current_tags:
            groups.append(TagGroup(current_name, current_tags, current_desc))
        current_name = None
        current_tags = []
        current_desc = {}

    for i, raw in enumerate(col):
        s = (raw or "").strip()
        if not s:
            continue
        if s.lower().startswith("h/"):
            # start new group, flush previous
            _flush()
            current_name = s[2:].strip() or "Группа"
            continue
        if s.lower().startswith("end/"):
            _flush()
            continue
        # normal tag
        current_tags.append(s)
        d = (col2.iloc[i] or "").strip() if len(col2) > i else ""
        if d:
            current_desc[_norm_tag(s)] = d

    _flush()
    return groups


