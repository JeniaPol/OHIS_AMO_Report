from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Dict, Any
import json
from datetime import datetime
from .sheets import _get_client_from_creds_dict


def _resolve_cache_path(preferred: str = "AMO_CRM_Report/tags_cache.json", fallback: str = "tags_cache.json") -> Path:
    p1 = Path(preferred)
    if p1.parent.exists():
        return p1
    return Path(fallback)


def load_tags_cache(path: str | Path | None = None) -> Tuple[List[str], Dict[str, Any]]:
    cache_path = _resolve_cache_path() if path is None else Path(path)
    if not cache_path.exists():
        return [], {"updated_at": None}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        tags = data.get("tags", [])
        meta = {k: v for k, v in data.items() if k != "tags"}
        return tags, meta
    except Exception:
        return [], {"updated_at": None}


def save_tags_cache(tags: List[str], path: str | Path | None = None) -> Path:
    cache_path = _resolve_cache_path() if path is None else Path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "tags": sorted(list({t for t in tags if str(t).strip()})),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return cache_path


def _tags_ws_title(key: str | None) -> str:
    base = "TagsCache | Global" if not key else f"TagsCache | {key}"
    return base


def load_tags_cache_gs(spreadsheet_id: str, creds_dict: Dict[str, Any], key: str | None = None) -> Tuple[List[str], Dict[str, Any]]:
    try:
        client = _get_client_from_creds_dict(creds_dict)
        sh = client.open_by_key(spreadsheet_id)
        title = _tags_ws_title(key)
        try:
            ws = sh.worksheet(title)
        except Exception:
            return [], {"updated_at": None}
        values = ws.get_all_values()
        if not values:
            return [], {"updated_at": None}
        # Expect header in first row: ["tag", "updated_at"]
        headers = values[0]
        updated_at = None
        if len(headers) > 1:
            updated_at = headers[1]
        tags = [row[0] for row in values[1:] if row and row[0]]
        return tags, {"updated_at": updated_at}
    except Exception:
        return [], {"updated_at": None}


def save_tags_cache_gs(tags: List[str], spreadsheet_id: str, creds_dict: Dict[str, Any], key: str | None = None) -> None:
    client = _get_client_from_creds_dict(creds_dict)
    sh = client.open_by_key(spreadsheet_id)
    title = _tags_ws_title(key)
    try:
        ws = sh.worksheet(title)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=title, rows="1000", cols="3")
    unique_sorted = sorted(list({t for t in tags if str(t).strip()}))
    updated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    values = [["tag", updated_at]] + [[t] for t in unique_sorted]
    ws.update(values)


