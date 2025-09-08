from __future__ import annotations

from datetime import date
from typing import List

import pandas as pd

from .utils import (
    normalize_series,
    only_date,
    last_wednesday_on_or_before,
    mask_stage_in,
    sum_budget,
    explode_by_tags,
    budget_to_float,
    collect_unique_norm_tags,
    mask_no_wazzap,
)


REQUIRED_COLS = [
    "Этап сделки",
    "Воронка",
    "Теги сделки",
    "Бюджет",
    "Дата создания",
    "Основной контакт",
]


def _pick(cfg: dict, segment: str, key: str) -> List[str]:
    node = cfg["stages"][key]
    if "ALL" in node:
        return node["ALL"]
    return node.get(segment, [])


def _calc_block(df: pd.DataFrame, cfg: dict, segment: str, mode: str) -> dict:
    total_in_period = len(df)

    already_bought = _pick(cfg, segment, "already_bought")
    closed_not_impl = _pick(cfg, segment, "closed_not_impl")
    lead_not_distributed = _pick(cfg, segment, "lead_not_distributed")
    # Contact group may differ by mode; use contact_group_auto for auto if provided
    if mode == "auto" and "contact_group_auto" in cfg.get("stages", {}):
        contact_group = _pick(cfg, segment, "contact_group_auto")
    else:
        contact_group = _pick(cfg, segment, "contact_group")
    reply_group = _pick(cfg, segment, "reply_group")
    # Revenue counts only stages that imply payment (e.g., prepayment or fully implemented)
    revenue_group = _pick(cfg, segment, "revenue_group") if "revenue_group" in cfg.get("stages", {}) else []
    no_wazzap = _pick(cfg, segment, "no_wazzap")

    m_already = mask_stage_in(df, already_bought)
    m_closed = mask_stage_in(df, closed_not_impl)
    m_lead_nd = mask_stage_in(df, lead_not_distributed)
    m_contact = mask_stage_in(df, contact_group)
    m_reply = mask_stage_in(df, reply_group)
    m_nowz = mask_no_wazzap(df, no_wazzap)

    if mode == "basket":
        cnt_wo_already = total_in_period - int(m_closed.sum()) - int(m_already.sum())
        processed = cnt_wo_already - int(m_lead_nd.sum())
        base_denom = max(cnt_wo_already, 1)
    elif mode == "auto":
        cnt = total_in_period - int(m_nowz.sum())
        processed = cnt
        cnt_wo_already = cnt
        base_denom = max(cnt, 1)
    elif mode == "manager":
        cnt = total_in_period - int(m_closed.sum())
        processed = cnt - int(m_lead_nd.sum())
        cnt_wo_already = cnt
        base_denom = max(cnt, 1)
    else:
        raise ValueError("mode должен быть 'basket' | 'auto' | 'manager'")

    contact = int(m_contact.sum())
    ignore = max(processed - contact, 0)
    reply = int(m_reply.sum())
    if revenue_group:
        m_revenue = mask_stage_in(df, revenue_group)
        budget = sum_budget(df[m_revenue])
    else:
        # Fallback: include common payment-like stages
        m_revenue = mask_stage_in(df, [
            "аванс",
            "успешно реализовано",
            "prepayment",
            "successfully and implemented",
        ])
        budget = sum_budget(df[m_revenue])

    def pct(a, b):
        return round((a / b) * 100, 2) if b > 0 else 0.0

    tbl = {
        "Кол-во": cnt_wo_already,
        "Обработано": processed,
        "% обработано": pct(processed, base_denom),
        "Контакт": contact,
        "% контакт": pct(contact, processed),
        "Игнор": ignore,
        "% игнор": pct(ignore, processed),
        "Отклик (Покупка)": reply,
        "Оборот, €": round(budget, 2),
        "CR, %": pct(reply, contact),
        "Конверсия в покупку, %": pct(reply, processed),
    }
    return tbl


def compute_report_by_tags(
    df_in: pd.DataFrame,
    cfg: dict,
    segment: str,
    funnel: str,
    mode: str,  # "basket" | "auto" | "manager"
    date_from: date | None,
    date_to: date | None,
    tags: list[str],  # list of tags to include (display order)
    tag_desc_by_norm: dict[str, str] | None = None,  # optional: excel group descriptions
) -> dict:
    df = df_in.copy()
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Не найдены колонки: {missing}")

    # Precompute normalized columns once for performance
    df["__stage"] = pd.Categorical(normalize_series(df["Этап сделки"]))  # used in masks
    df["__funnel"] = pd.Categorical(normalize_series(df["Воронка"]))  # used for filtering
    df["__date"] = only_date(df["Дата создания"])  # used for date filter
    df["__budget_float"] = budget_to_float(df["Бюджет"])  # used for sum_budget

    df = df[df["__funnel"] == funnel.strip().lower()]
    if mode == "basket" and date_from is not None and date_to is not None:
        df = df[(df["__date"] >= date_from) & (df["__date"] <= date_to)]

    # Build header early so special cases can return
    if mode == "basket" and date_from and date_to:
        op_date = last_wednesday_on_or_before(date_to)
        from datetime import date as _date
        header = {
            "Название": "Брошенная корзина",
            "Период": f"с {date_from.strftime('%d %B')} по {date_to.strftime('%d %B')}",
            "Отданы в ОП": op_date.strftime("%d.%b").replace(".", "."),
            "Дней от начала": (_date.today() - op_date).days,
        }
    else:
        header = {
            "Название": "Автосообщение" if mode == "auto" else "Через менеджера",
            "Период": "по выбранным тегам (без фильтра по дате)",
            "Отданы в ОП": "",
            "Дней от начала": "",
        }

    chosen = [t for t in tags if str(t).strip()]
    chosen_norm = [str(t).strip().lower() for t in chosen]
    auto_tags_order = False
    if not chosen_norm:
        chosen_norm = collect_unique_norm_tags(df)
        auto_tags_order = True

    include_set = set(chosen_norm) if chosen_norm else None
    # Special case: for basket with no tags selected, compute overall aggregate without tag slicing
    if mode == "basket" and not chosen_norm:
        metrics = _calc_block(df, cfg, segment, mode)
        table_df = pd.DataFrame([{"Тег сделки": "Все сделки", **metrics}])
        # Format percentage columns consistently
        percent_cols = ["% обработано", "% контакт", "% игнор", "CR, %", "Конверсия в покупку, %"]
        for col in percent_cols:
            if col in table_df.columns:
                def _fmt_pct(v):
                    if pd.isna(v):
                        return ""
                    try:
                        s = f"{float(v):.0f}"
                        return f"{s}%"
                    except Exception:
                        return f"{v}%"
                table_df[col] = table_df[col].apply(_fmt_pct)

        # Prepare reply contacts aggregated, include ID if available
        reply_group = _pick(cfg, segment, "reply_group")
        if "__stage" in df.columns:
            reply_mask_all = df["__stage"].isin([x.lower() for x in reply_group])
        else:
            reply_mask_all = normalize_series(df["Этап сделки"]).isin([x.lower() for x in reply_group])
        cols = ["Основной контакт"] + (["ID"] if "ID" in df.columns else [])
        cont_df = (
            df.loc[reply_mask_all, cols]
            .dropna(subset=["Основной контакт"])  # require contact
            .assign(**{"Основной контакт": lambda x: x["Основной контакт"].astype(str).str.strip()})
        )
        cont_df = cont_df.replace({"Основной контакт": {"": pd.NA}}).dropna(subset=["Основной контакт"]).drop_duplicates()
        reply_contacts_by_tag = {"Все сделки": cont_df.to_dict("records")}
        return {"header": header, "table_df": table_df, "reply_contacts_by_tag": reply_contacts_by_tag}

    dfe = explode_by_tags(df, include_norm_tags=include_set)

    rows = []
    for tag_norm in chosen_norm:
        sub = dfe[dfe["__tag_norm"] == tag_norm]
        tag_display = sub["__tag_display"].iloc[0] if not sub.empty else tag_norm
        if tag_desc_by_norm:
            desc = tag_desc_by_norm.get(tag_norm, "")
        else:
            desc = ""
        metrics = _calc_block(sub, cfg, segment, mode)
        row = {"Тег сделки": tag_display, "Описание": desc, **metrics}
        rows.append(row)

    # header already built above

    table_df = pd.DataFrame(rows)
    # Keep explicit tag order (from file/selection). Only sort when tags were auto-detected.
    if auto_tags_order and not table_df.empty and "Кол-во" in table_df.columns:
        table_df = table_df.sort_values("Кол-во", ascending=False).reset_index(drop=True)

    # Format percentage columns: round to 2 decimals, trim trailing zeros, add '%'
    percent_cols = ["% обработано", "% контакт", "% игнор", "CR, %", "Конверсия в покупку, %"]
    for col in percent_cols:
        if col in table_df.columns:
            def _fmt_pct(v):
                if pd.isna(v):
                    return ""
                try:
                    s = f"{float(v):.0f}"
                    return f"{s}%"
                except Exception:
                    return f"{v}%"
            table_df[col] = table_df[col].apply(_fmt_pct)

    reply_group = _pick(cfg, segment, "reply_group")
    # use precomputed normalized stage when available
    if "__stage" in dfe.columns:
        reply_mask = dfe["__stage"].isin([x.lower() for x in reply_group])
    else:
        reply_mask = normalize_series(dfe["Этап сделки"]).isin([x.lower() for x in reply_group])
    reply_contacts_by_tag: dict[str, list[dict]] = {}
    for tag_norm in chosen_norm:
        sub = dfe[(dfe["__tag_norm"] == tag_norm) & reply_mask]
        cols = ["Основной контакт"] + (["ID"] if "ID" in sub.columns else [])
        cont_df = (
            sub[cols]
            .dropna(subset=["Основной контакт"])  # require contact
            .assign(**{"Основной контакт": lambda x: x["Основной контакт"].astype(str).str.strip()})
        )
        cont_df = cont_df.replace({"Основной контакт": {"": pd.NA}}).dropna(subset=["Основной контакт"]).drop_duplicates()
        disp = sub["__tag_display"].iloc[0] if not sub.empty else tag_norm
        reply_contacts_by_tag[disp] = cont_df.to_dict("records")

    return {"header": header, "table_df": table_df, "reply_contacts_by_tag": reply_contacts_by_tag}


