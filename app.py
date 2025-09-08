import streamlit as st
import pandas as pd
from datetime import date
from amo_report.config import load_config
from amo_report.report import compute_report_by_tags
from amo_report.utils import parse_tags
from amo_report.sheets import export_two_tabs
from amo_report.tags_cache import (
    load_tags_cache,
    save_tags_cache,
    load_tags_cache_gs,
    save_tags_cache_gs,
)
from amo_report.tag_groups import parse_tag_groups_excel, TagGroup

st.set_page_config(page_title="AmoCRM → Отчёт по тегам", layout="wide")
st.title("AmoCRM → Отчёт с разрезом по тегам")

@st.cache_resource
def get_cfg():
    # Try local folder first, then repo root
    try:
        return load_config("OHIS_AMO_Report/config.yaml")
    except FileNotFoundError:
        return load_config("config.yaml")


@st.cache_data(show_spinner=False)
def compute_cached(df_in: pd.DataFrame, cfg: dict, segment: str, funnel: str, mode: str, date_from, date_to, selected_tags: list[str]):
    return compute_report_by_tags(
        df_in=df_in,
        cfg=cfg,
        segment=segment,
        funnel=funnel.lower(),
        mode=mode,
        date_from=date_from,
        date_to=date_to,
        tags=selected_tags,
    )


@st.cache_data(show_spinner=False)
def load_df_cached(file_bytes: bytes, name: str) -> pd.DataFrame:
    import io
    bio = io.BytesIO(file_bytes)
    if name.endswith(".csv"):
        try:
            return pd.read_csv(bio, sep=",", dtype=str, engine="pyarrow")
        except Exception:
            bio.seek(0)
            return pd.read_csv(bio, sep=",", dtype=str)
    else:
        return pd.read_excel(bio, dtype=str)


@st.cache_data(show_spinner=False)
def extract_tag_options_cached(df: pd.DataFrame) -> list[str]:
    if "Теги сделки" not in df.columns:
        return []
    tag_series = df["Теги сделки"].dropna().astype(str).apply(parse_tags)
    tags = sorted({t for sub in tag_series for t in sub})
    return tags

cfg = get_cfg()

segment = st.selectbox("Сегмент", ["RUS", "ENG", "ESP"])
mode_map = {"Брошенная корзина": "basket", "Автосообщение": "auto", "Через менеджера": "manager"}
mode_label = st.selectbox("Режим/функция", list(mode_map.keys()))
mode = mode_map[mode_label]

# Determine funnels based on segment and mode
all_funnels = cfg["funnels"][segment]
if mode == "basket":
    # Restrict to the cart funnel per language
    preferred = {
        "RUS": "Корзина",
        "ENG": "Cart ENG",
        "ESP": "Cart ESP",
    }[segment]
    funnels = [preferred]
else:
    funnels = all_funnels
funnel = st.selectbox("Воронка", funnels)

df_file = st.file_uploader("Выгрузка из Amo (Excel/CSV)", type=["xlsx", "xls", "csv"])
group_file = st.file_uploader("Файл групп тегов (Excel)", type=["xlsx", "xls"], help="Первая колонка: h/Заголовок, теги, ... end/")

date_from = None
date_to = None
if mode == "basket":
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("Дата с", value=date(date.today().year, date.today().month, 1))
    with col2:
        date_to   = st.date_input("Дата по", value=date.today())

selected_tags = []
df = None
tags = []
if df_file:
    file_bytes = df_file.getvalue()
    df = load_df_cached(file_bytes, df_file.name)

    with st.expander("Общий кэш тегов (Google Sheets)", expanded=False):
        col_gs1, col_gs2 = st.columns(2)
        with col_gs1:
            tags_spreadsheet_id = st.text_input("Spreadsheet ID (Tags)", key="tags_spreadsheet_id")
        with col_gs2:
            tags_cache_scope = st.selectbox(
                "Область кэша",
                ["Глобально", "По сегменту", "Сегмент+Воронка", "Свой ключ"],
                help="Как выделять отдельные наборы тегов",
                key="tags_cache_scope",
            )
        custom_key = None
        if tags_cache_scope == "Свой ключ":
            custom_key = st.text_input("Ключ набора тегов", key="tags_custom_key")
        creds_json_tags = st.text_area(
            "Service Account JSON (Tags)",
            help="Опционально: для общего кэша в Google Sheets",
            key="creds_json_tags",
        )

    # Determine cache key
    cache_key = None
    if tags_cache_scope == "По сегменту":
        cache_key = segment
    elif tags_cache_scope == "Сегмент+Воронка":
        cache_key = f"{segment} | {funnel}"
    elif tags_cache_scope == "Свой ключ":
        cache_key = (custom_key or "").strip() or None

    # Try Google Sheets-backed cache first if configured, else local JSON
    tags = []
    used_source = None
    if tags_spreadsheet_id and creds_json_tags:
        try:
            import json
            creds_dict_tags = json.loads(creds_json_tags)
            cached_tags, meta = load_tags_cache_gs(tags_spreadsheet_id, creds_dict_tags, key=cache_key)
            if cached_tags:
                tags = cached_tags
                used_source = f"GS (обновлено: {meta.get('updated_at', '—')})"
        except Exception as _ex:
            pass
    if not tags:
        cached_tags, meta = load_tags_cache()
        if cached_tags:
            tags = cached_tags
            used_source = f"Локально (обновлено: {meta.get('updated_at', '—')})"
    if not tags:
        tags = extract_tag_options_cached(df)
        used_source = "Из файла"
    if used_source:
        st.caption(f"Источник тегов: {used_source}")

col_tags, col_btn = st.columns([4, 1])
with col_tags:
    selected_tags = st.multiselect(
        "Выберите теги (строка = тег)",
        options=tags,
        help="Начните вводить тег, чтобы отфильтровать список. Если не выберете — будут все теги из файла."
    )
with col_btn:
    st.write("")
    st.write("")
    if df_file and st.button("Обновить теги"):
        fresh_tags = extract_tag_options_cached(df)
        # Save to Google Sheets if configured; else local JSON
        saved_ok = False
        if tags_spreadsheet_id and creds_json_tags:
            try:
                import json
                creds_dict_tags = json.loads(creds_json_tags)
                save_tags_cache_gs(fresh_tags, tags_spreadsheet_id, creds_dict_tags, key=cache_key)
                saved_ok = True
                st.success("Теги обновлены в общем кэше (Google Sheets).")
            except Exception as ex:
                st.warning(f"Не удалось сохранить в Google Sheets: {ex}")
        if not saved_ok:
            save_tags_cache(fresh_tags)
            st.success("Теги обновлены в локальном кэше.")
        tags = fresh_tags

export_area = st.empty()

report_res = None
if df_file and st.button("Сформировать отчёт"):
    try:
        res = compute_cached(df, cfg, segment, funnel, mode, date_from, date_to, selected_tags)
        report_res = res

        st.subheader(f"{res['header']['Название']} — {res['header']['Период']}")
        if res['header']['Отданы в ОП']:
            st.caption(f"Отданы в ОП: {res['header']['Отданы в ОП']} • Дней от начала: {res['header']['Дней от начала']}")

        if res["table_df"].empty:
            st.warning("По выбранным условиям данных не найдено.")
        else:
            st.markdown("### Итоговая таблица (по тегам)")
            st.dataframe(res["table_df"], use_container_width=True)

        st.markdown("### Списки контактов с откликом (по тегам)")
        if not res["reply_contacts_by_tag"]:
            st.write("— нет контактов с откликом —")
        else:
            for tag, contacts in res["reply_contacts_by_tag"].items():
                st.markdown(f"**{tag}**")
                # contacts can be list of dicts with optional ID
                if contacts and isinstance(contacts[0], dict):
                    df_contacts = pd.DataFrame(contacts)
                else:
                    df_contacts = pd.DataFrame({"Основной контакт": contacts})
                st.write(df_contacts)
    except Exception as e:
        st.error(str(e))

if df_file and group_file and st.button("Сформировать отчёты по группам"):
    try:
        groups = parse_tag_groups_excel(group_file.getvalue())
        if not groups:
            st.warning("Группы не найдены в файле.")
        else:
            for tg in groups:
                # Preserve order from file; append additional selected tags keeping their order
                union_tags = tg.tags + [t for t in (selected_tags or []) if t not in tg.tags]
                # Build description map for the group's tags (normalized)
                desc_map = {t.strip().lower(): (tg.desc_by_norm.get(t.strip().lower(), "") if hasattr(tg, 'desc_by_norm') else "") for t in tg.tags}
                # Compute with description mapping (cache key unaffected, safe to pass at runtime)
                res = compute_report_by_tags(
                    df_in=df,
                    cfg=cfg,
                    segment=segment,
                    funnel=funnel.lower(),
                    mode=mode,
                    date_from=date_from,
                    date_to=date_to,
                    tags=union_tags,
                    tag_desc_by_norm=desc_map,
                )
                st.subheader(f"Группа: {tg.name}")
                if res["table_df"].empty:
                    st.write("— нет данных —")
                else:
                    st.dataframe(res["table_df"], use_container_width=True)
                st.markdown("Списки контактов с откликом")
                if not res["reply_contacts_by_tag"]:
                    st.write("— нет контактов —")
                else:
                    for tag, contacts in res["reply_contacts_by_tag"].items():
                        st.markdown(f"**{tag}**")
                        if contacts and isinstance(contacts[0], dict):
                            df_contacts = pd.DataFrame(contacts)
                        else:
                            df_contacts = pd.DataFrame({"Основной контакт": contacts})
                        st.write(df_contacts)
    except Exception as ex:
        st.error(f"Ошибка обработки групп: {ex}")

st.divider()
st.caption("Примечание: режимы 'Автосообщение' и 'Через менеджера' не используют фильтр по датам; 'Брошенная корзина' использует.")

# Export to Google Sheets
if report_res is not None:
    st.markdown("### Экспорт в Google Sheets")
    with st.expander("Настройки экспорта", expanded=False):
        spreadsheet_id = st.text_input("Spreadsheet ID")
        creds_json = st.text_area("Service Account JSON", help="Вставьте содержимое JSON ключа сервисного аккаунта")
        base_name = st.text_input("Имя набора листов", value=f"{segment} | {funnel} | {date_from or ''}..{date_to or ''}")
        can_export = bool(spreadsheet_id and creds_json)
    if can_export and st.button("Обновить Google Sheets"):
        try:
            import json

            creds_dict = json.loads(creds_json)
            report_df = report_res["table_df"]
            # Prepare contacts df (include ID if present)
            contacts_rows = []
            for tag, contacts in report_res["reply_contacts_by_tag"].items():
                for c in contacts:
                    if isinstance(c, dict):
                        row = {"Тег": tag, **c}
                    else:
                        row = {"Тег": tag, "Основной контакт": c}
                    contacts_rows.append(row)
            contacts_df = pd.DataFrame(contacts_rows)

            export_two_tabs(
                spreadsheet_id=spreadsheet_id,
                creds_dict=creds_dict,
                base_name=base_name,
                report_df=report_df,
                contacts_df=contacts_df,
            )
            st.success("Экспорт завершён.")
        except Exception as ex:
            st.error(f"Ошибка экспорта: {ex}")
