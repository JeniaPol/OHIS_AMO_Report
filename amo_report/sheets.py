from __future__ import annotations

from typing import Any, Dict

import gspread
import pandas as pd


def _get_client_from_creds_dict(creds_dict: Dict[str, Any]) -> gspread.Client:
    try:
        # Prefer native helper if available
        client = gspread.service_account_from_dict(creds_dict)
        return client
    except Exception:
        # Fallback to oauth2client path if environment pins an older flow
        from oauth2client.service_account import ServiceAccountCredentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
        return gspread.authorize(credentials)


def _ensure_worksheet(spreadsheet: gspread.Spreadsheet, title: str, rows: int = 1000, cols: int = 26):
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
        return ws
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def _dataframe_to_values(df: pd.DataFrame) -> list[list]:
    headers = list(df.columns)
    body = df.astype(object).where(pd.notna(df), "").values.tolist()
    return [headers] + body


def export_two_tabs(
    spreadsheet_id: str,
    creds_dict: Dict[str, Any],
    base_name: str,
    report_df: pd.DataFrame,
    contacts_df: pd.DataFrame,
) -> None:
    client = _get_client_from_creds_dict(creds_dict)
    sh = client.open_by_key(spreadsheet_id)

    title_report = f"{base_name} | Отчёт"
    title_contacts = f"{base_name} | Список_Отклик"

    ws_report = _ensure_worksheet(sh, title_report, rows=max(len(report_df) + 10, 100), cols=max(len(report_df.columns) + 2, 10))
    ws_contacts = _ensure_worksheet(sh, title_contacts, rows=max(len(contacts_df) + 10, 100), cols=max(len(contacts_df.columns) + 2, 6))

    ws_report.update(_dataframe_to_values(report_df))
    ws_contacts.update(_dataframe_to_values(contacts_df))


def export_group_result(
    spreadsheet_id: str,
    creds_dict: Dict[str, Any],
    group_name: str,
    report_df: pd.DataFrame,
    contacts_df: pd.DataFrame,
) -> None:
    client = _get_client_from_creds_dict(creds_dict)
    sh = client.open_by_key(spreadsheet_id)
    title_report = f"Group | {group_name} | Отчёт"
    title_contacts = f"Group | {group_name} | Список_Отклик"
    ws_report = _ensure_worksheet(sh, title_report, rows=max(len(report_df) + 10, 100), cols=max(len(report_df.columns) + 2, 10))
    ws_contacts = _ensure_worksheet(sh, title_contacts, rows=max(len(contacts_df) + 10, 100), cols=max(len(contacts_df.columns) + 2, 6))
    ws_report.update(_dataframe_to_values(report_df))
    ws_contacts.update(_dataframe_to_values(contacts_df))


