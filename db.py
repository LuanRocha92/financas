# db.py (Google Sheets como "banco")
from __future__ import annotations

from datetime import datetime
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

# =========================
# CONFIG
# =========================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",  # permite ler/criar abas e operar melhor
]

SHEET_ID = None

# Nomes das "tabelas" (abas)
TAB_TRANSACTIONS = "transactions"
TAB_ADJUSTMENTS = "cashflow_adjustments"
TAB_DEBTS = "debts"
TAB_NOTES = "notes"

TAB_SAVINGS_GOAL = "savings_goal_v2"
TAB_SAVINGS_DEPOSITS = "savings_deposits_v2"
TAB_SAVINGS_OVERRIDES = "savings_overrides_v2"
TAB_SAVINGS_TX_LINK = "savings_tx_link_v2"


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _get_spreadsheet_id() -> str:
    # Streamlit Cloud: usar secrets
    try:
        sid = st.secrets.get("GSHEETS_SPREADSHEET_ID", "")
    except Exception:
        sid = ""

    sid = str(sid).strip()
    if not sid:
        raise RuntimeError(
            "GSHEETS_SPREADSHEET_ID não encontrado nos secrets. "
            "Crie uma planilha no Google Sheets, pegue o ID do link e coloque em Secrets."
        )
    return sid


def _get_client():
    import json
    import gspread
    import streamlit as st
    from google.oauth2.service_account import Credentials

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    raw = str(st.secrets["GCP_SERVICE_ACCOUNT_JSON"]).strip()
    sa_info = json.loads(raw)

    # garante quebra de linha certa
    sa_info["private_key"] = (
        sa_info["private_key"]
        .replace("\\n", "\n")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)
def _open_spreadsheet(client: gspread.Client):
    sid = _get_spreadsheet_id()
    return client.open_by_key(sid)


def ping_db() -> tuple[bool, str]:
    try:
        client = _get_client()
        sh = _open_spreadsheet(client)
        _ = sh.title  # força leitura
        return True, "ok"
    except Exception as e:
        return False, str(e)


def _ensure_worksheet(sh, title: str, headers: list[str]):
    """
    Garante que a aba exista e tenha cabeçalho.
    """
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(headers) + 2))

    values = ws.get_all_values()
    if not values:
        ws.append_row(headers)
        return ws

    first_row = values[0]
    if [c.strip() for c in first_row] != headers:
        # Se existe mas cabeçalho não bate, a gente reescreve o cabeçalho (mantendo dados)
        # (pra uso pessoal, isso é ok)
        ws.update("A1", [headers])

    return ws


def _ws_to_df(ws) -> pd.DataFrame:
    """
    Lê aba inteira como DataFrame.
    """
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0, dtype=str)
    df = df.dropna(how="all")  # remove linhas totalmente vazias
    # normaliza nomes de coluna
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _append_row(ws, row: dict, headers: list[str]):
    """
    Appenda seguindo a ordem do cabeçalho.
    """
    values = [row.get(h, "") for h in headers]
    ws.append_row(values, value_input_option="USER_ENTERED")


def _next_id(df: pd.DataFrame) -> int:
    if df is None or df.empty or "id" not in df.columns:
        return 1
    s = pd.to_numeric(df["id"], errors="coerce").dropna()
    return int(s.max()) + 1 if not s.empty else 1


# =========================
# INIT "DB" (cria abas/headers)
# =========================
def init_db():
    client = _get_client()
    sh = _open_spreadsheet(client)

    # Transactions
    _ensure_worksheet(
        sh,
        TAB_TRANSACTIONS,
        headers=["id", "date", "description", "type", "amount", "category", "paid", "created_at"],
    )

    # Cashflow adjustments
    _ensure_worksheet(
        sh,
        TAB_ADJUSTMENTS,
        headers=["id", "data", "valor", "descricao", "created_at"],
    )

    # Debts
    _ensure_worksheet(
        sh,
        TAB_DEBTS,
        headers=["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"],
    )

    # Notes
    _ensure_worksheet(
        sh,
        TAB_NOTES,
        headers=["id", "titulo", "texto", "created_at", "updated_at"],
    )

    # Savings / desafio v2
    _ensure_worksheet(
        sh,
        TAB_SAVINGS_GOAL,
        headers=["id", "target_amount", "due_date", "n_deposits"],
    )
    _ensure_worksheet(
        sh,
        TAB_SAVINGS_DEPOSITS,
        headers=["n", "done"],
    )
    _ensure_worksheet(
        sh,
        TAB_SAVINGS_OVERRIDES,
        headers=["n", "amount"],
    )
    _ensure_worksheet(
        sh,
        TAB_SAVINGS_TX_LINK,
        headers=["n", "tx_id"],
    )

    # garante que exista linha id=1 no goal
    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    df_goal = _ws_to_df(ws_goal)
    if df_goal.empty:
        ws_goal.append_row(["1", "", "", ""])
    else:
        has_id1 = (df_goal.get("id", "") == "1").any()
        if not has_id1:
            ws_goal.append_row(["1", "", "", ""])


# =========================
# TRANSACTIONS
# =========================
def add_transaction(date_: str, description: str, ttype: str, amount: float, category: str, paid: int):
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws)
    new_id = _next_id(df)

    row = {
        "id": str(new_id),
        "date": str(date_),
        "description": str(description).strip(),
        "type": str(ttype).strip().lower(),
        "amount": str(float(amount)),
        "category": str(category).strip() if str(category).strip() else "Outros",
        "paid": str(int(paid)),
        "created_at": _now_iso(),
    }
    headers = ["id", "date", "description", "type", "amount", "category", "paid", "created_at"]
    _append_row(ws, row, headers)


def fetch_transactions(date_start: str | None = None, date_end: str | None = None) -> pd.DataFrame:
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "description", "type", "amount", "category", "paid"])

    # normaliza
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0)
    df["paid"] = pd.to_numeric(df.get("paid", 0), errors="coerce").fillna(0).astype(int)
    df["type"] = df.get("type", "").astype(str).str.strip().str.lower()
    df["category"] = df.get("category", "Outros").astype(str).fillna("Outros")

    # datas (guardadas como yyyy-mm-dd)
    df["date"] = df.get("date", "").astype(str)

    if date_start:
        df = df[df["date"] >= str(date_start)]
    if date_end:
        df = df[df["date"] <= str(date_end)]

    # id numérico
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)

    df = df.sort_values(["date", "id"], ascending=[False, False])
    return df[["id", "date", "description", "type", "amount", "category", "paid"]].copy()


def delete_transaction(tx_id: int):
    tx_id = int(tx_id)
    client = _get_client()
    sh = _open_spreadsheet(client)

    # remove link do desafio
    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link)
    if not df_link.empty and "tx_id" in df_link.columns:
        df_link["tx_id"] = pd.to_numeric(df_link["tx_id"], errors="coerce").fillna(-1).astype(int)
        df_link = df_link[df_link["tx_id"] != tx_id]
        ws_link.clear()
        ws_link.append_row(["n", "tx_id"])
        for _, r in df_link.iterrows():
            ws_link.append_row([str(r.get("n", "")), str(r.get("tx_id", ""))])

    # remove da transactions
    ws = sh.worksheet(TAB_TRANSACTIONS)
    df = _ws_to_df(ws)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != tx_id]

    ws.clear()
    headers = ["id", "date", "description", "type", "amount", "category", "paid", "created_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("date", "")),
            str(r.get("description", "")),
            str(r.get("type", "")),
            str(r.get("amount", "")),
            str(r.get("category", "")),
            str(r.get("paid", "")),
            str(r.get("created_at", "")),
        ])


def update_transactions_bulk(df_updates: pd.DataFrame):
    if df_updates is None or df_updates.empty:
        return

    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)

    upd = df_updates.copy()
    upd["id"] = pd.to_numeric(upd["id"], errors="coerce").fillna(0).astype(int)

    # atualiza em memória
    for _, r in upd.iterrows():
        rid = int(r["id"])
        mask = df["id"] == rid
        if not mask.any():
            continue
        df.loc[mask, "date"] = str(r.get("date", ""))
        df.loc[mask, "description"] = str(r.get("description", "")).strip()
        df.loc[mask, "type"] = str(r.get("type", "")).strip().lower()
        df.loc[mask, "amount"] = str(float(r.get("amount", 0.0)))
        cat = str(r.get("category", "")).strip()
        df.loc[mask, "category"] = cat if cat else "Outros"
        df.loc[mask, "paid"] = str(int(r.get("paid", 0)))

    # regrava
    ws.clear()
    headers = ["id", "date", "description", "type", "amount", "category", "paid", "created_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("date", "")),
            str(r.get("description", "")),
            str(r.get("type", "")),
            str(r.get("amount", "")),
            str(r.get("category", "")),
            str(r.get("paid", "")),
            str(r.get("created_at", "")),
        ])


# =========================
# AJUSTES DO FLUXO
# =========================
def add_cashflow_adjustment(data: str, valor: float, descricao: str | None = None):
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws)
    new_id = _next_id(df.rename(columns={"id": "id"}) if "id" in df.columns else df)

    row = {
        "id": str(new_id),
        "data": str(data),
        "valor": str(float(valor)),
        "descricao": (descricao or "").strip(),
        "created_at": _now_iso(),
    }
    headers = ["id", "data", "valor", "descricao", "created_at"]
    _append_row(ws, row, headers)


def fetch_cashflow_adjustments(date_start: str, date_end: str) -> pd.DataFrame:
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["id", "data", "valor", "descricao"])

    df["valor"] = pd.to_numeric(df.get("valor", 0), errors="coerce").fillna(0.0)
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df["data"] = df.get("data", "").astype(str)

    df = df[(df["data"] >= str(date_start)) & (df["data"] <= str(date_end))]
    df = df.sort_values(["data", "id"], ascending=[True, True])
    return df[["id", "data", "valor", "descricao"]].copy()


def delete_cashflow_adjustment(adj_id: int):
    adj_id = int(adj_id)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != adj_id]

    ws.clear()
    headers = ["id", "data", "valor", "descricao", "created_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("data", "")),
            str(r.get("valor", "")),
            str(r.get("descricao", "")),
            str(r.get("created_at", "")),
        ])


# =========================
# DÍVIDAS
# =========================
def add_debt(credor: str, descricao: str, valor: float, vencimento: str | None, prioridade: int):
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws)
    new_id = _next_id(df)

    row = {
        "id": str(new_id),
        "credor": credor.strip(),
        "descricao": (descricao or "").strip(),
        "valor": str(float(valor)),
        "vencimento": "" if not vencimento else str(vencimento),
        "prioridade": str(int(prioridade)),
        "quitada": "0",
        "created_at": _now_iso(),
    }
    headers = ["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"]
    _append_row(ws, row, headers)


def fetch_debts(show_quitadas: bool = False) -> pd.DataFrame:
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["id","credor","descricao","valor","vencimento","prioridade","quitada","created_at"])

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df["valor"] = pd.to_numeric(df.get("valor", 0), errors="coerce").fillna(0.0)
    df["prioridade"] = pd.to_numeric(df.get("prioridade", 1), errors="coerce").fillna(1).astype(int)
    df["quitada"] = pd.to_numeric(df.get("quitada", 0), errors="coerce").fillna(0).astype(int)
    df["vencimento"] = df.get("vencimento", "").astype(str)

    if not show_quitadas:
        df = df[df["quitada"] == 0]

    # ordenação simples
    df = df.sort_values(["prioridade", "vencimento", "id"], ascending=[True, True, False])
    return df[["id","credor","descricao","valor","vencimento","prioridade","quitada","created_at"]].copy()


def mark_debt_paid(debt_id: int, paid: bool):
    debt_id = int(debt_id)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == debt_id
    if not mask.any():
        return
    df.loc[mask, "quitada"] = "1" if paid else "0"

    ws.clear()
    headers = ["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("credor", "")),
            str(r.get("descricao", "")),
            str(r.get("valor", "")),
            str(r.get("vencimento", "")),
            str(r.get("prioridade", "")),
            str(r.get("quitada", "")),
            str(r.get("created_at", "")),
        ])


def delete_debt(debt_id: int):
    debt_id = int(debt_id)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != debt_id]

    ws.clear()
    headers = ["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("credor", "")),
            str(r.get("descricao", "")),
            str(r.get("valor", "")),
            str(r.get("vencimento", "")),
            str(r.get("prioridade", "")),
            str(r.get("quitada", "")),
            str(r.get("created_at", "")),
        ])


# =========================
# NOTAS
# =========================
def add_note(titulo: str, texto: str):
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws)
    new_id = _next_id(df)

    now = _now_iso()
    row = {
        "id": str(new_id),
        "titulo": str(titulo or "").strip(),
        "texto": str(texto or "").strip(),
        "created_at": now,
        "updated_at": now,
    }
    headers = ["id", "titulo", "texto", "created_at", "updated_at"]
    _append_row(ws, row, headers)


def fetch_notes() -> pd.DataFrame:
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["id","titulo","texto","created_at","updated_at"])

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)

    # formato amigável
    df["created_at"] = pd.to_datetime(df.get("created_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    df["updated_at"] = pd.to_datetime(df.get("updated_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")

    df = df.sort_values(["updated_at", "id"], ascending=[False, False])
    return df[["id","titulo","texto","created_at","updated_at"]].copy()


def update_note(note_id: int, titulo: str, texto: str):
    note_id = int(note_id)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == note_id
    if not mask.any():
        return

    df.loc[mask, "titulo"] = str(titulo or "").strip()
    df.loc[mask, "texto"] = str(texto or "").strip()
    df.loc[mask, "updated_at"] = _now_iso()

    ws.clear()
    headers = ["id", "titulo", "texto", "created_at", "updated_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("titulo", "")),
            str(r.get("texto", "")),
            str(r.get("created_at", "")),
            str(r.get("updated_at", "")),
        ])


def delete_note(note_id: int):
    note_id = int(note_id)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != note_id]

    ws.clear()
    headers = ["id", "titulo", "texto", "created_at", "updated_at"]
    ws.append_row(headers)
    for _, r in df.iterrows():
        ws.append_row([
            str(r.get("id", "")),
            str(r.get("titulo", "")),
            str(r.get("texto", "")),
            str(r.get("created_at", "")),
            str(r.get("updated_at", "")),
        ])


# =========================
# DESAFIO v2 (Sheets)
# =========================
def _min_n_for_target(target: float) -> int:
    import math
    if target <= 0:
        return 1
    n = int((math.sqrt(1 + 8 * target) - 1) / 2)
    if n * (n + 1) / 2 < target:
        n += 1
    return max(1, n)


def set_savings_goal_v2(target_amount: float, due_date: str | None):
    target_amount = float(target_amount)
    n = _min_n_for_target(target_amount)

    client = _get_client()
    sh = _open_spreadsheet(client)

    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    df_goal = _ws_to_df(ws_goal)
    if df_goal.empty:
        ws_goal.append_row(["1", str(target_amount), str(due_date or ""), str(n)])
    else:
        # regrava goal inteiro simples (uso pessoal)
        ws_goal.clear()
        ws_goal.append_row(["id", "target_amount", "due_date", "n_deposits"])
        ws_goal.append_row(["1", str(target_amount), str(due_date or ""), str(n)])

    # deposits
    ws_dep = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    df_dep = _ws_to_df(ws_dep)
    existing = {}
    if not df_dep.empty and "n" in df_dep.columns:
        df_dep["n"] = pd.to_numeric(df_dep["n"], errors="coerce").fillna(0).astype(int)
        df_dep["done"] = pd.to_numeric(df_dep.get("done", 0), errors="coerce").fillna(0).astype(int)
        existing = {int(r["n"]): int(r["done"]) for _, r in df_dep.iterrows()}

    ws_dep.clear()
    ws_dep.append_row(["n", "done"])
    for i in range(1, n + 1):
        done = existing.get(i, 0)
        ws_dep.append_row([str(i), str(done)])

    # overrides e link: mantém só até n
    ws_ov = sh.worksheet(TAB_SAVINGS_OVERRIDES)
    df_ov = _ws_to_df(ws_ov)
    if df_ov.empty:
        pass
    else:
        df_ov["n"] = pd.to_numeric(df_ov.get("n", 0), errors="coerce").fillna(0).astype(int)
        df_ov = df_ov[df_ov["n"] <= n]
        ws_ov.clear()
        ws_ov.append_row(["n", "amount"])
        for _, r in df_ov.iterrows():
            ws_ov.append_row([str(int(r.get("n", 0))), str(r.get("amount", ""))])

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link)
    if df_link.empty:
        pass
    else:
        df_link["n"] = pd.to_numeric(df_link.get("n", 0), errors="coerce").fillna(0).astype(int)
        df_link = df_link[df_link["n"] <= n]
        ws_link.clear()
        ws_link.append_row(["n", "tx_id"])
        for _, r in df_link.iterrows():
            ws_link.append_row([str(int(r.get("n", 0))), str(r.get("tx_id", ""))])


def get_savings_goal_v2():
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_SAVINGS_GOAL)
    df = _ws_to_df(ws)
    if df.empty:
        return None, None, None

    # pega id=1
    row = df[df.get("id", "") == "1"]
    if row.empty:
        return None, None, None
    r = row.iloc[0]
    t = r.get("target_amount", "")
    d = r.get("due_date", "")
    n = r.get("n_deposits", "")

    target = float(t) if str(t).strip() else None
    due = str(d).strip() or None
    ndeps = int(float(n)) if str(n).strip() else None
    return target, due, ndeps


def fetch_savings_deposits_v2_with_amount() -> pd.DataFrame:
    client = _get_client()
    sh = _open_spreadsheet(client)

    ws_dep = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    ws_ov = sh.worksheet(TAB_SAVINGS_OVERRIDES)

    dep = _ws_to_df(ws_dep)
    ov = _ws_to_df(ws_ov)

    if dep.empty:
        return pd.DataFrame(columns=["n", "done", "amount"])

    dep["n"] = pd.to_numeric(dep.get("n", 0), errors="coerce").fillna(0).astype(int)
    dep["done"] = pd.to_numeric(dep.get("done", 0), errors="coerce").fillna(0).astype(int)

    if ov.empty:
        dep["amount"] = dep["n"].astype(float)
        return dep[["n", "done", "amount"]].sort_values("n")

    ov["n"] = pd.to_numeric(ov.get("n", 0), errors="coerce").fillna(0).astype(int)
    ov["amount"] = pd.to_numeric(ov.get("amount", 0), errors="coerce").fillna(0.0)

    merged = dep.merge(ov, on="n", how="left", suffixes=("", "_ov"))
    merged["amount"] = merged["amount"].fillna(merged["n"].astype(float))
    return merged[["n", "done", "amount"]].sort_values("n")


def toggle_savings_deposit_v2(n: int, done: bool):
    n = int(n)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_SAVINGS_DEPOSITS)

    df = _ws_to_df(ws)
    if df.empty:
        return

    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)
    mask = df["n"] == n
    if not mask.any():
        return
    df.loc[mask, "done"] = "1" if done else "0"

    ws.clear()
    ws.append_row(["n", "done"])
    for _, r in df.iterrows():
        ws.append_row([str(int(r.get("n", 0))), str(r.get("done", "0"))])


def set_savings_override_v2(n: int, amount: float | None):
    n = int(n)
    client = _get_client()
    sh = _open_spreadsheet(client)
    ws = sh.worksheet(TAB_SAVINGS_OVERRIDES)

    df = _ws_to_df(ws)
    if df.empty:
        df = pd.DataFrame(columns=["n", "amount"])

    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)

    if amount is None:
        df = df[df["n"] != n]
    else:
        amount = float(amount)
        if (df["n"] == n).any():
            df.loc[df["n"] == n, "amount"] = str(amount)
        else:
            df = pd.concat([df, pd.DataFrame([{"n": n, "amount": str(amount)}])], ignore_index=True)

    ws.clear()
    ws.append_row(["n", "amount"])
    for _, r in df.sort_values("n").iterrows():
        ws.append_row([str(int(r.get("n", 0))), str(r.get("amount", ""))])


def reset_savings_marks_v2():
    client = _get_client()
    sh = _open_spreadsheet(client)

    ws = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    df = _ws_to_df(ws)
    if df.empty:
        return
    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)
    df["done"] = "0"

    ws.clear()
    ws.append_row(["n", "done"])
    for _, r in df.sort_values("n").iterrows():
        ws.append_row([str(int(r.get("n", 0))), "0"])

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    ws_link.clear()
    ws_link.append_row(["n", "tx_id"])


def clear_savings_goal_v2():
    client = _get_client()
    sh = _open_spreadsheet(client)

    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    ws_goal.clear()
    ws_goal.append_row(["id", "target_amount", "due_date", "n_deposits"])
    ws_goal.append_row(["1", "", "", ""])

    for tab, headers in [
        (TAB_SAVINGS_DEPOSITS, ["n", "done"]),
        (TAB_SAVINGS_OVERRIDES, ["n", "amount"]),
        (TAB_SAVINGS_TX_LINK, ["n", "tx_id"]),
    ]:
        ws = sh.worksheet(tab)
        ws.clear()
        ws.append_row(headers)


def create_desafio_transaction(date_: str, n: int, amount: float):
    # cria entrada na transactions e grava link n -> tx_id
    client = _get_client()
    sh = _open_spreadsheet(client)

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link)
    if not df_link.empty:
        df_link["n"] = pd.to_numeric(df_link.get("n", 0), errors="coerce").fillna(0).astype(int)
        row = df_link[df_link["n"] == int(n)]
        if not row.empty:
            tx_id = row.iloc[0].get("tx_id", "")
            if str(tx_id).strip():
                return int(float(tx_id))

    # cria transação
    add_transaction(
        date_=str(date_),
        description=f"Desafio - Depósito #{int(n)}",
        ttype="entrada",
        amount=float(amount),
        category="Desafio",
        paid=1,
    )

    # pega último id inserido (mais simples)
    df_tx = fetch_transactions(None, None)
    tx_id = int(df_tx["id"].max()) if not df_tx.empty else 1

    # grava link
    if df_link.empty:
        df_link = pd.DataFrame(columns=["n", "tx_id"])
    df_link = pd.concat([df_link, pd.DataFrame([{"n": int(n), "tx_id": int(tx_id)}])], ignore_index=True)

    ws_link.clear()
    ws_link.append_row(["n", "tx_id"])
    for _, r in df_link.iterrows():
        ws_link.append_row([str(int(float(r.get("n", 0)))), str(int(float(r.get("tx_id", 0))))])

    return int(tx_id)


def delete_desafio_transaction(n: int):
    n = int(n)
    client = _get_client()
    sh = _open_spreadsheet(client)

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link)
    if df_link.empty:
        return

    df_link["n"] = pd.to_numeric(df_link.get("n", 0), errors="coerce").fillna(0).astype(int)
    row = df_link[df_link["n"] == n]
    if row.empty:
        return

    tx_id = row.iloc[0].get("tx_id", "")
    if str(tx_id).strip():
        delete_transaction(int(float(tx_id)))

    df_link = df_link[df_link["n"] != n]
    ws_link.clear()
    ws_link.append_row(["n", "tx_id"])
    for _, r in df_link.iterrows():
        ws_link.append_row([str(int(r.get("n", 0))), str(r.get("tx_id", ""))])

