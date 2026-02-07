# db.py (Google Sheets como "banco")
from __future__ import annotations

from datetime import datetime
import time
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
    "https://www.googleapis.com/auth/drive",
]

# Nomes das abas
TAB_TRANSACTIONS = "transactions"
TAB_ADJUSTMENTS = "cashflow_adjustments"
TAB_DEBTS = "debts"
TAB_NOTES = "notes"

TAB_SAVINGS_GOAL = "savings_goal_v2"
TAB_SAVINGS_DEPOSITS = "savings_deposits_v2"
TAB_SAVINGS_OVERRIDES = "savings_overrides_v2"
TAB_SAVINGS_TX_LINK = "savings_tx_link_v2"

# Headers (fixos)
H_TRANSACTIONS = ["id", "date", "description", "type", "amount", "category", "paid", "created_at"]
H_ADJUSTMENTS = ["id", "data", "valor", "descricao", "created_at"]
H_DEBTS = ["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"]
H_NOTES = ["id", "titulo", "texto", "created_at", "updated_at"]
H_SAVINGS_GOAL = ["id", "target_amount", "due_date", "n_deposits"]
H_SAVINGS_DEPOSITS = ["n", "done"]
H_SAVINGS_OVERRIDES = ["n", "amount"]
H_SAVINGS_TX_LINK = ["n", "tx_id"]

# =========================
# RETRY / BACKOFF (reduz 429)
# =========================
def _with_retry(fn, tries: int = 5, base_sleep: float = 0.6):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            msg = str(e).lower()
            # 429 / quota / rate limit -> espera e tenta de novo
            if "429" in msg or "quota" in msg or "rate" in msg or "user-rate" in msg:
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last

# =========================
# HELPERS
# =========================
def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _get_spreadsheet_id() -> str:
    sid = str(st.secrets.get("GSHEETS_SPREADSHEET_ID", "")).strip()
    if not sid:
        raise RuntimeError(
            "GSHEETS_SPREADSHEET_ID não encontrado nos secrets.\n"
            'Coloque no Streamlit Secrets: GSHEETS_SPREADSHEET_ID = "SEU_ID_AQUI"'
        )
    return sid

def _get_client() -> gspread.Client:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Falta [gcp_service_account] no Streamlit Secrets.")
    sa_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)

def _open_spreadsheet():
    sid = _get_spreadsheet_id()

    sa_email = "desconhecido"
    try:
        sa_email = dict(st.secrets["gcp_service_account"]).get("client_email", "desconhecido")
    except Exception:
        pass

    client = _get_client()
    try:
        return _with_retry(lambda: client.open_by_key(sid))
    except Exception as e:
        raise RuntimeError(
            "Não consegui abrir a planilha no Google Sheets.\n"
            f"- Spreadsheet ID: {sid}\n"
            f"- Service Account: {sa_email}\n\n"
            "Checklist:\n"
            "1) Compartilhe a planilha com esse e-mail como EDITOR.\n"
            "2) Ative as APIs no Google Cloud do projeto:\n"
            "   - Google Sheets API\n"
            "   - Google Drive API\n"
        ) from e

def ping_db() -> tuple[bool, str]:
    try:
        sh = _open_spreadsheet()
        _ = sh.title
        return True, "ok"
    except Exception as e:
        return False, str(e)

def _ensure_worksheet(sh: gspread.Spreadsheet, title: str, headers: list[str]):
    """
    Garante que a aba exista e tenha cabeçalho.
    À prova de "already exists".
    """
    # 1) abre se existir
    try:
        ws = _with_retry(lambda: sh.worksheet(title))
    except Exception:
        # 2) cria se não existir
        try:
            ws = _with_retry(lambda: sh.add_worksheet(title=title, rows=2000, cols=max(10, len(headers) + 2)))
        except Exception as e:
            # 3) se já existe (race condition), abre de novo
            msg = str(e).lower()
            if "already exists" in msg or "exists" in msg:
                ws = _with_retry(lambda: sh.worksheet(title))
            else:
                raise

    # lê só a linha 1 (barato)
    first_row = _with_retry(lambda: ws.row_values(1))
    first_row = [str(c).strip() for c in first_row] if first_row else []

    if not first_row:
        _with_retry(lambda: ws.append_row(headers))
    elif first_row != headers:
        _with_retry(lambda: ws.update("A1", [headers]))

    return ws

def _ws_to_df(ws, headers_expected: list[str]) -> pd.DataFrame:
    """
    Lê aba inteira, mas já tenta padronizar header e evitar bug de columns vazias.
    """
    df = _with_retry(lambda: get_as_dataframe(ws, evaluate_formulas=True, header=0, dtype=str))
    df = df.dropna(how="all")

    # normaliza colunas
    df.columns = [str(c).strip() for c in df.columns]
    # se vier sem colunas (aba vazia), força
    if df.empty and (len(df.columns) == 0):
        return pd.DataFrame(columns=headers_expected)
    return df

def _append_row(ws, row: dict, headers: list[str]):
    values = [row.get(h, "") for h in headers]
    _with_retry(lambda: ws.append_row(values, value_input_option="USER_ENTERED"))

def _next_id(df: pd.DataFrame) -> int:
    if df is None or df.empty or "id" not in df.columns:
        return 1
    s = pd.to_numeric(df["id"], errors="coerce").dropna()
    return int(s.max()) + 1 if not s.empty else 1

# =========================
# INIT
# =========================
def init_db():
    # lock contra rerun duplicado
    if st.session_state.get("_INIT_DB_RUNNING", False):
        return
    st.session_state["_INIT_DB_RUNNING"] = True

    try:
        sh = _open_spreadsheet()

        _ensure_worksheet(sh, TAB_TRANSACTIONS, H_TRANSACTIONS)
        _ensure_worksheet(sh, TAB_ADJUSTMENTS, H_ADJUSTMENTS)
        _ensure_worksheet(sh, TAB_DEBTS, H_DEBTS)
        _ensure_worksheet(sh, TAB_NOTES, H_NOTES)

        _ensure_worksheet(sh, TAB_SAVINGS_GOAL, H_SAVINGS_GOAL)
        _ensure_worksheet(sh, TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
        _ensure_worksheet(sh, TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES)
        _ensure_worksheet(sh, TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)

        # --- conserta goal se tiver lixo ---
        ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
        df_goal = _ws_to_df(ws_goal, H_SAVINGS_GOAL)

        # se a aba não tem as colunas certas, reescreve o header
        if df_goal.empty or ("id" not in df_goal.columns):
            _with_retry(lambda: ws_goal.clear())
            _with_retry(lambda: ws_goal.append_row(H_SAVINGS_GOAL))
            df_goal = _ws_to_df(ws_goal, H_SAVINGS_GOAL)

        # garante linha id=1
        has_id1 = False
        if not df_goal.empty and "id" in df_goal.columns:
            has_id1 = (df_goal["id"].astype(str).str.strip() == "1").any()

        if not has_id1:
            _with_retry(lambda: ws_goal.append_row(["1", "", "", ""]))

    finally:
        st.session_state["_INIT_DB_RUNNING"] = False

# =========================
# TRANSACTIONS
# =========================
def add_transaction(date_: str, description: str, ttype: str, amount: float, category: str, paid: int):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws, H_TRANSACTIONS)
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
    _append_row(ws, row, H_TRANSACTIONS)

def fetch_transactions(date_start: str | None = None, date_end: str | None = None) -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws, H_TRANSACTIONS)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "description", "type", "amount", "category", "paid"])

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0)
    df["paid"] = pd.to_numeric(df.get("paid", 0), errors="coerce").fillna(0).astype(int)
    df["type"] = df.get("type", "").astype(str).str.strip().str.lower()
    df["category"] = df.get("category", "Outros").astype(str).fillna("Outros")
    df["date"] = df.get("date", "").astype(str)

    if date_start:
        df = df[df["date"] >= str(date_start)]
    if date_end:
        df = df[df["date"] <= str(date_end)]

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df.sort_values(["date", "id"], ascending=[False, False])
    return df[["id", "date", "description", "type", "amount", "category", "paid"]].copy()

def delete_transaction(tx_id: int):
    tx_id = int(tx_id)
    sh = _open_spreadsheet()

    # remove link do desafio
    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link, H_SAVINGS_TX_LINK)
    if not df_link.empty and "tx_id" in df_link.columns:
        df_link["tx_id"] = pd.to_numeric(df_link["tx_id"], errors="coerce").fillna(-1).astype(int)
        df_link = df_link[df_link["tx_id"] != tx_id]
        _with_retry(lambda: ws_link.clear())
        _with_retry(lambda: ws_link.append_row(H_SAVINGS_TX_LINK))
        for _, r in df_link.iterrows():
            _with_retry(lambda rr=r: ws_link.append_row([str(rr.get("n", "")), str(rr.get("tx_id", ""))]))

    # remove da transactions
    ws = sh.worksheet(TAB_TRANSACTIONS)
    df = _ws_to_df(ws, H_TRANSACTIONS)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != tx_id]

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_TRANSACTIONS))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("date", "")),
            str(rr.get("description", "")),
            str(rr.get("type", "")),
            str(rr.get("amount", "")),
            str(rr.get("category", "")),
            str(rr.get("paid", "")),
            str(rr.get("created_at", "")),
        ]))

def update_transactions_bulk(df_updates: pd.DataFrame):
    if df_updates is None or df_updates.empty:
        return

    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _ws_to_df(ws, H_TRANSACTIONS)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)

    upd = df_updates.copy()
    upd["id"] = pd.to_numeric(upd["id"], errors="coerce").fillna(0).astype(int)

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

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_TRANSACTIONS))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("date", "")),
            str(rr.get("description", "")),
            str(rr.get("type", "")),
            str(rr.get("amount", "")),
            str(rr.get("category", "")),
            str(rr.get("paid", "")),
            str(rr.get("created_at", "")),
        ]))

# =========================
# AJUSTES DO FLUXO
# =========================
def add_cashflow_adjustment(data: str, valor: float, descricao: str | None = None):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws, H_ADJUSTMENTS)
    new_id = _next_id(df)

    row = {
        "id": str(new_id),
        "data": str(data),
        "valor": str(float(valor)),
        "descricao": (descricao or "").strip(),
        "created_at": _now_iso(),
    }
    _append_row(ws, row, H_ADJUSTMENTS)

def fetch_cashflow_adjustments(date_start: str, date_end: str) -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws, H_ADJUSTMENTS)
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
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _ws_to_df(ws, H_ADJUSTMENTS)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != adj_id]

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_ADJUSTMENTS))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("data", "")),
            str(rr.get("valor", "")),
            str(rr.get("descricao", "")),
            str(rr.get("created_at", "")),
        ]))

# =========================
# DÍVIDAS
# =========================
def add_debt(credor: str, descricao: str, valor: float, vencimento: str | None, prioridade: int):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws, H_DEBTS)
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
    _append_row(ws, row, H_DEBTS)

def fetch_debts(show_quitadas: bool = False) -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws, H_DEBTS)
    if df.empty:
        return pd.DataFrame(columns=H_DEBTS)

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df["valor"] = pd.to_numeric(df.get("valor", 0), errors="coerce").fillna(0.0)
    df["prioridade"] = pd.to_numeric(df.get("prioridade", 1), errors="coerce").fillna(1).astype(int)
    df["quitada"] = pd.to_numeric(df.get("quitada", 0), errors="coerce").fillna(0).astype(int)
    df["vencimento"] = df.get("vencimento", "").astype(str)

    if not show_quitadas:
        df = df[df["quitada"] == 0]

    df = df.sort_values(["prioridade", "vencimento", "id"], ascending=[True, True, False])
    return df[["id","credor","descricao","valor","vencimento","prioridade","quitada","created_at"]].copy()

def mark_debt_paid(debt_id: int, paid: bool):
    debt_id = int(debt_id)
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws, H_DEBTS)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == debt_id
    if not mask.any():
        return
    df.loc[mask, "quitada"] = "1" if paid else "0"

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_DEBTS))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("credor", "")),
            str(rr.get("descricao", "")),
            str(rr.get("valor", "")),
            str(rr.get("vencimento", "")),
            str(rr.get("prioridade", "")),
            str(rr.get("quitada", "")),
            str(rr.get("created_at", "")),
        ]))

def delete_debt(debt_id: int):
    debt_id = int(debt_id)
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_DEBTS)

    df = _ws_to_df(ws, H_DEBTS)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != debt_id]

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_DEBTS))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("credor", "")),
            str(rr.get("descricao", "")),
            str(rr.get("valor", "")),
            str(rr.get("vencimento", "")),
            str(rr.get("prioridade", "")),
            str(rr.get("quitada", "")),
            str(rr.get("created_at", "")),
        ]))

# =========================
# NOTAS
# =========================
def add_note(titulo: str, texto: str):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws, H_NOTES)
    new_id = _next_id(df)

    now = _now_iso()
    row = {
        "id": str(new_id),
        "titulo": str(titulo or "").strip(),
        "texto": str(texto or "").strip(),
        "created_at": now,
        "updated_at": now,
    }
    _append_row(ws, row, H_NOTES)

def fetch_notes() -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws, H_NOTES)
    if df.empty:
        return pd.DataFrame(columns=["id","titulo","texto","created_at","updated_at"])

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df["created_at"] = pd.to_datetime(df.get("created_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    df["updated_at"] = pd.to_datetime(df.get("updated_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")

    df = df.sort_values(["updated_at", "id"], ascending=[False, False])
    return df[["id","titulo","texto","created_at","updated_at"]].copy()

def update_note(note_id: int, titulo: str, texto: str):
    note_id = int(note_id)
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws, H_NOTES)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == note_id
    if not mask.any():
        return

    df.loc[mask, "titulo"] = str(titulo or "").strip()
    df.loc[mask, "texto"] = str(texto or "").strip()
    df.loc[mask, "updated_at"] = _now_iso()

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_NOTES))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("titulo", "")),
            str(rr.get("texto", "")),
            str(rr.get("created_at", "")),
            str(rr.get("updated_at", "")),
        ]))

def delete_note(note_id: int):
    note_id = int(note_id)
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_NOTES)

    df = _ws_to_df(ws, H_NOTES)
    if df.empty:
        return

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != note_id]

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_NOTES))
    for _, r in df.iterrows():
        _with_retry(lambda rr=r: ws.append_row([
            str(rr.get("id", "")),
            str(rr.get("titulo", "")),
            str(rr.get("texto", "")),
            str(rr.get("created_at", "")),
            str(rr.get("updated_at", "")),
        ]))

# =========================
# DESAFIO v2
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

    sh = _open_spreadsheet()

    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    _with_retry(lambda: ws_goal.clear())
    _with_retry(lambda: ws_goal.append_row(H_SAVINGS_GOAL))
    _with_retry(lambda: ws_goal.append_row(["1", str(target_amount), str(due_date or ""), str(n)]))

    # deposits
    ws_dep = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    dep = _ws_to_df(ws_dep, H_SAVINGS_DEPOSITS)
    existing = {}
    if not dep.empty and "n" in dep.columns:
        dep["n"] = pd.to_numeric(dep["n"], errors="coerce").fillna(0).astype(int)
        dep["done"] = pd.to_numeric(dep.get("done", 0), errors="coerce").fillna(0).astype(int)
        existing = {int(r["n"]): int(r["done"]) for _, r in dep.iterrows()}

    _with_retry(lambda: ws_dep.clear())
    _with_retry(lambda: ws_dep.append_row(H_SAVINGS_DEPOSITS))
    for i in range(1, n + 1):
        done = existing.get(i, 0)
        _with_retry(lambda ii=i, dd=done: ws_dep.append_row([str(ii), str(dd)]))

    # overrides mantém só até n
    ws_ov = sh.worksheet(TAB_SAVINGS_OVERRIDES)
    ov = _ws_to_df(ws_ov, H_SAVINGS_OVERRIDES)
    if not ov.empty:
        ov["n"] = pd.to_numeric(ov.get("n", 0), errors="coerce").fillna(0).astype(int)
        ov = ov[ov["n"] <= n]
    _with_retry(lambda: ws_ov.clear())
    _with_retry(lambda: ws_ov.append_row(H_SAVINGS_OVERRIDES))
    if not ov.empty:
        for _, r in ov.iterrows():
            _with_retry(lambda rr=r: ws_ov.append_row([str(int(rr.get("n", 0))), str(rr.get("amount", ""))]))

    # links mantém só até n
    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    link = _ws_to_df(ws_link, H_SAVINGS_TX_LINK)
    if not link.empty:
        link["n"] = pd.to_numeric(link.get("n", 0), errors="coerce").fillna(0).astype(int)
        link = link[link["n"] <= n]
    _with_retry(lambda: ws_link.clear())
    _with_retry(lambda: ws_link.append_row(H_SAVINGS_TX_LINK))
    if not link.empty:
        for _, r in link.iterrows():
            _with_retry(lambda rr=r: ws_link.append_row([str(int(rr.get("n", 0))), str(rr.get("tx_id", ""))]))

def get_savings_goal_v2():
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_SAVINGS_GOAL)
    df = _ws_to_df(ws, H_SAVINGS_GOAL)

    if df.empty:
        return None, None, None

    # tenta achar linha id=1; se não tiver, pega a primeira válida
    if "id" in df.columns:
        row = df[df["id"].astype(str).str.strip() == "1"]
        if row.empty:
            row = df.iloc[[0]]
    else:
        row = df.iloc[[0]]

    r = row.iloc[0]

    t = str(r.get("target_amount", "")).strip()
    d = str(r.get("due_date", "")).strip()
    n = str(r.get("n_deposits", "")).strip()

    # parse seguro
    target = pd.to_numeric(pd.Series([t]), errors="coerce").iloc[0]
    target = float(target) if pd.notna(target) else None

    due = d if d and d.lower() not in ("none", "nan") else None

    ndeps = pd.to_numeric(pd.Series([n]), errors="coerce").iloc[0]
    ndeps = int(ndeps) if pd.notna(ndeps) else None

    return target, due, ndeps

def fetch_savings_deposits_v2_with_amount() -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws_dep = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    ws_ov = sh.worksheet(TAB_SAVINGS_OVERRIDES)

    dep = _ws_to_df(ws_dep, H_SAVINGS_DEPOSITS)
    ov = _ws_to_df(ws_ov, H_SAVINGS_OVERRIDES)

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
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_SAVINGS_DEPOSITS)

    df = _ws_to_df(ws, H_SAVINGS_DEPOSITS)
    if df.empty:
        return

    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)
    mask = df["n"] == n
    if not mask.any():
        return
    df.loc[mask, "done"] = "1" if done else "0"

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_SAVINGS_DEPOSITS))
    for _, r in df.sort_values("n").iterrows():
        _with_retry(lambda rr=r: ws.append_row([str(int(rr.get("n", 0))), str(rr.get("done", "0"))]))

def set_savings_override_v2(n: int, amount: float | None):
    n = int(n)
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_SAVINGS_OVERRIDES)

    df = _ws_to_df(ws, H_SAVINGS_OVERRIDES)
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

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_SAVINGS_OVERRIDES))
    for _, r in df.sort_values("n").iterrows():
        _with_retry(lambda rr=r: ws.append_row([str(int(rr.get("n", 0))), str(rr.get("amount", ""))]))

def reset_savings_marks_v2():
    sh = _open_spreadsheet()

    ws = sh.worksheet(TAB_SAVINGS_DEPOSITS)
    df = _ws_to_df(ws, H_SAVINGS_DEPOSITS)
    if df.empty:
        return
    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)
    df["done"] = "0"

    _with_retry(lambda: ws.clear())
    _with_retry(lambda: ws.append_row(H_SAVINGS_DEPOSITS))
    for _, r in df.sort_values("n").iterrows():
        _with_retry(lambda rr=r: ws.append_row([str(int(rr.get("n", 0))), "0"]))

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    _with_retry(lambda: ws_link.clear())
    _with_retry(lambda: ws_link.append_row(H_SAVINGS_TX_LINK))

def clear_savings_goal_v2():
    sh = _open_spreadsheet()

    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    _with_retry(lambda: ws_goal.clear())
    _with_retry(lambda: ws_goal.append_row(H_SAVINGS_GOAL))
    _with_retry(lambda: ws_goal.append_row(["1", "", "", ""]))

    for tab, headers in [
        (TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS),
        (TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES),
        (TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK),
    ]:
        ws = sh.worksheet(tab)
        _with_retry(lambda w=ws: w.clear())
        _with_retry(lambda w=ws, h=headers: w.append_row(h))

def create_desafio_transaction(date_: str, n: int, amount: float):
    sh = _open_spreadsheet()

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link, H_SAVINGS_TX_LINK)
    if not df_link.empty:
        df_link["n"] = pd.to_numeric(df_link.get("n", 0), errors="coerce").fillna(0).astype(int)
        row = df_link[df_link["n"] == int(n)]
        if not row.empty:
            tx_id = row.iloc[0].get("tx_id", "")
            if str(tx_id).strip():
                return int(float(tx_id))

    add_transaction(
        date_=str(date_),
        description=f"Desafio - Depósito #{int(n)}",
        ttype="entrada",
        amount=float(amount),
        category="Desafio",
        paid=1,
    )

    df_tx = fetch_transactions(None, None)
    tx_id = int(df_tx["id"].max()) if not df_tx.empty else 1

    if df_link.empty:
        df_link = pd.DataFrame(columns=["n", "tx_id"])
    df_link = pd.concat([df_link, pd.DataFrame([{"n": int(n), "tx_id": int(tx_id)}])], ignore_index=True)

    _with_retry(lambda: ws_link.clear())
    _with_retry(lambda: ws_link.append_row(H_SAVINGS_TX_LINK))
    for _, r in df_link.iterrows():
        _with_retry(lambda rr=r: ws_link.append_row([str(int(float(rr.get("n", 0)))), str(int(float(rr.get("tx_id", 0))))]))

    return int(tx_id)

def delete_desafio_transaction(n: int):
    n = int(n)
    sh = _open_spreadsheet()

    ws_link = sh.worksheet(TAB_SAVINGS_TX_LINK)
    df_link = _ws_to_df(ws_link, H_SAVINGS_TX_LINK)
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

    _with_retry(lambda: ws_link.clear())
    _with_retry(lambda: ws_link.append_row(H_SAVINGS_TX_LINK))
    for _, r in df_link.iterrows():
        _with_retry(lambda rr=r: ws_link.append_row([str(int(rr.get("n", 0))), str(rr.get("tx_id", ""))]))


