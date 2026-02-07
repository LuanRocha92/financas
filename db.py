# db.py (Google Sheets como "banco") - OTIMIZADO
from __future__ import annotations

from datetime import datetime
import time
import random
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# limite de linhas lidas por aba (evita ler infinito e reduz payload)
MAX_ROWS = int(st.secrets.get("GSHEETS_MAX_ROWS", 5000))

# Nomes das abas
TAB_TRANSACTIONS = "transactions"
TAB_ADJUSTMENTS = "cashflow_adjustments"
TAB_DEBTS = "debts"
TAB_NOTES = "notes"

TAB_SAVINGS_GOAL = "savings_goal_v2"
TAB_SAVINGS_DEPOSITS = "savings_deposits_v2"
TAB_SAVINGS_OVERRIDES = "savings_overrides_v2"
TAB_SAVINGS_TX_LINK = "savings_tx_link_v2"


# =========================
# HELPERS
# =========================
def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _get_spreadsheet_id() -> str:
    sid = str(st.secrets.get("GSHEETS_SPREADSHEET_ID", "")).strip()
    if not sid:
        raise RuntimeError(
            "GSHEETS_SPREADSHEET_ID não encontrado nos secrets. "
            'Ex: GSHEETS_SPREADSHEET_ID = "SEU_ID_AQUI"'
        )
    return sid


def _get_sa_email() -> str:
    try:
        return dict(st.secrets["gcp_service_account"]).get("client_email", "desconhecido")
    except Exception:
        return "desconhecido"


def _is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("429" in msg) or ("quota" in msg) or ("rate" in msg) or ("too many" in msg)


def _with_retry(fn, tries: int = 6, base_sleep: float = 0.6):
    """
    Retry com backoff exponencial pra 429 / 503 / instabilidade.
    """
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            # retry só em casos típicos de quota/instabilidade
            msg = str(e).lower()
            retryable = _is_rate_limit_error(e) or ("503" in msg) or ("internal error" in msg)
            if not retryable or i == tries - 1:
                raise
            sleep = (base_sleep * (2 ** i)) + random.uniform(0, 0.4)
            time.sleep(sleep)
    raise last  # nunca chega aqui


def _col_letter(n: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA ...
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


@st.cache_resource
def _client_cached() -> gspread.Client:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets não configurado. Falta [gcp_service_account] no Streamlit.")
    sa_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def _spreadsheet_cached() -> gspread.Spreadsheet:
    client = _client_cached()
    sid = _get_spreadsheet_id()
    sa_email = _get_sa_email()

    def _open():
        return client.open_by_key(sid)

    try:
        return _with_retry(_open)
    except Exception as e:
        raise RuntimeError(
            "Não consegui abrir a planilha no Google Sheets.\n"
            f"- Spreadsheet ID: {sid}\n"
            f"- Service Account: {sa_email}\n\n"
            "Checklist:\n"
            "1) Compartilhe a planilha com o e-mail da Service Account como EDITOR.\n"
            "2) Ative no Google Cloud do projeto:\n"
            "   - Google Sheets API\n"
            "   - Google Drive API\n"
            "3) Confirme se o ID é o do link (entre /d/ e /edit).\n"
        ) from e


def _open_spreadsheet():
    return _spreadsheet_cached()


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
    IMPORTANTE: aqui a gente NÃO usa get_all_values() (pesado).
    """
    def _get_or_create():
        try:
            return sh.worksheet(title)
        except Exception:
            return sh.add_worksheet(title=title, rows=2000, cols=max(10, len(headers) + 2))

    ws = _with_retry(_get_or_create)

    # lê só a primeira linha
    def _read_header():
        return ws.row_values(1)

    first_row = _with_retry(_read_header)
    first_row = [str(c).strip() for c in first_row] if first_row else []

    if not first_row:
        _with_retry(lambda: ws.append_row(headers))
    elif first_row != headers:
        # sobrescreve só o header
        _with_retry(lambda: ws.update("A1", [headers]))

    return ws


def _ws_to_df(ws: gspread.Worksheet, headers: list[str]) -> pd.DataFrame:
    """
    Lê um range limitado: A1:COL{MAX_ROWS} (muito mais leve).
    """
    last_col = _col_letter(len(headers))
    rng = f"A1:{last_col}{MAX_ROWS}"

    def _read_values():
        return ws.get_values(rng, value_render_option="UNFORMATTED_VALUE")

    values = _with_retry(_read_values)

    if not values:
        return pd.DataFrame(columns=headers)

    # garante pelo menos o header
    sheet_header = [str(x).strip() for x in values[0]] if values else []
    if sheet_header != headers:
        # tenta forçar header correto (sem travar)
        _with_retry(lambda: ws.update("A1", [headers]))
        sheet_header = headers

    rows = values[1:] if len(values) > 1 else []
    # remove linhas totalmente vazias
    cleaned = []
    for r in rows:
        r = list(r) + [""] * (len(headers) - len(r))
        r = r[: len(headers)]
        if any(str(x).strip() != "" for x in r):
            cleaned.append(r)

    df = pd.DataFrame(cleaned, columns=headers)
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
# HEADERS FIXOS
# =========================
H_TRANSACTIONS = ["id", "date", "description", "type", "amount", "category", "paid", "created_at"]
H_ADJUSTMENTS = ["id", "data", "valor", "descricao", "created_at"]
H_DEBTS = ["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada", "created_at"]
H_NOTES = ["id", "titulo", "texto", "created_at", "updated_at"]

H_SAVINGS_GOAL = ["id", "target_amount", "due_date", "n_deposits"]
H_SAVINGS_DEPOSITS = ["n", "done"]
H_SAVINGS_OVERRIDES = ["n", "amount"]
H_SAVINGS_TX_LINK = ["n", "tx_id"]


# =========================
# INIT DB (leve)
# =========================
def init_db():
    sh = _open_spreadsheet()

    _ensure_worksheet(sh, TAB_TRANSACTIONS, H_TRANSACTIONS)
    _ensure_worksheet(sh, TAB_ADJUSTMENTS, H_ADJUSTMENTS)
    _ensure_worksheet(sh, TAB_DEBTS, H_DEBTS)
    _ensure_worksheet(sh, TAB_NOTES, H_NOTES)

    _ensure_worksheet(sh, TAB_SAVINGS_GOAL, H_SAVINGS_GOAL)
    _ensure_worksheet(sh, TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
    _ensure_worksheet(sh, TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES)
    _ensure_worksheet(sh, TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)

    # garante id=1 no goal (lê range pequeno)
    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    df_goal = _ws_to_df(ws_goal, H_SAVINGS_GOAL)
    if df_goal.empty or not (df_goal.get("id", "") == "1").any():
        _with_retry(lambda: ws_goal.append_row(["1", "", "", ""]))


# =========================
# CACHE DE LEITURA (reduz muito as chamadas)
# =========================
@st.cache_data(ttl=10)
def _read_tab(tab_name: str, headers: list[str]) -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = sh.worksheet(tab_name)
    return _ws_to_df(ws, headers)


def _rewrite_tab(tab_name: str, headers: list[str], df: pd.DataFrame):
    sh = _open_spreadsheet()
    ws = sh.worksheet(tab_name)

    def _write():
        ws.clear()
        ws.append_row(headers)
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                ws.append_row([str(r.get(h, "")) for h in headers])
    _with_retry(_write)

    # invalida cache da aba
    _read_tab.clear()


# =========================
# TRANSACTIONS
# =========================
def add_transaction(date_: str, description: str, ttype: str, amount: float, category: str, paid: int):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_TRANSACTIONS)

    df = _read_tab(TAB_TRANSACTIONS, H_TRANSACTIONS)
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
    _read_tab.clear()


def fetch_transactions(date_start: str | None = None, date_end: str | None = None) -> pd.DataFrame:
    df = _read_tab(TAB_TRANSACTIONS, H_TRANSACTIONS)
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

    # remove link do desafio
    df_link = _read_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)
    if not df_link.empty and "tx_id" in df_link.columns:
        df_link["tx_id"] = pd.to_numeric(df_link["tx_id"], errors="coerce").fillna(-1).astype(int)
        df_link = df_link[df_link["tx_id"] != tx_id]
        _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, df_link)

    # remove da transactions
    df = _read_tab(TAB_TRANSACTIONS, H_TRANSACTIONS)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != tx_id]
    _rewrite_tab(TAB_TRANSACTIONS, H_TRANSACTIONS, df)


def update_transactions_bulk(df_updates: pd.DataFrame):
    if df_updates is None or df_updates.empty:
        return

    df = _read_tab(TAB_TRANSACTIONS, H_TRANSACTIONS)
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

    _rewrite_tab(TAB_TRANSACTIONS, H_TRANSACTIONS, df)


# =========================
# AJUSTES DO FLUXO
# =========================
def add_cashflow_adjustment(data: str, valor: float, descricao: str | None = None):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_ADJUSTMENTS)

    df = _read_tab(TAB_ADJUSTMENTS, H_ADJUSTMENTS)
    new_id = _next_id(df)

    row = {
        "id": str(new_id),
        "data": str(data),
        "valor": str(float(valor)),
        "descricao": (descricao or "").strip(),
        "created_at": _now_iso(),
    }
    _append_row(ws, row, H_ADJUSTMENTS)
    _read_tab.clear()


def fetch_cashflow_adjustments(date_start: str, date_end: str) -> pd.DataFrame:
    df = _read_tab(TAB_ADJUSTMENTS, H_ADJUSTMENTS)
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
    df = _read_tab(TAB_ADJUSTMENTS, H_ADJUSTMENTS)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != adj_id]
    _rewrite_tab(TAB_ADJUSTMENTS, H_ADJUSTMENTS, df)


# =========================
# DÍVIDAS
# =========================
def add_debt(credor: str, descricao: str, valor: float, vencimento: str | None, prioridade: int):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_DEBTS)

    df = _read_tab(TAB_DEBTS, H_DEBTS)
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
    _read_tab.clear()


def fetch_debts(show_quitadas: bool = False) -> pd.DataFrame:
    df = _read_tab(TAB_DEBTS, H_DEBTS)
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
    df = _read_tab(TAB_DEBTS, H_DEBTS)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == debt_id
    if not mask.any():
        return
    df.loc[mask, "quitada"] = "1" if paid else "0"
    _rewrite_tab(TAB_DEBTS, H_DEBTS, df)


def delete_debt(debt_id: int):
    debt_id = int(debt_id)
    df = _read_tab(TAB_DEBTS, H_DEBTS)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != debt_id]
    _rewrite_tab(TAB_DEBTS, H_DEBTS, df)


# =========================
# NOTAS
# =========================
def add_note(titulo: str, texto: str):
    sh = _open_spreadsheet()
    ws = sh.worksheet(TAB_NOTES)

    df = _read_tab(TAB_NOTES, H_NOTES)
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
    _read_tab.clear()


def fetch_notes() -> pd.DataFrame:
    df = _read_tab(TAB_NOTES, H_NOTES)
    if df.empty:
        return pd.DataFrame(columns=["id","titulo","texto","created_at","updated_at"])

    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df["created_at"] = pd.to_datetime(df.get("created_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    df["updated_at"] = pd.to_datetime(df.get("updated_at", ""), errors="coerce").dt.strftime("%d/%m/%Y %H:%M")

    df = df.sort_values(["updated_at", "id"], ascending=[False, False])
    return df[["id","titulo","texto","created_at","updated_at"]].copy()


def update_note(note_id: int, titulo: str, texto: str):
    note_id = int(note_id)
    df = _read_tab(TAB_NOTES, H_NOTES)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    mask = df["id"] == note_id
    if not mask.any():
        return
    df.loc[mask, "titulo"] = str(titulo or "").strip()
    df.loc[mask, "texto"] = str(texto or "").strip()
    df.loc[mask, "updated_at"] = _now_iso()
    _rewrite_tab(TAB_NOTES, H_NOTES, df)


def delete_note(note_id: int):
    note_id = int(note_id)
    df = _read_tab(TAB_NOTES, H_NOTES)
    if df.empty:
        return
    df["id"] = pd.to_numeric(df.get("id", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["id"] != note_id]
    _rewrite_tab(TAB_NOTES, H_NOTES, df)


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

    sh = _open_spreadsheet()

    # goal
    ws_goal = sh.worksheet(TAB_SAVINGS_GOAL)
    _rewrite_tab(TAB_SAVINGS_GOAL, H_SAVINGS_GOAL, pd.DataFrame([{
        "id": "1",
        "target_amount": str(target_amount),
        "due_date": str(due_date or ""),
        "n_deposits": str(n),
    }]))

    # deposits (preserva marcações anteriores)
    dep = _read_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
    existing = {}
    if not dep.empty:
        dep["n"] = pd.to_numeric(dep.get("n", 0), errors="coerce").fillna(0).astype(int)
        dep["done"] = pd.to_numeric(dep.get("done", 0), errors="coerce").fillna(0).astype(int)
        existing = dict(zip(dep["n"], dep["done"]))

    new_dep = pd.DataFrame([{"n": str(i), "done": str(existing.get(i, 0))} for i in range(1, n + 1)])
    _rewrite_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS, new_dep)

    # overrides (mantém só até n)
    ov = _read_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES)
    if not ov.empty:
        ov["n"] = pd.to_numeric(ov.get("n", 0), errors="coerce").fillna(0).astype(int)
        ov = ov[ov["n"] <= n]
    _rewrite_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES, ov)

    # link (mantém só até n)
    lk = _read_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)
    if not lk.empty:
        lk["n"] = pd.to_numeric(lk.get("n", 0), errors="coerce").fillna(0).astype(int)
        lk = lk[lk["n"] <= n]
    _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, lk)


def get_savings_goal_v2():
    df = _read_tab(TAB_SAVINGS_GOAL, H_SAVINGS_GOAL)
    if df.empty:
        return None, None, None
    row = df[df.get("id", "") == "1"]
    if row.empty:
        return None, None, None
    r = row.iloc[0]
    t = str(r.get("target_amount", "")).strip()
    d = str(r.get("due_date", "")).strip()
    n = str(r.get("n_deposits", "")).strip()
    target = float(t) if t else None
    due = d or None
    ndeps = int(float(n)) if n else None
    return target, due, ndeps


def fetch_savings_deposits_v2_with_amount() -> pd.DataFrame:
    dep = _read_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
    ov = _read_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES)

    if dep.empty:
        return pd.DataFrame(columns=["n", "done", "amount"])

    dep["n"] = pd.to_numeric(dep.get("n", 0), errors="coerce").fillna(0).astype(int)
    dep["done"] = pd.to_numeric(dep.get("done", 0), errors="coerce").fillna(0).astype(int)

    if ov.empty:
        dep["amount"] = dep["n"].astype(float)
        return dep[["n", "done", "amount"]].sort_values("n")

    ov["n"] = pd.to_numeric(ov.get("n", 0), errors="coerce").fillna(0).astype(int)
    ov["amount"] = pd.to_numeric(ov.get("amount", 0), errors="coerce").fillna(0.0)

    merged = dep.merge(ov, on="n", how="left")
    merged["amount"] = merged["amount"].fillna(merged["n"].astype(float))
    return merged[["n", "done", "amount"]].sort_values("n")


def toggle_savings_deposit_v2(n: int, done: bool):
    n = int(n)
    df = _read_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
    if df.empty:
        return
    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)
    mask = df["n"] == n
    if not mask.any():
        return
    df.loc[mask, "done"] = "1" if done else "0"
    _rewrite_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS, df)


def set_savings_override_v2(n: int, amount: float | None):
    n = int(n)
    df = _read_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES)
    if df.empty:
        df = pd.DataFrame(columns=H_SAVINGS_OVERRIDES)

    df["n"] = pd.to_numeric(df.get("n", 0), errors="coerce").fillna(0).astype(int)

    if amount is None:
        df = df[df["n"] != n]
    else:
        amount = float(amount)
        if (df["n"] == n).any():
            df.loc[df["n"] == n, "amount"] = str(amount)
        else:
            df = pd.concat([df, pd.DataFrame([{"n": str(n), "amount": str(amount)}])], ignore_index=True)

    _rewrite_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES, df)


def reset_savings_marks_v2():
    df = _read_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS)
    if df.empty:
        return
    df["done"] = "0"
    _rewrite_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS, df)

    lk = pd.DataFrame(columns=H_SAVINGS_TX_LINK)
    _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, lk)


def clear_savings_goal_v2():
    _rewrite_tab(TAB_SAVINGS_GOAL, H_SAVINGS_GOAL, pd.DataFrame([{"id":"1","target_amount":"","due_date":"","n_deposits":""}]))
    _rewrite_tab(TAB_SAVINGS_DEPOSITS, H_SAVINGS_DEPOSITS, pd.DataFrame(columns=H_SAVINGS_DEPOSITS))
    _rewrite_tab(TAB_SAVINGS_OVERRIDES, H_SAVINGS_OVERRIDES, pd.DataFrame(columns=H_SAVINGS_OVERRIDES))
    _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, pd.DataFrame(columns=H_SAVINGS_TX_LINK))


def create_desafio_transaction(date_: str, n: int, amount: float):
    # cria entrada em transactions e grava link n -> tx_id
    n = int(n)
    lk = _read_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)
    if not lk.empty:
        lk["n"] = pd.to_numeric(lk.get("n", 0), errors="coerce").fillna(0).astype(int)
        row = lk[lk["n"] == n]
        if not row.empty:
            tx_id = str(row.iloc[0].get("tx_id", "")).strip()
            if tx_id:
                return int(float(tx_id))

    add_transaction(
        date_=str(date_),
        description=f"Desafio - Depósito #{n}",
        ttype="entrada",
        amount=float(amount),
        category="Desafio",
        paid=1,
    )

    df_tx = fetch_transactions(None, None)
    tx_id = int(df_tx["id"].max()) if not df_tx.empty else 1

    if lk.empty:
        lk = pd.DataFrame(columns=H_SAVINGS_TX_LINK)
    lk = pd.concat([lk, pd.DataFrame([{"n": str(n), "tx_id": str(tx_id)}])], ignore_index=True)
    _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, lk)
    return tx_id


def delete_desafio_transaction(n: int):
    n = int(n)
    lk = _read_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK)
    if lk.empty:
        return
    lk["n"] = pd.to_numeric(lk.get("n", 0), errors="coerce").fillna(0).astype(int)
    row = lk[lk["n"] == n]
    if row.empty:
        return
    tx_id = str(row.iloc[0].get("tx_id", "")).strip()
    if tx_id:
        delete_transaction(int(float(tx_id)))
    lk = lk[lk["n"] != n]
    _rewrite_tab(TAB_SAVINGS_TX_LINK, H_SAVINGS_TX_LINK, lk)
