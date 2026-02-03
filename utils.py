# utils.py
import pandas as pd

def fmt_brl(v) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def build_cashflow(
    df_tx: pd.DataFrame,
    start,
    end,
    only_paid: bool,
    df_adj: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Tabela diária:
    data | entrada | saida | ajuste | saldo_dia | saldo_acumulado

    - df_tx: lançamentos (date, type, amount, paid)
    - df_adj: ajustes manuais (data, valor) -> entram como SAÍDA (reduzem o saldo)
    """
    if df_tx is None or df_tx.empty:
        df_tx = pd.DataFrame(columns=["date", "type", "amount", "paid"])

    df = df_tx.copy()

    if only_paid and "paid" in df.columns:
        df = df[df["paid"] == 1]

    # normaliza
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        df["date"] = pd.NaT

    df["type"] = df.get("type", "").astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0)

    # calendário diário
    days = pd.date_range(start=start, end=end, freq="D")
    out = pd.DataFrame({"data": days.date})

    # soma entradas/saídas por dia
    if not df.empty:
        g_in = df[df["type"] == "entrada"].groupby("date")["amount"].sum()
        g_out = df[df["type"] == "saida"].groupby("date")["amount"].sum()
    else:
        g_in = pd.Series(dtype=float)
        g_out = pd.Series(dtype=float)

    out["entrada"] = out["data"].map(g_in).fillna(0.0)
    out["saida"] = out["data"].map(g_out).fillna(0.0)

    # ajustes manuais (sempre considerados como saída)
    if df_adj is None or df_adj.empty:
        out["ajuste"] = 0.0
    else:
        a = df_adj.copy()
        a["data"] = pd.to_datetime(a["data"], errors="coerce").dt.date
        a["valor"] = pd.to_numeric(a["valor"], errors="coerce").fillna(0.0)
        g_adj = a.groupby("data")["valor"].sum()
        out["ajuste"] = out["data"].map(g_adj).fillna(0.0)

    # saldo do dia e acumulado
    out["saldo_dia"] = out["entrada"] - out["saida"] - out["ajuste"]
    out["saldo_acumulado"] = out["saldo_dia"].cumsum()

    return out
