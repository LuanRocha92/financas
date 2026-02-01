import pandas as pd
from datetime import date, timedelta

def fmt_brl(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def build_cashflow(df_tx: pd.DataFrame, start: date, end_with_projection: date, only_paid: bool) -> pd.DataFrame:
    """
    Gera um DF diário com:
    data | entrada | saida | saldo_dia | saldo_acumulado
    """
    if df_tx is None or df_tx.empty:
        df_tx = pd.DataFrame(columns=["date","type","amount","paid"])

    df = df_tx.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["type"] = df["type"].astype(str).str.strip().str.lower()
    df["paid"] = pd.to_numeric(df["paid"], errors="coerce").fillna(0).astype(int)

    if only_paid:
        df = df[df["paid"] == 1]

    days = pd.date_range(start=start, end=end_with_projection, freq="D").date
    out = pd.DataFrame({"data": days})

    if df.empty:
        out["entrada"] = 0.0
        out["saida"] = 0.0
        out["saldo_dia"] = 0.0
        out["saldo_acumulado"] = 0.0
        return out

    piv = (
        df.pivot_table(index="date", columns="type", values="amount", aggfunc="sum", fill_value=0.0)
        .reset_index()
        .rename(columns={"date": "data"})
    )

    if "entrada" not in piv.columns:
        piv["entrada"] = 0.0
    if "saida" not in piv.columns:
        piv["saida"] = 0.0

    out = out.merge(piv[["data","entrada","saida"]], on="data", how="left")
    out["entrada"] = out["entrada"].fillna(0.0)
    out["saida"] = out["saida"].fillna(0.0)
    out["saldo_dia"] = out["entrada"] - out["saida"]
    out["saldo_acumulado"] = out["saldo_dia"].cumsum()
    return out
