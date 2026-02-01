import streamlit as st
import pandas as pd
import altair as alt
import io
import zipfile
from pathlib import Path
from datetime import date, timedelta

from db import (
    init_db,
    add_transaction,
    fetch_transactions,
    delete_transaction,
    update_transactions_bulk,
)

from utils import build_cashflow, fmt_brl
from desafio import render_desafio

# =====================================================
# CONFIGURAÇÃO GERAL
# =====================================================
st.set_page_config(
    page_title="Finanças Pessoais",
    page_icon="💰",
    layout="wide"
)

init_db()

# =====================================================
# TEMA / ESTILO (REFINADO)
# =====================================================
alt.themes.register(
    "refinado",
    lambda: {
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelFont": "Inter",
                "titleFont": "Inter",
                "gridColor": "#E9ECEF",
                "tickColor": "#E9ECEF",
                "labelColor": "#343A40",
                "titleColor": "#343A40",
            },
            "legend": {"labelFont": "Inter", "titleFont": "Inter"},
            "title": {"font": "Inter", "color": "#212529"},
        }
    },
)
alt.themes.enable("refinado")

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
      [data-testid="stMetricValue"] { font-size: 1.6rem; }
    </style>
    """,
    unsafe_allow_html=True
)

# =====================================================
# MENU LATERAL
# =====================================================
st.sidebar.title("📌 Menu")

pagina = st.sidebar.radio(
    "Ir para:",
    ["🏠 Visão Geral", "🧾 Lançamentos", "📆 Fluxo de Caixa", "🎯 Desafio"],
    index=0
)

st.sidebar.markdown("---")

# =====================================================
# BACKUP / RESTAURAÇÃO (finance.db)
# =====================================================
DB_PATH = Path(__file__).resolve().parent / "finance.db"

st.sidebar.markdown("## 💾 Backup")

# --------- Download do backup (ZIP) ----------
def _make_backup_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if DB_PATH.exists():
            z.writestr("finance.db", DB_PATH.read_bytes())
    buf.seek(0)
    return buf.getvalue()

if DB_PATH.exists():
    st.sidebar.download_button(
        label="📥 Baixar backup (ZIP)",
        data=_make_backup_zip(),
        file_name="backup_financas.zip",
        mime="application/zip",
        use_container_width=True,
    )
else:
    st.sidebar.info("Banco ainda não existe. Crie algum lançamento e depois faça backup.")

st.sidebar.caption("Backup = seu banco de dados (finance.db).")

# --------- Upload para restaurar ----------
st.sidebar.markdown("### 📤 Restaurar backup")

up = st.sidebar.file_uploader(
    "Anexar backup (.zip ou .db)",
    type=["zip", "db"],
    accept_multiple_files=False,
)

def _restore_db_from_upload(uploaded_file) -> tuple[bool, str]:
    if uploaded_file is None:
        return False, "Nenhum arquivo enviado."

    name = (uploaded_file.name or "").lower()
    raw = uploaded_file.getvalue()

    try:
        if name.endswith(".db"):
            db_bytes = raw
        elif name.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(raw), "r") as z:
                # procura finance.db dentro do zip
                candidates = [n for n in z.namelist() if n.lower().endswith("finance.db")]
                if not candidates:
                    return False, "ZIP não contém um arquivo 'finance.db'."
                db_bytes = z.read(candidates[0])
        else:
            return False, "Formato inválido. Envie .db ou .zip."
    except Exception as e:
        return False, f"Falha ao ler o backup: {e}"

    if not db_bytes or len(db_bytes) < 100:
        return False, "Arquivo de banco parece inválido (muito pequeno)."

    # escreve o banco novo no lugar
    try:
        # (opcional) cria um backup local automático antes de substituir
        if DB_PATH.exists():
            old = DB_PATH.read_bytes()
            (DB_PATH.parent / "finance_old_auto_backup.db").write_bytes(old)

        DB_PATH.write_bytes(db_bytes)
        return True, "Backup restaurado com sucesso!"
    except Exception as e:
        return False, f"Falha ao salvar o banco: {e}"

if up is not None:
    st.sidebar.warning("A restauração substitui o banco atual. Eu faço uma cópia local: finance_old_auto_backup.db")
    if st.sidebar.button("✅ Restaurar agora", type="primary", use_container_width=True):
        ok, msg = _restore_db_from_upload(up)
        if ok:
            st.sidebar.success(msg)
            st.rerun()
        else:
            st.sidebar.error(msg)


st.sidebar.caption("• Simples • Funcional")

# =====================================================
# HELPERS
# =====================================================
def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

# =====================================================
# 🏠 VISÃO GERAL
# =====================================================
if pagina == "🏠 Visão Geral":
    st.title("💰 Visão Geral")

    c1, c2 = st.columns(2)
    with c1:
        inicio = st.date_input("Início", value=date.today() - timedelta(days=30))
    with c2:
        fim = st.date_input("Fim", value=date.today())

    if inicio > fim:
        inicio, fim = fim, inicio

    only_paid = st.toggle("Modo real (somente pagos)", value=False)

    df = fetch_transactions(str(inicio), str(fim))
    if only_paid:
        df = df[df["paid"] == 1]

    entradas = df.loc[df["type"] == "entrada", "amount"].sum() if not df.empty else 0
    saidas = df.loc[df["type"] == "saida", "amount"].sum() if not df.empty else 0
    saldo = entradas - saidas

    k1, k2, k3 = st.columns(3)
    k1.metric("Entradas", fmt_brl(entradas))
    k2.metric("Saídas", fmt_brl(saidas))
    k3.metric("Saldo", fmt_brl(saldo))

    st.divider()

    st.subheader("📌 Gastos por categoria")

    gastos = df[df["type"] == "saida"]
    if gastos.empty:
        st.info("Sem gastos no período.")
    else:
        cat = gastos.groupby("category")["amount"].sum().reset_index()
        cat["valor_fmt"] = cat["amount"].apply(fmt_brl)

        chart = (
            alt.Chart(cat)
            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
            .encode(
                x=alt.X("amount:Q", title="Valor (R$)"),
                y=alt.Y("category:N", sort="-x", title="Categoria"),
                tooltip=[
                    alt.Tooltip("category:N", title="Categoria"),
                    alt.Tooltip("valor_fmt:N", title="Valor"),
                ],
            )
            .properties(height=360)
        )

        st.altair_chart(chart, use_container_width=True)

# =====================================================
# 🧾 LANÇAMENTOS
# =====================================================
elif pagina == "🧾 Lançamentos":
    st.title("🧾 Lançamentos")

    with st.expander("➕ Novo lançamento", expanded=True):
        c1, c2, c3, c4 = st.columns([1.2, 2.2, 1.2, 1.2])
        dt = c1.date_input("Data", value=date.today())
        desc = c2.text_input("Descrição")
        ttype = c3.selectbox("Tipo", ["saida", "entrada"])
        amount = c4.number_input("Valor", min_value=0.0, step=10.0)

        c5, c6 = st.columns([2, 1])
        cat = c5.text_input("Categoria", value="Outros")
        paid = c6.checkbox("Pago", value=True)

        if st.button("Salvar", type="primary"):
            if not desc.strip():
                st.error("Informe a descrição.")
            else:
                add_transaction(
                    date_=str(dt),
                    description=desc,
                    ttype=ttype,
                    amount=amount,
                    category=cat,
                    paid=1 if paid else 0,
                )
                st.success("Lançamento salvo.")
                st.rerun()

    st.divider()

    inicio = st.date_input("Início (filtro)", value=date.today() - timedelta(days=30))
    fim = st.date_input("Fim (filtro)", value=date.today())

    if inicio > fim:
        inicio, fim = fim, inicio

    df = fetch_transactions(str(inicio), str(fim))

    if df.empty:
        st.info("Sem lançamentos.")
    else:
        edit = df.copy()
        edit["paid"] = edit["paid"].map({1: True, 0: False})
        edit["date"] = pd.to_datetime(edit["date"]).dt.strftime("%Y-%m-%d")

        edited = st.data_editor(
            edit,
            hide_index=True,
            use_container_width=True,
            disabled=["id"],
            column_config={
                "type": st.column_config.SelectboxColumn("type", options=["entrada", "saida"]),
                "paid": st.column_config.CheckboxColumn("paid"),
            },
        )

        if st.button("Salvar edições", type="primary"):
            save = edited.copy()
            save["paid"] = save["paid"].apply(lambda x: 1 if x else 0)
            update_transactions_bulk(save)
            st.success("Edições salvas.")
            st.rerun()

        tx_id = st.number_input("ID para excluir", min_value=0, step=1)
        if st.button("Excluir", type="secondary"):
            delete_transaction(tx_id)
            st.success("Lançamento excluído.")
            st.rerun()

# =====================================================
# 📆 FLUXO DE CAIXA (+30 DIAS)
# =====================================================
elif pagina == "📆 Fluxo de Caixa":
    st.title("📆 Fluxo de Caixa (com projeção +30 dias)")

    c1, c2 = st.columns(2)
    with c1:
        inicio = st.date_input("Início", value=date.today().replace(day=1))
    with c2:
        fim = st.date_input("Fim", value=date.today())

    if inicio > fim:
        inicio, fim = fim, inicio

    only_paid = st.toggle("Modo real (somente pagos)", value=False)
    fim_proj = fim + timedelta(days=30)

    df_tx = fetch_transactions(str(inicio), str(fim_proj))
    df_cf = build_cashflow(df_tx, inicio, fim_proj, only_paid)

    if df_cf.empty:
        st.info("Sem dados.")
        st.stop()

    df_cf["data"] = pd.to_datetime(df_cf["data"])

    chart = (
        alt.Chart(df_cf)
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("data:T", title="Data"),
            y=alt.Y("saldo_acumulado:Q", title="Saldo acumulado (R$)"),
            color=alt.condition(
                alt.datum.saldo_acumulado >= 0,
                alt.value("#1f7a1f"),
                alt.value("#b00020"),
            ),
            tooltip=[
                alt.Tooltip("data:T", format="%d/%m/%Y", title="Data"),
                alt.Tooltip("saldo_acumulado:Q", title="Saldo"),
            ],
        )
        .properties(height=380)
    )

    st.altair_chart(chart, use_container_width=True)

    st.subheader("📋 Tabela diária")

    def style_posneg(v):
        v = _safe_float(v)
        return (
            "background-color: rgba(31,122,31,0.15); color:#1f7a1f; font-weight:700;"
            if v >= 0
            else "background-color: rgba(176,0,32,0.15); color:#b00020; font-weight:700;"
        )

    show = df_cf.copy()
    show["data"] = show["data"].dt.strftime("%d/%m/%Y")

    styled = (
        show.style
        .format({
            "entrada": fmt_brl,
            "saida": fmt_brl,
            "saldo_dia": fmt_brl,
            "saldo_acumulado": fmt_brl,
        })
        .map(style_posneg, subset=["saldo_dia", "saldo_acumulado"])
    )

    st.dataframe(styled, use_container_width=True, hide_index=True)

# =====================================================
# 🎯 DESAFIO
# =====================================================
elif pagina == "🎯 Desafio":
    render_desafio()
