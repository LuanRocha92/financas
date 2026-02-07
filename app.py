# app.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta

from db import (
    init_db,
    ping_db,
    add_transaction, fetch_transactions, delete_transaction, update_transactions_bulk,
    add_cashflow_adjustment, fetch_cashflow_adjustments, delete_cashflow_adjustment,
    add_debt, fetch_debts, mark_debt_paid, delete_debt,
    add_note, fetch_notes, update_note, delete_note,
    fetch_savings_deposits_v2_with_amount,
)
from utils import build_cashflow, fmt_brl
from desafio import render_desafio


# -----------------------------------
# CONFIG INICIAL (UMA VEZ)
# -----------------------------------
st.set_page_config(page_title="Finanças", page_icon="💰", layout="wide")

# -----------------------------------
# INICIALIZA DB (GOOGLE SHEETS) - UMA VEZ
# -----------------------------------
try:
    init_db()
except Exception as e:
    st.error("❌ Erro ao inicializar o banco (Google Sheets)")
    st.code(str(e))
    st.stop()

ok, msg = ping_db()
if ok:
    st.sidebar.success("✅ Banco conectado (Google Sheets)")
else:
    st.sidebar.error("❌ Google Sheets NÃO conectou")
    st.sidebar.caption(msg)
    st.stop()


# -----------------------------------
# TEMA (ALTAIR)
# -----------------------------------
alt.themes.register(
    "refinado",
    lambda: {
        "config": {
            "view": {"stroke": None},
            "axis": {
                "gridColor": "#2a2f38",
                "labelColor": "#cbd5e1",
                "titleColor": "#e2e8f0",
            },
            "title": {"color": "#e2e8f0"},
            "legend": {"labelColor": "#e2e8f0", "titleColor": "#e2e8f0"},
        }
    },
)
alt.themes.enable("refinado")


# -----------------------------------
# DATA BASE
# -----------------------------------
def _first_day_of_month(d: date) -> date:
    return d.replace(day=1)

if "data_base" not in st.session_state:
    st.session_state.data_base = date.today()


# -----------------------------------
# MENU + DATA NA SIDEBAR
# -----------------------------------
st.sidebar.title("📌 Menu")
pagina = st.sidebar.radio(
    "Ir para:",
    ["💰 Visão Geral", "🧾 Lançamentos", "📆 Fluxo de Caixa", "📍 Mapa de Dívidas", "📝 Bloco de Notas", "🎯 Desafio"],
    index=0
)

st.sidebar.markdown("---")
st.sidebar.markdown("## 📅 Data")
data_base = st.sidebar.date_input("Data", st.session_state.data_base)
st.session_state.data_base = data_base

inicio = _first_day_of_month(data_base)
fim = data_base
st.sidebar.caption(f"{inicio.strftime('%d/%m/%Y')} - {fim.strftime('%d/%m/%Y')}")

# projeção do fluxo (+30 dias)
fim_fluxo = fim + timedelta(days=30)


# -----------------------------------
# HELPERS
# -----------------------------------
def _style_pos_neg(v: float):
    try:
        v = float(v)
    except Exception:
        v = 0.0
    if v < 0:
        return "color:#ff4d4f; font-weight:700;"
    return "color:#22c55e; font-weight:700;"


# -----------------------------------
# PÁGINAS
# -----------------------------------

# =========================
# 💰 VISÃO GERAL
# =========================
if pagina == "💰 Visão Geral":
    st.title("💰 Visão Geral")

    only_paid = st.toggle("Modo real (somente pagos)", value=False)

    df = fetch_transactions(str(inicio), str(fim))
    if only_paid and not df.empty:
        df = df[df["paid"] == 1]

    entradas = df.loc[df["type"] == "entrada", "amount"].sum() if not df.empty else 0.0
    saidas = df.loc[df["type"] == "saida", "amount"].sum() if not df.empty else 0.0
    saldo = entradas - saidas

    # --- resumo do desafio ---
    dep = fetch_savings_deposits_v2_with_amount()
    if dep is None or dep.empty:
        guardado = 0.0
        total_desafio = 0.0
    else:
        dep["done"] = pd.to_numeric(dep["done"], errors="coerce").fillna(0).astype(int)
        dep["amount"] = pd.to_numeric(dep["amount"], errors="coerce").fillna(0.0)
        guardado = float((dep["amount"] * dep["done"]).sum())
        total_desafio = float(dep["amount"].sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Entradas", fmt_brl(entradas))
    k2.metric("Saídas", fmt_brl(saidas))
    k3.metric("Saldo", fmt_brl(saldo))
    k4.metric("Investimento (Desafio)", fmt_brl(guardado))

    if total_desafio > 0:
        st.caption(f"Desafio: {fmt_brl(guardado)} guardado de {fmt_brl(total_desafio)}")

    st.divider()

    # --- próximos 7 dias ---
    st.subheader("📅 Próximos 7 dias (panorama)")
    start7 = fim
    end7 = fim + timedelta(days=7)

    tx7 = fetch_transactions(str(start7), str(end7))
    adj7 = fetch_cashflow_adjustments(str(start7), str(end7))
    cf7 = build_cashflow(tx7, start7, end7, only_paid=only_paid, df_adj=adj7)

    if cf7.empty:
        st.info("Sem dados pros próximos 7 dias.")
    else:
        in7 = float(cf7["entrada"].sum())
        out7 = float(cf7["saida"].sum())
        adj7t = float(cf7["ajuste"].sum())
        net7 = in7 - out7 - adj7t

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Entradas (7 dias)", fmt_brl(in7))
        a2.metric("Saídas (7 dias)", fmt_brl(out7))
        a3.metric("Ajustes (7 dias)", fmt_brl(adj7t))
        a4.metric("Saldo líquido (7 dias)", fmt_brl(net7))

    st.divider()
    st.subheader("📌 Gastos por categoria (período)")

    gastos = df[df["type"] == "saida"].copy() if not df.empty else pd.DataFrame()
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


# =========================
# 🧾 LANÇAMENTOS
# =========================
elif pagina == "🧾 Lançamentos":
    st.title("🧾 Lançamentos")

    with st.expander("➕ Novo lançamento", expanded=True):
        c1, c2, c3, c4 = st.columns([1.2, 2.2, 1.2, 1.2])
        dt = c1.date_input("Data", value=fim)
        desc = c2.text_input("Descrição", placeholder="Ex: Mercado, Internet, Cliente X...")
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
                    amount=float(amount),
                    category=cat,
                    paid=1 if paid else 0,
                )
                st.success("Lançamento salvo.")
                st.rerun()

    st.divider()

    df = fetch_transactions(str(inicio), str(fim))
    if df.empty:
        st.info("Sem lançamentos no período.")
    else:
        view = df.copy()
        view["date"] = pd.to_datetime(view["date"]).dt.strftime("%d/%m/%Y")
        view["paid"] = view["paid"].map({1: "Sim", 0: "Não"})
        view["amount"] = view["amount"].apply(fmt_brl)
        view.columns = ["ID", "Data", "Descrição", "Tipo", "Valor", "Categoria", "Pago"]
        st.dataframe(view, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("✏️ Editar (rápido)")

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
            save["paid"] = save["paid"].apply(lambda x: 1 if bool(x) else 0)
            update_transactions_bulk(save)
            st.success("Edições salvas.")
            st.rerun()

        st.divider()
        st.subheader("🗑️ Excluir lançamento")
        tx_id = st.number_input("ID para excluir", min_value=0, step=1, value=0)
        if st.button("Excluir", type="secondary"):
            if tx_id > 0:
                delete_transaction(int(tx_id))
                st.success("Excluído.")
                st.rerun()
            else:
                st.warning("Informe um ID válido.")


# =========================
# 📆 FLUXO DE CAIXA
# =========================
elif pagina == "📆 Fluxo de Caixa":
    st.title("📆 Fluxo de Caixa")

    only_paid = st.toggle("Modo real (somente pagos)", value=False)

    df_tx = fetch_transactions(str(inicio), str(fim_fluxo))
    df_adj = fetch_cashflow_adjustments(str(inicio), str(fim_fluxo))
    df_cf = build_cashflow(df_tx, inicio, fim_fluxo, only_paid=only_paid, df_adj=df_adj)

    if df_cf.empty:
        st.info("Sem dados para o período.")
        st.stop()

    tab_fluxo, tab_ajustes = st.tabs(["📋 Fluxo (tabela + gráfico)", "🧮 Ajustes manuais (simulação)"])

    with tab_fluxo:
        st.subheader("📋 Tabela diária (cores automáticas)")

        show = df_cf.copy()
        show["data"] = pd.to_datetime(show["data"], errors="coerce")
        show["Data"] = show["data"].dt.strftime("%d/%m/%Y")

        tab = show[["Data", "entrada", "saida", "ajuste", "saldo_dia", "saldo_acumulado"]].copy()
        tab.columns = ["Data", "Entrada", "Saída", "Ajuste (simulação)", "Saldo do dia", "Saldo acumulado"]

        styled = (
            tab.style
            .format({
                "Entrada": fmt_brl,
                "Saída": fmt_brl,
                "Ajuste (simulação)": fmt_brl,
                "Saldo do dia": fmt_brl,
                "Saldo acumulado": fmt_brl,
            })
            .map(lambda v: _style_pos_neg(v), subset=["Saldo do dia", "Saldo acumulado"])
        )

        st.dataframe(styled, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("📈 Gráfico (saldo acumulado)")

        plot = df_cf.copy()
        plot["data"] = pd.to_datetime(plot["data"])

        chart = (
            alt.Chart(plot)
            .mark_line(strokeWidth=3)
            .encode(
                x=alt.X("data:T", title="Data"),
                y=alt.Y("saldo_acumulado:Q", title="Saldo acumulado (R$)"),
                tooltip=[
                    alt.Tooltip("data:T", title="Data", format="%d/%m/%Y"),
                    alt.Tooltip("saldo_acumulado:Q", title="Saldo"),
                ],
            )
            .properties(height=360)
        )
        st.altair_chart(chart, use_container_width=True)

    with tab_ajustes:
        st.subheader("🧮 Ajustes manuais (simular gastos)")
        st.caption("Aqui você coloca um valor como uma SAÍDA simulada. Isso impacta o saldo do dia e os próximos dias.")

        c1, c2, c3 = st.columns([1, 1, 2])
        data_adj = c1.date_input("Data do ajuste", value=fim)
        valor_adj = c2.number_input("Valor (R$)", min_value=0.0, step=10.0)
        desc_adj = c3.text_input("Descrição", placeholder="Ex: simulação mercado / conserto / compra...")

        if st.button("Adicionar ajuste", type="primary"):
            if valor_adj <= 0:
                st.warning("Informe um valor maior que zero.")
            else:
                add_cashflow_adjustment(str(data_adj), float(valor_adj), desc_adj)
                st.success("Ajuste adicionado.")
                st.rerun()

        st.divider()
        st.subheader("📋 Ajustes cadastrados (+30 dias)")

        adj = fetch_cashflow_adjustments(str(inicio), str(fim_fluxo))
        if adj.empty:
            st.info("Sem ajustes no período.")
        else:
            view = adj.copy()
            view["data"] = pd.to_datetime(view["data"]).dt.strftime("%d/%m/%Y")
            view["valor"] = view["valor"].apply(fmt_brl)
            view.columns = ["ID", "Data", "Valor", "Descrição"]
            st.dataframe(view, use_container_width=True, hide_index=True)

            del_id = st.number_input("ID do ajuste para excluir", min_value=0, step=1, value=0)
            if st.button("Excluir ajuste", type="secondary"):
                if del_id > 0:
                    delete_cashflow_adjustment(int(del_id))
                    st.success("Ajuste excluído.")
                    st.rerun()
                else:
                    st.warning("Informe um ID válido.")


# =========================
# 📍 MAPA DE DÍVIDAS
# =========================
elif pagina == "📍 Mapa de Dívidas":
    st.title("📍 Mapa de Dívidas")
    st.caption("Dívidas que você quer quitar na primeira oportunidade.")

    with st.expander("➕ Nova dívida", expanded=True):
        c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
        credor = c1.text_input("Credor", placeholder="Ex: Cartão, Banco, Pessoa...")
        descricao = c2.text_input("Descrição", placeholder="Ex: parcela 3/5, empréstimo...")
        valor = c3.number_input("Valor (R$)", min_value=0.0, step=50.0)
        prioridade = c4.selectbox("Prioridade", [1, 2, 3, 4, 5], index=0)

        tem_venc = st.checkbox("Tem vencimento?", value=False)
        venc = None
        if tem_venc:
            venc = st.date_input("Vencimento", value=fim)

        if st.button("Salvar dívida", type="primary"):
            if not credor.strip():
                st.error("Informe o credor.")
            elif valor <= 0:
                st.error("Informe um valor maior que zero.")
            else:
                venc_str = None if venc is None else str(venc)
                add_debt(credor, descricao, float(valor), venc_str, int(prioridade))
                st.success("Dívida cadastrada.")
                st.rerun()

    st.divider()

    show_quitadas = st.toggle("Mostrar dívidas quitadas", value=False)
    df = fetch_debts(show_quitadas=show_quitadas)

    if df.empty:
        st.info("Nenhuma dívida cadastrada.")
        st.stop()

    total_aberto = df[df["quitada"] == 0]["valor"].sum() if not df.empty else 0.0
    st.metric("Total em dívidas (abertas)", fmt_brl(total_aberto))

    st.subheader("📋 Lista")
    view = df.copy()
    view["vencimento"] = pd.to_datetime(view["vencimento"], errors="coerce").dt.strftime("%d/%m/%Y")
    view["vencimento"] = view["vencimento"].fillna("—")
    view["valor"] = view["valor"].apply(fmt_brl)
    view["quitada"] = view["quitada"].map({0: "Não", 1: "Sim"})
    view = view[["id", "credor", "descricao", "valor", "vencimento", "prioridade", "quitada"]]
    view.columns = ["ID", "Credor", "Descrição", "Valor", "Vencimento", "Prioridade", "Quitada"]
    st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("✅ Quitar dívida (vira lançamento)")

    debt_id = st.number_input("ID da dívida", min_value=0, step=1, value=0)
    if st.button("Quitar agora", type="primary"):
        if debt_id <= 0:
            st.warning("Informe um ID válido.")
        else:
            row = df[df["id"] == debt_id]
            if row.empty:
                st.error("ID não encontrado.")
            else:
                r = row.iloc[0]
                add_transaction(
                    date_=str(fim),
                    description=f"Quitar dívida - {r['credor']} ({r['descricao']})".strip(),
                    ttype="saida",
                    amount=float(r["valor"]),
                    category="Dívidas",
                    paid=1
                )
                mark_debt_paid(debt_id, True)
                st.success("Dívida quitada e registrada como SAÍDA.")
                st.rerun()

    st.subheader("🗑️ Excluir dívida")
    del_id = st.number_input("ID para excluir", min_value=0, step=1, value=0, key="del_debt")
    if st.button("Excluir dívida", type="secondary"):
        if del_id > 0:
            delete_debt(int(del_id))
            st.success("Excluída.")
            st.rerun()
        else:
            st.warning("Informe um ID válido.")


# =========================
# 📝 BLOCO DE NOTAS
# =========================
elif pagina == "📝 Bloco de Notas":
    st.title("📝 Bloco de Notas")
    st.caption("Anotações rápidas.")

    with st.expander("➕ Nova nota", expanded=True):
        titulo = st.text_input("Título", placeholder="Ex: metas do mês, compras, lembretes...")
        texto = st.text_area("Conteúdo", placeholder="Escreve aqui...", height=160)

        if st.button("Salvar nota", type="primary"):
            if not titulo.strip() and not texto.strip():
                st.warning("Escreve pelo menos um título ou conteúdo.")
            else:
                add_note(titulo=titulo, texto=texto)
                st.success("Nota salva.")
                st.rerun()

    st.divider()
    st.subheader("📋 Suas notas")

    notes = fetch_notes()
    if notes.empty:
        st.info("Nenhuma nota ainda.")
    else:
        edit = notes.copy()
        edit.columns = ["ID", "Título", "Conteúdo", "Criada em", "Atualizada em"]

        edited = st.data_editor(
            edit,
            use_container_width=True,
            hide_index=True,
            disabled=["ID", "Criada em", "Atualizada em"],
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Salvar edições", type="primary"):
                for _, r in edited.iterrows():
                    update_note(int(r["ID"]), str(r["Título"]), str(r["Conteúdo"]))
                st.success("Notas atualizadas.")
                st.rerun()

        with c2:
            del_note_id = st.number_input("ID para excluir", min_value=0, step=1, value=0)
            if st.button("Excluir nota", type="secondary"):
                if del_note_id > 0:
                    delete_note(int(del_note_id))
                    st.success("Nota excluída.")
                    st.rerun()
                else:
                    st.warning("Informe um ID válido.")


# =========================
# 🎯 DESAFIO
# =========================
elif pagina == "🎯 Desafio":
    render_desafio(data_padrao=fim)


# -----------------------------------
# DEBUG (NO FINAL DA SIDEBAR, PRA NAO FICAR FEIO)
# -----------------------------------
with st.sidebar.expander("🛠️ Debug (Secrets)", expanded=False):
    st.write("🔎 Secrets disponíveis:", list(st.secrets.keys()))
    st.write("Keys do secrets:", list(st.secrets.keys()))
