import streamlit as st
import pandas as pd
import math
from datetime import date

from db import (
    init_db,
    set_savings_goal_v2,
    get_savings_goal_v2,
    fetch_savings_deposits_v2_with_amount,
    toggle_savings_deposit_v2,
    set_savings_override_v2,
    reset_savings_marks_v2,
    clear_savings_goal_v2,
)

def fmt(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def render_desafio():
    init_db()

    st.title("🎯 Desafio (depósitos 1..N)")
    st.caption("Crie uma meta e marque depósitos. Clica e fica verde na hora ✅")

    target_amount, due_date, n_deposits = get_savings_goal_v2()

    with st.expander("⚙️ Configurar meta", expanded=True):
        c1, c2, c3 = st.columns([1.2, 1.2, 1])
        with c1:
            meta_txt = st.text_input(
                "Meta (R$)",
                value="" if target_amount is None else str(
                    int(target_amount) if float(target_amount).is_integer() else target_amount
                ),
                placeholder="Ex: 5000 ou 1500,50"
            )
        with c2:
            prazo_default = None if due_date is None else date.fromisoformat(due_date)
            prazo = st.date_input("Data da meta", value=prazo_default)
        with c3:
            aplicar = st.button("Aplicar", type="primary")

        if aplicar:
            meta_clean = meta_txt.strip().replace(".", "").replace(",", ".")
            if meta_clean == "":
                st.error("Informe uma meta (ex: 5000).")
                st.stop()

            try:
                meta_val = float(meta_clean)
                if meta_val <= 0:
                    st.error("A meta precisa ser maior que zero.")
                    st.stop()
            except Exception:
                st.error("Meta inválida. Ex: 5000 ou 1500,50")
                st.stop()

            prazo_str = None if prazo is None else prazo.isoformat()
            set_savings_goal_v2(meta_val, prazo_str)
            st.success("Desafio criado/atualizado!")
            st.rerun()

    target_amount, due_date, n_deposits = get_savings_goal_v2()
    if target_amount is None or n_deposits is None:
        st.info("Defina uma meta acima para gerar automaticamente os depósitos (1..N).")
        return

    df = fetch_savings_deposits_v2_with_amount()
    df["done"] = df["done"].astype(int)

    total_final = float(df["amount"].sum())
    guardado = float((df["amount"] * df["done"]).sum())
    falta = max(total_final - guardado, 0.0)
    progresso = guardado / total_final if total_final > 0 else 0.0

    st.subheader("📌 Resumo")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Meta informada", fmt(float(target_amount)))
    k2.metric("Total do desafio", fmt(total_final))
    k3.metric("Guardado", fmt(guardado))
    k4.metric("Prazo", "—" if due_date is None else pd.to_datetime(due_date).strftime("%d/%m/%Y"))
    st.progress(progresso)
    st.caption(f"Falta: **{fmt(falta)}**")

    meta_val = float(target_amount)
    if abs(total_final - meta_val) >= 0.01:
        st.warning(
            f"Seu desafio tem **{n_deposits} depósitos** e aproxima sua meta de **{fmt(meta_val)}**. "
            f"No final, o total será **{fmt(total_final)}**."
        )

    st.divider()

    tab_visual, tab_edicao, tab_exclusao = st.tabs(["✅ Visual", "✏️ Edição", "🗑️ Exclusão / Reset"])

    with tab_visual:
        st.subheader("✅ Clique para marcar (fica verde na hora)")

        amount_map = dict(zip(df["n"].astype(int), df["amount"].astype(float)))
        done_map = dict(zip(df["n"].astype(int), df["done"].astype(int)))

        cols_per_row = 10 if n_deposits >= 80 else 8 if n_deposits >= 40 else 6
        rows = math.ceil(n_deposits / cols_per_row)

        # estado local (evita flicker e permite “marca na hora”)
        if "challenge_state" not in st.session_state or len(st.session_state.challenge_state) != n_deposits:
            st.session_state.challenge_state = {int(n): bool(done) for n, done in done_map.items()}

        changed = []
        for r in range(rows):
            cols = st.columns(cols_per_row)
            for c in range(cols_per_row):
                n = r * cols_per_row + (c + 1)
                if n > n_deposits:
                    break

                current = bool(st.session_state.challenge_state.get(n, False))
                amount = float(amount_map.get(n, n))
                label = f"R$ {int(amount):,}".replace(",", ".") if float(amount).is_integer() else fmt(amount)

                with cols[c]:
                    new_val = st.checkbox(label, value=current, key=f"chk_{n}")

                if new_val != current:
                    st.session_state.challenge_state[n] = new_val
                    changed.append((n, new_val))

        if changed:
            for n, new_val in changed:
                toggle_savings_deposit_v2(n, new_val)
            st.rerun()

        st.divider()
        st.subheader("📈 Evolução (sem datas)")

        marked = fetch_savings_deposits_v2_with_amount()
        marked = marked[marked["done"] == 1].sort_values("n")

        if marked.empty:
            st.info("Você ainda não marcou nenhum depósito.")
        else:
            marked["acumulado"] = marked["amount"].cumsum()
            marked["passo"] = range(1, len(marked) + 1)
            st.line_chart(marked.set_index("passo")[["acumulado"]], use_container_width=True)

    with tab_edicao:
        st.subheader("✏️ Editar valores dos depósitos")
        st.caption("Por padrão, o depósito N vale R$ N. Aqui você pode alterar qualquer valor (override).")

        edit = df[["n", "amount", "done"]].copy()
        edit["done"] = edit["done"].map({1: True, 0: False})

        edited = st.data_editor(
            edit,
            use_container_width=True,
            hide_index=True,
            disabled=["n", "done"],
            column_config={"amount": st.column_config.NumberColumn("amount", step=10.0, format="%.2f")},
        )

        if st.button("Salvar valores", type="primary"):
            for _, row in edited.iterrows():
                n = int(row["n"])
                amount = float(row["amount"])
                if abs(amount - float(n)) < 0.0001:
                    set_savings_override_v2(n, None)
                else:
                    set_savings_override_v2(n, amount)
            st.success("Valores atualizados!")
            st.rerun()

    with tab_exclusao:
        st.subheader("🗑️ Exclusão / Reset")
        st.warning("Cuidado: essas ações são permanentes.")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Resetar marcações (desmarcar tudo)", type="secondary"):
                reset_savings_marks_v2()
                st.success("Marcações resetadas.")
                st.rerun()

        with c2:
            if st.button("Apagar desafio (meta + depósitos)", type="secondary"):
                clear_savings_goal_v2()
                st.success("Desafio apagado. Configure uma nova meta.")
                st.rerun()
