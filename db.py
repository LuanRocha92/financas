# db.py
import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd

DB_PATH = Path(__file__).resolve().parent / "finance.db"

def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn

def init_db():
    conn = _conn()
    cur = conn.cursor()

    # -----------------------------------------
    # LANÇAMENTOS
    # -----------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                -- YYYY-MM-DD
            description TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('entrada','saida')),
            amount REAL NOT NULL CHECK(amount >= 0),
            category TEXT NOT NULL,
            paid INTEGER NOT NULL CHECK(paid IN (0,1)),
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_paid ON transactions(paid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(type)")

    # -----------------------------------------
    # DESAFIO (v2)
    # -----------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS savings_goal_v2 (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            target_amount REAL,
            due_date TEXT,
            n_deposits INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS savings_deposits_v2 (
            n INTEGER PRIMARY KEY,
            done INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS savings_overrides_v2 (
            n INTEGER PRIMARY KEY,
            amount REAL NOT NULL
        )
    """)
    cur.execute("""
        INSERT OR IGNORE INTO savings_goal_v2 (id, target_amount, due_date, n_deposits)
        VALUES (1, NULL, NULL, NULL)
    """)

    # link opcional: depósito do desafio -> lançamento criado
    cur.execute("""
        CREATE TABLE IF NOT EXISTS savings_tx_link_v2 (
            n INTEGER PRIMARY KEY,
            tx_id INTEGER NOT NULL
        )
    """)

    # -----------------------------------------
    # AJUSTES MANUAIS DO FLUXO (simulação)
    # -----------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cashflow_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,         -- YYYY-MM-DD
            valor REAL NOT NULL,        -- valor positivo (tratado como saída no cálculo)
            descricao TEXT,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_adj_data ON cashflow_adjustments(data)")

    # -----------------------------------------
    # MAPA DE DÍVIDAS
    # -----------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            credor TEXT NOT NULL,
            descricao TEXT,
            valor REAL NOT NULL,
            vencimento TEXT,                 -- YYYY-MM-DD (opcional)
            prioridade INTEGER NOT NULL DEFAULT 1,
            quitada INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_debts_quitada ON debts(quitada)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_debts_prioridade ON debts(prioridade)")

    # -----------------------------------------
    # BLOCO DE NOTAS
    # -----------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT,
            texto TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_updated ON notes(updated_at)")

    conn.commit()
    conn.close()

# -----------------------------------------
# TRANSACTIONS
# -----------------------------------------
def add_transaction(date_: str, description: str, ttype: str, amount: float, category: str, paid: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO transactions (date, description, type, amount, category, paid, created_at)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            str(date_),
            str(description).strip(),
            str(ttype).strip().lower(),
            float(amount),
            str(category).strip() if str(category).strip() else "Outros",
            int(paid),
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()

def fetch_transactions(date_start: str | None = None, date_end: str | None = None) -> pd.DataFrame:
    conn = _conn()
    q = """
        SELECT id, date, description, type, amount, category, paid
        FROM transactions
        WHERE 1=1
    """
    params = []
    if date_start:
        q += " AND date >= ?"
        params.append(str(date_start))
    if date_end:
        q += " AND date <= ?"
        params.append(str(date_end))

    q += " ORDER BY date DESC, id DESC"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["id","date","description","type","amount","category","paid"])

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["paid"] = pd.to_numeric(df["paid"], errors="coerce").fillna(0).astype(int)
    df["type"] = df["type"].astype(str).str.strip().str.lower()
    df["category"] = df["category"].astype(str).fillna("Outros")
    return df

def delete_transaction(tx_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE id=?", (int(tx_id),))
    conn.commit()
    conn.close()

def update_transactions_bulk(df_updates: pd.DataFrame):
    if df_updates is None or df_updates.empty:
        return

    conn = _conn()
    cur = conn.cursor()

    for _, r in df_updates.iterrows():
        cur.execute(
            """
            UPDATE transactions
            SET date=?, description=?, type=?, amount=?, category=?, paid=?
            WHERE id=?
            """,
            (
                str(r["date"]),
                str(r["description"]).strip(),
                str(r["type"]).strip().lower(),
                float(r["amount"]),
                str(r["category"]).strip() if str(r["category"]).strip() else "Outros",
                int(r["paid"]),
                int(r["id"]),
            ),
        )

    conn.commit()
    conn.close()

# -----------------------------------------
# AJUSTES DO FLUXO (SIMULAÇÃO)
# -----------------------------------------
def add_cashflow_adjustment(data: str, valor: float, descricao: str | None = None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cashflow_adjustments (data, valor, descricao, created_at)
        VALUES (?,?,?,?)
        """,
        (str(data), float(valor), (descricao or "").strip(), datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()

def fetch_cashflow_adjustments(date_start: str, date_end: str) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT id, data, valor, descricao
        FROM cashflow_adjustments
        WHERE data >= ? AND data <= ?
        ORDER BY data ASC, id ASC
        """,
        conn,
        params=[str(date_start), str(date_end)],
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["id","data","valor","descricao"])

    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    return df

def delete_cashflow_adjustment(adj_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM cashflow_adjustments WHERE id=?", (int(adj_id),))
    conn.commit()
    conn.close()

# -----------------------------------------
# DÍVIDAS
# -----------------------------------------
def add_debt(credor: str, descricao: str, valor: float, vencimento: str | None, prioridade: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO debts (credor, descricao, valor, vencimento, prioridade, quitada, created_at)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            credor.strip(),
            (descricao or "").strip(),
            float(valor),
            None if not vencimento else str(vencimento),
            int(prioridade),
            0,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()

def fetch_debts(show_quitadas: bool = False) -> pd.DataFrame:
    conn = _conn()
    q = """
        SELECT id, credor, descricao, valor, vencimento, prioridade, quitada, created_at
        FROM debts
    """
    if not show_quitadas:
        q += " WHERE quitada = 0"
    q += " ORDER BY prioridade ASC, COALESCE(vencimento,'9999-12-31') ASC, id DESC"
    df = pd.read_sql_query(q, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["id","credor","descricao","valor","vencimento","prioridade","quitada","created_at"])

    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    df["quitada"] = pd.to_numeric(df["quitada"], errors="coerce").fillna(0).astype(int)
    return df

def mark_debt_paid(debt_id: int, paid: bool):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE debts SET quitada=? WHERE id=?", (1 if paid else 0, int(debt_id)))
    conn.commit()
    conn.close()

def delete_debt(debt_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM debts WHERE id=?", (int(debt_id),))
    conn.commit()
    conn.close()

# -----------------------------------------
# DESAFIO (v2) - COMPLETO (pra não quebrar seu desafio.py)
# -----------------------------------------
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

    conn = _conn()
    cur = conn.cursor()

    cur.execute(
        "UPDATE savings_goal_v2 SET target_amount=?, due_date=?, n_deposits=? WHERE id=1",
        (target_amount, due_date, n),
    )

    cur.execute("SELECT n, done FROM savings_deposits_v2")
    existing = {int(r["n"]): int(r["done"]) for r in cur.fetchall()}

    cur.execute("DELETE FROM savings_deposits_v2 WHERE n > ?", (n,))
    cur.execute("DELETE FROM savings_overrides_v2 WHERE n > ?", (n,))
    cur.execute("DELETE FROM savings_tx_link_v2 WHERE n > ?", (n,))

    for i in range(1, n + 1):
        if i not in existing:
            cur.execute("INSERT INTO savings_deposits_v2 (n, done) VALUES (?, 0)", (i,))

    conn.commit()
    conn.close()

def get_savings_goal_v2():
    conn = _conn()
    row = conn.execute(
        "SELECT target_amount, due_date, n_deposits FROM savings_goal_v2 WHERE id=1"
    ).fetchone()
    conn.close()
    if not row:
        return None, None, None
    return row["target_amount"], row["due_date"], row["n_deposits"]

def fetch_savings_deposits_v2_with_amount() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query("""
        SELECT
          d.n,
          d.done,
          COALESCE(o.amount, d.n) AS amount
        FROM savings_deposits_v2 d
        LEFT JOIN savings_overrides_v2 o ON o.n = d.n
        ORDER BY d.n
    """, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["n","done","amount"])

    df["done"] = pd.to_numeric(df["done"], errors="coerce").fillna(0).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    return df

def toggle_savings_deposit_v2(n: int, done: bool):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE savings_deposits_v2 SET done=? WHERE n=?", (1 if done else 0, int(n)))
    conn.commit()
    conn.close()

def set_savings_override_v2(n: int, amount: float | None):
    conn = _conn()
    cur = conn.cursor()
    if amount is None:
        cur.execute("DELETE FROM savings_overrides_v2 WHERE n=?", (int(n),))
    else:
        cur.execute(
            """
            INSERT INTO savings_overrides_v2 (n, amount) VALUES (?, ?)
            ON CONFLICT(n) DO UPDATE SET amount=excluded.amount
            """,
            (int(n), float(amount)),
        )
    conn.commit()
    conn.close()

def reset_savings_marks_v2():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE savings_deposits_v2 SET done=0")
    cur.execute("DELETE FROM savings_tx_link_v2")
    conn.commit()
    conn.close()

def clear_savings_goal_v2():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE savings_goal_v2 SET target_amount=NULL, due_date=NULL, n_deposits=NULL WHERE id=1")
    cur.execute("DELETE FROM savings_deposits_v2")
    cur.execute("DELETE FROM savings_overrides_v2")
    cur.execute("DELETE FROM savings_tx_link_v2")
    conn.commit()
    conn.close()

def create_desafio_transaction(date_: str, n: int, amount: float):
    conn = _conn()
    cur = conn.cursor()

    row = cur.execute("SELECT tx_id FROM savings_tx_link_v2 WHERE n=?", (int(n),)).fetchone()
    if row:
        conn.close()
        return int(row["tx_id"])

    cur.execute(
        """
        INSERT INTO transactions (date, description, type, amount, category, paid, created_at)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            str(date_),
            f"Desafio - Depósito #{int(n)}",
            "entrada",
            float(amount),
            "Desafio",
            1,
            datetime.utcnow().isoformat(),
        ),
    )
    tx_id = cur.lastrowid

    cur.execute(
        "INSERT OR REPLACE INTO savings_tx_link_v2 (n, tx_id) VALUES (?,?)",
        (int(n), int(tx_id)),
    )

    conn.commit()
    conn.close()
    return int(tx_id)

def delete_desafio_transaction(n: int):
    conn = _conn()
    cur = conn.cursor()

    row = cur.execute("SELECT tx_id FROM savings_tx_link_v2 WHERE n=?", (int(n),)).fetchone()
    if not row:
        conn.close()
        return

    tx_id = int(row["tx_id"])
    cur.execute("DELETE FROM transactions WHERE id=?", (int(tx_id),))
    cur.execute("DELETE FROM savings_tx_link_v2 WHERE n=?", (int(n),))

    conn.commit()
    conn.close()

# -----------------------------------------
# BLOCO DE NOTAS
# -----------------------------------------
def add_note(titulo: str, texto: str):
    conn = _conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT INTO notes (titulo, texto, created_at, updated_at)
        VALUES (?,?,?,?)
        """,
        (str(titulo or "").strip(), str(texto or "").strip(), now, now),
    )
    conn.commit()
    conn.close()

def fetch_notes() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT id, titulo, texto, created_at, updated_at
        FROM notes
        ORDER BY updated_at DESC, id DESC
        """,
        conn,
    )
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["id","titulo","texto","created_at","updated_at"])

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
    return df

def update_note(note_id: int, titulo: str, texto: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE notes
        SET titulo=?, texto=?, updated_at=?
        WHERE id=?
        """,
        (str(titulo or "").strip(), str(texto or "").strip(), datetime.utcnow().isoformat(), int(note_id)),
    )
    conn.commit()
    conn.close()

def delete_note(note_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM notes WHERE id=?", (int(note_id),))
    conn.commit()
    conn.close()
