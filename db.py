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

    # -----------------------------
    # LANÇAMENTOS (simples)
    # -----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                     -- YYYY-MM-DD
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

    # -----------------------------
    # DESAFIO (v2) - META + DEPÓSITOS
    # -----------------------------
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

    conn.commit()
    conn.close()

# -----------------------------
# LANÇAMENTOS
# -----------------------------
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

# -----------------------------
# DESAFIO (v2)
# -----------------------------
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

    # corta o que sobrou se a meta diminuiu
    cur.execute("DELETE FROM savings_deposits_v2 WHERE n > ?", (n,))
    cur.execute("DELETE FROM savings_overrides_v2 WHERE n > ?", (n,))

    # garante 1..N
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
    conn.commit()
    conn.close()

def clear_savings_goal_v2():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE savings_goal_v2 SET target_amount=NULL, due_date=NULL, n_deposits=NULL WHERE id=1")
    cur.execute("DELETE FROM savings_deposits_v2")
    cur.execute("DELETE FROM savings_overrides_v2")
    conn.commit()
    conn.close()
