"""
Módulo de persistência — armazena histórico de spreads em SQLite.
"""

import sqlite3
import logging
from datetime import date, timedelta

import pandas as pd

import config

logger = logging.getLogger(__name__)

# Colunas que devem existir na tabela (além das da PK original)
EXPECTED_COLUMNS = {
    "data": "TEXT NOT NULL",
    "codigo": "TEXT NOT NULL",
    "nome": "TEXT NOT NULL",
    "vencimento": "TEXT",
    "indexador": "TEXT",
    "taxa_indicativa": "REAL",
    "pu": "REAL",
    "duration": "REAL",
    "taxa_compra": "REAL",
    "taxa_venda": "REAL",
    "desvio_padrao": "REAL",
    "intervalo_min": "REAL",
    "intervalo_max": "REAL",
    "pct_pu_par": "REAL",
    "pct_reune": "REAL",
    "ref_ntnb": "TEXT",
    "bid_ask_spread": "REAL",
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Cria tabelas se não existirem e migra colunas faltantes."""
    with get_connection() as conn:
        # Tabela de volumes SND
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snd_volumes (
                codigo TEXT PRIMARY KEY,
                quantidade_mercado INTEGER,
                vna REAL,
                volume_outstanding REAL,
                spread_emissao REAL,
                updated_at TEXT
            )
        """)

        # Migração: adicionar spread_emissao se não existir
        snd_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(snd_volumes)").fetchall()
        }
        if "spread_emissao" not in snd_cols:
            conn.execute("ALTER TABLE snd_volumes ADD COLUMN spread_emissao REAL")
            logger.info("Migração: coluna 'spread_emissao' adicionada à tabela snd_volumes")

        # Tabela de spreads
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spreads (
                data TEXT NOT NULL,
                codigo TEXT NOT NULL,
                nome TEXT NOT NULL,
                vencimento TEXT,
                indexador TEXT,
                taxa_indicativa REAL,
                pu REAL,
                duration REAL,
                taxa_compra REAL,
                taxa_venda REAL,
                desvio_padrao REAL,
                intervalo_min REAL,
                intervalo_max REAL,
                pct_pu_par REAL,
                pct_reune REAL,
                ref_ntnb TEXT,
                bid_ask_spread REAL,
                PRIMARY KEY (data, codigo)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spreads_codigo ON spreads(codigo)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spreads_data ON spreads(data)
        """)

        # Migração: adicionar colunas faltantes
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(spreads)").fetchall()
        }
        for col, col_type in EXPECTED_COLUMNS.items():
            if col not in existing:
                # ALTER TABLE não aceita NOT NULL sem default
                clean_type = col_type.replace("NOT NULL", "").strip()
                conn.execute(f"ALTER TABLE spreads ADD COLUMN {col} {clean_type}")
                logger.info(f"Migração: coluna '{col}' adicionada à tabela spreads")


def save_spreads(df: pd.DataFrame):
    """Salva DataFrame de spreads no banco. Ignora duplicatas via INSERT OR IGNORE."""
    if df is None or df.empty:
        return

    cols = [c for c in df.columns if c in EXPECTED_COLUMNS]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    with get_connection() as conn:
        rows = df[cols].values.tolist()
        conn.executemany(
            f"INSERT OR IGNORE INTO spreads ({col_names}) VALUES ({placeholders})",
            rows,
        )
    logger.info(f"Salvos {len(df)} registros no banco (duplicatas ignoradas)")


def has_data_for_date(dt: date) -> bool:
    """Verifica se já temos dados para uma data."""
    with get_connection() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM spreads WHERE data = ?",
            (dt.isoformat(),)
        ).fetchone()
        return result[0] > 0


def get_all_stored_dates() -> set[str]:
    """Retorna todas as datas distintas já armazenadas no banco."""
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT data FROM spreads").fetchall()
        return {row[0] for row in rows}


def get_previous_spreads(dt: date) -> pd.DataFrame:
    """Busca os spreads do dia útil anterior mais recente."""
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT * FROM spreads
            WHERE data = (
                SELECT MAX(data) FROM spreads WHERE data < ?
            )
            """,
            conn,
            params=(dt.isoformat(),)
        )
    return df


def get_spreads_for_date(dt: date) -> pd.DataFrame:
    """Busca spreads para uma data específica."""
    with get_connection() as conn:
        df = pd.read_sql_query(
            "SELECT * FROM spreads WHERE data = ?",
            conn,
            params=(dt.isoformat(),)
        )
    return df


def get_spread_history(codigo: str, days: int = 30) -> pd.DataFrame:
    """Busca histórico de spreads de um título."""
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT * FROM spreads
            WHERE codigo = ?
            ORDER BY data DESC
            LIMIT ?
            """,
            conn,
            params=(codigo, days)
        )
    return df


def get_spread_at_date(codigo: str, target_date: str) -> float | None:
    """Retorna o spread (taxa_indicativa) mais próximo antes de uma data."""
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT taxa_indicativa FROM spreads
            WHERE codigo = ? AND data <= ?
            ORDER BY data DESC
            LIMIT 1
            """,
            (codigo, target_date),
        ).fetchone()
    return row[0] if row else None


def get_multi_period_variation(
    codigo: str, current_rate: float, current_date: str
) -> dict[str, float | None]:
    """
    Calcula variações percentuais do spread para múltiplos períodos.
    Retorna dict {"1D": -5.2, "5D": -3.1, "1M": None, ...}
    """
    result = {}
    with get_connection() as conn:
        # Buscar todas as datas distintas ordenadas para mapear dias úteis
        dates = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT data FROM spreads WHERE data <= ? ORDER BY data DESC",
                (current_date,),
            ).fetchall()
        ]

    date_set = {d: i for i, d in enumerate(dates)}

    for label, biz_days in config.MULTI_PERIOD_DAYS.items():
        if biz_days < len(dates):
            past_date = dates[biz_days]
            past_rate = get_spread_at_date(codigo, past_date)
            if past_rate and past_rate != 0:
                result[label] = (current_rate - past_rate) / abs(past_rate) * 100
            else:
                result[label] = None
        else:
            result[label] = None

    return result


def get_historical_rank(codigo: str, current_rate: float) -> dict:
    """
    Retorna contexto histórico: percentil, min/max, e se é menor spread em N dias.
    """
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT taxa_indicativa, data FROM spreads WHERE codigo = ? ORDER BY data",
            (codigo,),
        ).fetchall()

    if not rows:
        return {}

    rates = [r[0] for r in rows if r[0] is not None]
    if not rates:
        return {}

    hist_min = min(rates)
    hist_max = max(rates)
    below_count = sum(1 for r in rates if r <= current_rate)
    percentil = below_count / len(rates) * 100

    # Verificar se é menor spread recente
    menor_em_dias = None
    for i in range(len(rows) - 1, -1, -1):
        r = rows[i][0]
        if r is not None and r < current_rate:
            break
        menor_em_dias = len(rows) - i

    return {
        "percentil": percentil,
        "hist_min": hist_min,
        "hist_max": hist_max,
        "total_obs": len(rates),
        "menor_em_dias": menor_em_dias,
    }


# ── SND Volumes ─────────────────────────────────────────


def save_volumes(df: pd.DataFrame):
    """Salva DataFrame de volumes SND no banco. Atualiza se já existir."""
    if df is None or df.empty:
        return

    with get_connection() as conn:
        for _, row in df.iterrows():
            spread_em = float(row["spread_emissao"]) if pd.notna(row.get("spread_emissao")) else None
            conn.execute(
                """
                INSERT OR REPLACE INTO snd_volumes
                    (codigo, quantidade_mercado, vna, volume_outstanding, spread_emissao, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["codigo"],
                    int(row["quantidade_mercado"]),
                    float(row["vna"]),
                    float(row["volume_outstanding"]),
                    spread_em,
                    row["updated_at"],
                ),
            )
    logger.info(f"Salvos {len(df)} volumes SND no banco")


def get_volume(codigo: str) -> float | None:
    """Retorna volume_outstanding de um código, ou None se não existir."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT volume_outstanding FROM snd_volumes WHERE codigo = ?",
            (codigo,),
        ).fetchone()
    return row[0] if row else None


def get_spread_emissao(codigo: str) -> float | None:
    """Retorna spread de emissão de um código, ou None se não existir."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT spread_emissao FROM snd_volumes WHERE codigo = ?",
            (codigo,),
        ).fetchone()
    return row[0] if row else None


def get_new_codigos() -> list[str]:
    """Retorna códigos ativos (na data mais recente de spreads) que não existem na tabela snd_volumes."""
    with get_connection() as conn:
        latest_date = conn.execute(
            "SELECT MAX(data) FROM spreads"
        ).fetchone()[0]

        if not latest_date:
            return []

        rows = conn.execute(
            """
            SELECT DISTINCT s.codigo
            FROM spreads s
            LEFT JOIN snd_volumes v ON s.codigo = v.codigo
            WHERE s.data = ?
              AND v.codigo IS NULL
            """,
            (latest_date,),
        ).fetchall()

    return [r[0] for r in rows]


def get_outdated_codigos(max_age_days: int = 30) -> list[str]:
    """Retorna códigos ativos com updated_at mais antigo que max_age_days (0 = todos)."""
    from datetime import datetime, timedelta

    with get_connection() as conn:
        latest_date = conn.execute(
            "SELECT MAX(data) FROM spreads"
        ).fetchone()[0]

        if not latest_date:
            return []

        if max_age_days == 0:
            # Retorna todos os códigos ativos
            rows = conn.execute(
                "SELECT DISTINCT codigo FROM spreads WHERE data = ?",
                (latest_date,),
            ).fetchall()
        else:
            cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
            rows = conn.execute(
                """
                SELECT DISTINCT s.codigo
                FROM spreads s
                LEFT JOIN snd_volumes v ON s.codigo = v.codigo
                WHERE s.data = ?
                  AND (v.updated_at IS NULL OR v.updated_at < ?)
                """,
                (latest_date, cutoff),
            ).fetchall()

    return [r[0] for r in rows]


def get_top_movers(current_date: str, n: int = 10) -> dict:
    """
    Retorna top N maiores compressões e aberturas do dia.
    Retorna dict com chaves 'compressoes' e 'aberturas', cada uma com lista de dicts.
    """
    with get_connection() as conn:
        # Buscar data anterior
        prev_date_row = conn.execute(
            "SELECT MAX(data) FROM spreads WHERE data < ?",
            (current_date,),
        ).fetchone()

    if not prev_date_row or not prev_date_row[0]:
        return {"compressoes": [], "aberturas": []}

    prev_date = prev_date_row[0]

    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                h.codigo,
                h.nome,
                h.indexador,
                h.taxa_indicativa AS taxa_hoje,
                h.vencimento,
                h.duration,
                a.taxa_indicativa AS taxa_ant,
                CASE WHEN ABS(a.taxa_indicativa) > 0
                    THEN (h.taxa_indicativa - a.taxa_indicativa) / ABS(a.taxa_indicativa) * 100
                    ELSE NULL
                END AS variacao_pct
            FROM spreads h
            JOIN spreads a ON h.codigo = a.codigo AND a.data = ?
            WHERE h.data = ?
              AND h.taxa_indicativa IS NOT NULL
              AND a.taxa_indicativa IS NOT NULL
              AND ABS(a.taxa_indicativa) > 0
            ORDER BY variacao_pct ASC
            """,
            conn,
            params=(prev_date, current_date),
        )

    if df.empty:
        return {"compressoes": [], "aberturas": []}

    compressoes = df.head(n).to_dict("records")
    aberturas = df.tail(n).sort_values("variacao_pct", ascending=False).to_dict("records")

    return {"compressoes": compressoes, "aberturas": aberturas}
