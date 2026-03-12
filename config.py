"""
Configurações do Robô de Monitoramento de Spreads ANBIMA
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Alertas ───────────────────────────────────────────────
# Queda mínima (%) no spread para disparar alerta
# Ex: 5.0 = alerta se spread caiu 5% ou mais em relação ao dia anterior
SPREAD_DROP_THRESHOLD = 5.0

# Indexadores para monitorar (None = todos)
# Opções: "DI", "IPCA", "IGPM", "PRE" ou None para todos
INDEXADORES_FILTRO = None

# Empresas específicas para monitorar (None = todas)
# Ex: ["VALE", "PETROBRAS", "ELETROBRAS"]
EMPRESAS_FILTRO = None

# ── Backfill / Histórico ─────────────────────────────────
BACKFILL_MAX_DAYS = 45  # janela máxima de backfill em dias corridos

# ── Resumo Diário ────────────────────────────────────────
SUMMARY_ENABLED = True   # enviar resumo diário mesmo sem alertas
SUMMARY_TOP_N = 10       # top movers no resumo

# ── Cálculo de Economia / Refinanciamento ─────────────────
REFINANCE_NOTIONAL = 500_000_000  # nocional padrão (R$) — fallback quando SND indisponível

# Custos de emissão como % do volume (banco, advogados, CVM, ANBIMA)
REFINANCE_EMISSION_COST_PCT = 1.0  # 1.0%

# Prêmio de resgate antecipado como % do volume (make-whole / call premium)
REFINANCE_CALL_PREMIUM_PCT = 0.5  # 0.5%

# ── SND (Volume em Circulação) ─────────────────────────
SND_REFRESH_DAYS = 30  # refresh completo de volumes a cada N dias

# ── Períodos para variação multi-período ─────────────────
MULTI_PERIOD_DAYS = {
    "1D": 1,
    "5D": 5,
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "1Y": 252,
}

# ── Limpeza de nomes ─────────────────────────────────────
# Regex patterns para limpar nomes de emissores
NAME_STRIP_PATTERNS = [
    r"\s*\(\*+\)",      # (*), (**)
    r"\s+S[./]A\.?",    # S/A, S.A., S.A
    r"\s+LTDA\.?",      # LTDA, LTDA.
    r"\s+S\.C\.?",      # S.C.
]

# ── Caminhos ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "spreads.db")
LOG_PATH = os.path.join(BASE_DIR, "robot.log")
