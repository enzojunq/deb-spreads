"""
Módulo para baixar e parsear dados de spreads de debêntures da ANBIMA.
Fonte: https://www.anbima.com.br/informacoes/merc-sec-debentures/

Formato do arquivo TXT:
- Separador: @
- Colunas: Código@Nome@Repac./Venc.@Índice/Correção@Taxa Compra@Taxa Venda@
           Taxa Indicativa@Desvio Padrão@Int. Min@Int. Max@PU@%PU Par@Duration@%Reune@Ref NTN-B
"""

import logging
import re
from datetime import date, timedelta

import pandas as pd
import requests

import config

logger = logging.getLogger(__name__)

BASE_URL = "https://www.anbima.com.br/informacoes/merc-sec-debentures/arqs"

# Colunas do arquivo ANBIMA (15 campos)
COLUMNS = [
    "codigo", "nome", "vencimento", "indexador_raw",
    "taxa_compra", "taxa_venda", "taxa_indicativa",
    "desvio_padrao", "intervalo_min", "intervalo_max",
    "pu", "pct_pu_par", "duration", "pct_reune", "ref_ntnb"
]

# Compilar patterns de limpeza de nome
_NAME_PATTERNS = [re.compile(p, re.IGNORECASE) for p in config.NAME_STRIP_PATTERNS]


def _clean_name(name: str) -> str:
    """Remove sufixos como (*), S/A, S.A., LTDA do nome do emissor."""
    for pat in _NAME_PATTERNS:
        name = pat.sub("", name)
    return name.strip()


def _build_url(dt: date) -> str:
    """Monta URL do arquivo TXT para uma data. Formato: dbYYMMDD.txt"""
    return f"{BASE_URL}/db{dt.strftime('%y%m%d')}.txt"


def download_spreads(dt: date | None = None) -> pd.DataFrame | None:
    """
    Baixa e parseia o arquivo de spreads da ANBIMA para a data informada.
    Retorna DataFrame com colunas padronizadas ou None se não disponível.
    """
    if dt is None:
        dt = date.today()

    url = _build_url(dt)
    logger.info(f"Baixando dados de {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Falha ao baixar dados para {dt}: {e}")
        return None

    return _parse_txt(resp.text, dt)


def _classify_indexador(raw: str) -> str:
    """Classifica o indexador a partir do texto bruto."""
    raw = raw.upper().strip()
    if "IPCA" in raw:
        return "IPCA"
    if "IGP" in raw:
        return "IGPM"
    if "DI" in raw or "CDI" in raw:
        return "DI"
    if "PRE" in raw or "PRÉ" in raw:
        return "PRE"
    return raw


def _parse_number(s: str) -> float | None:
    """Converte string numérica BR (vírgula decimal) para float."""
    if not s or s in ("--", "-", "N/D"):
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def _safe_col(cols: list, idx: int) -> str:
    """Retorna coluna no índice ou string vazia se não existir."""
    if idx < len(cols):
        return cols[idx].strip()
    return ""


def _parse_txt(content: str, dt: date) -> pd.DataFrame | None:
    """
    Parseia o conteúdo do arquivo TXT da ANBIMA.
    Separador: @ (arroba). Primeira linha útil é o cabeçalho.
    Captura todos os 15 campos + bid_ask_spread calculado.
    """
    lines = content.strip().split("\n")

    records = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        cols = line.split("@")
        if len(cols) < 10:
            continue

        codigo = cols[0].strip()

        # Pular cabeçalho e linhas não-dados
        if not codigo or codigo.startswith("Código") or codigo.startswith("ANBIMA"):
            continue

        taxa_indicativa = _parse_number(_safe_col(cols, 6))
        if taxa_indicativa is None:
            continue  # Pular títulos sem taxa (vencidos, sem mercado)

        indexador = _classify_indexador(_safe_col(cols, 3))
        taxa_compra = _parse_number(_safe_col(cols, 4))
        taxa_venda = _parse_number(_safe_col(cols, 5))

        # Calcular bid-ask spread
        bid_ask_spread = None
        if taxa_compra is not None and taxa_venda is not None:
            bid_ask_spread = round(abs(taxa_compra - taxa_venda), 4)

        records.append({
            "data": dt.isoformat(),
            "codigo": codigo,
            "nome": _clean_name(cols[1].strip()),
            "vencimento": _safe_col(cols, 2),
            "indexador": indexador,
            "taxa_compra": taxa_compra,
            "taxa_venda": taxa_venda,
            "taxa_indicativa": taxa_indicativa,
            "desvio_padrao": _parse_number(_safe_col(cols, 7)),
            "intervalo_min": _parse_number(_safe_col(cols, 8)),
            "intervalo_max": _parse_number(_safe_col(cols, 9)),
            "pu": _parse_number(_safe_col(cols, 10)),
            "pct_pu_par": _parse_number(_safe_col(cols, 11)),
            "duration": _parse_number(_safe_col(cols, 12)),
            "pct_reune": _parse_number(_safe_col(cols, 13)),
            "ref_ntnb": _safe_col(cols, 14) if len(cols) > 14 else None,
            "bid_ask_spread": bid_ask_spread,
        })

    if not records:
        logger.warning(f"Nenhum registro parseado para {dt}")
        return None

    df = pd.DataFrame(records)
    logger.info(f"Parseados {len(df)} títulos para {dt}")
    return df


def download_all_available(already_have: set[str] | None = None) -> list[date]:
    """
    Itera até BACKFILL_MAX_DAYS dias atrás, baixando datas que não estão no banco.
    Retorna lista de datas carregadas com sucesso.
    """
    if already_have is None:
        already_have = set()

    loaded = []
    dt = date.today()
    end = date.today() - timedelta(days=config.BACKFILL_MAX_DAYS)

    while dt >= end:
        dt_str = dt.isoformat()
        if dt_str not in already_have and dt.weekday() < 5:  # pular fins de semana
            df = download_spreads(dt)
            if df is not None:
                from db import save_spreads
                save_spreads(df)
                loaded.append(dt)
                logger.info(f"Backfill: carregado {dt} ({len(df)} títulos)")
        dt -= timedelta(days=1)

    logger.info(f"Backfill completo: {len(loaded)} datas carregadas")
    return loaded


def find_last_available_date(max_lookback: int = 10) -> date | None:
    """Procura a última data com dados disponíveis."""
    dt = date.today()
    for _ in range(max_lookback):
        url = _build_url(dt)
        try:
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                return dt
        except requests.RequestException:
            pass
        dt -= timedelta(days=1)
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    last_date = find_last_available_date()
    if last_date:
        print(f"Última data disponível: {last_date}")
        df = download_spreads(last_date)
        if df is not None:
            print(f"\n{len(df)} títulos encontrados")
            print(f"Colunas: {list(df.columns)}")
            print(df.head(20).to_string(index=False))
    else:
        print("Nenhuma data disponível nos últimos 10 dias")
