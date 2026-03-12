"""
Scraper do SND (debentures.com.br) — extrai volume em circulação de debêntures.

Busca quantidade em mercado e valor nominal atualizado (VNA) para calcular
o volume outstanding real de cada debênture.
"""

import logging
import re
import time
from datetime import datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SND_BASE_URL = (
    "https://www.debentures.com.br/exploreosnd/consultaadados/"
    "emissoesdedebentures/caracteristicas_d.asp"
)

# Padrão brasileiro: 1.000,000000 → usar vírgula como decimal
_RE_MERCADO = re.compile(r"Mercado:</b>\s*([\d.]+)")
_RE_VNA = re.compile(r"Nominal\s+em\s+\d{2}/\d{2}/\d{4}:</b>\s*R\$\s*([\d.,]+)")
_RE_SPREAD = re.compile(r"Juros/Spread:</b>\s*</td>\s*<td[^>]*>\s*([\d.,]+)")


def _parse_br_number(s: str) -> float:
    """Converte número brasileiro (1.234,56) para float."""
    return float(s.replace(".", "").replace(",", "."))


def fetch_volume(codigo: str) -> dict | None:
    """
    Busca volume em circulação de uma debênture no SND.
    Retorna dict com codigo, quantidade_mercado, vna, volume_outstanding, updated_at.
    Retorna None se não encontrar dados.
    """
    url = f"{SND_BASE_URL}?tip_deb=publicas&selecao={codigo}"

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Erro ao acessar SND para {codigo}: {e}")
        return None

    html = resp.text

    m_mercado = _RE_MERCADO.search(html)
    m_vna = _RE_VNA.search(html)

    if not m_mercado or not m_vna:
        logger.warning(f"Dados não encontrados no SND para {codigo}")
        return None

    quantidade = int(m_mercado.group(1).replace(".", ""))
    vna = _parse_br_number(m_vna.group(1))
    volume = quantidade * vna

    # Spread de emissão (Taxa de Juros/Spread)
    m_spread = _RE_SPREAD.search(html)
    spread_emissao = _parse_br_number(m_spread.group(1)) if m_spread else None

    return {
        "codigo": codigo,
        "quantidade_mercado": quantidade,
        "vna": vna,
        "volume_outstanding": volume,
        "spread_emissao": spread_emissao,
        "updated_at": datetime.now().isoformat(),
    }


def fetch_all_volumes(codigos: list[str], delay: float = 0.1) -> pd.DataFrame:
    """
    Busca volumes para uma lista de códigos.
    Retorna DataFrame com resultados encontrados.
    """
    results = []
    total = len(codigos)

    for i, codigo in enumerate(codigos, 1):
        logger.info(f"SND [{i}/{total}]: buscando {codigo}")
        data = fetch_volume(codigo)
        if data:
            results.append(data)

        if i < total:
            time.sleep(delay)

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    test_codigos = ["NTSDA2", "NTSDA1", "SAPR10", "VALE19", "CODIGO_FAKE"]
    print(f"\nTestando SND scraper com {len(test_codigos)} códigos...\n")

    df = fetch_all_volumes(test_codigos)

    if not df.empty:
        for _, row in df.iterrows():
            vol = row["volume_outstanding"]
            fmt = f"R$ {vol/1e6:.1f}M" if vol >= 1e6 else f"R$ {vol:,.0f}"
            print(
                f"  {row['codigo']}: qtd={row['quantidade_mercado']:,} × "
                f"VNA={row['vna']:.2f} = {fmt}"
            )
    else:
        print("Nenhum resultado encontrado.")

    print(f"\nTotal: {len(df)} de {len(test_codigos)} encontrados")
