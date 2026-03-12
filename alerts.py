"""
Módulo de detecção de quedas de spread e envio de alertas via Telegram.
"""

import html
import logging
from datetime import date

import pandas as pd
import requests

import config
import db

logger = logging.getLogger(__name__)

TELEGRAM_MAX_LENGTH = 4096


def _estimate_savings(old_rate: float, new_rate: float, notional: float) -> float:
    """Estima economia anual em R$ com base na variação do spread."""
    delta = abs(old_rate - new_rate) / 100  # converter de % para decimal
    return delta * notional


def _analyze_refinancing(
    old_rate: float,
    new_rate: float,
    duration: float | None,
    volume: float,
) -> dict:
    """
    Analisa viabilidade de refinanciamento usando:
    ganho_bruto = (spread_antigo - spread_novo) × duration × volume
    custo_total = (emissão% + resgate%) × volume
    """
    if duration is None or duration <= 0:
        return {"viable": None}

    duration_years = duration / 252  # ANBIMA publica em dias úteis
    delta_pct = (old_rate - new_rate) / 100  # de % para decimal
    gross_saving = delta_pct * duration_years * volume

    emission_cost = (config.REFINANCE_EMISSION_COST_PCT / 100) * volume
    call_premium = (config.REFINANCE_CALL_PREMIUM_PCT / 100) * volume
    total_cost = emission_cost + call_premium

    net_saving = gross_saving - total_cost

    return {
        "viable": net_saving > 0,
        "gross_saving": gross_saving,
        "emission_cost": emission_cost,
        "call_premium": call_premium,
        "total_cost": total_cost,
        "net_saving": net_saving,
    }


def _fmt_brl(value: float) -> str:
    """Formata valor em R$ de forma legível (K, M, B)."""
    if abs(value) >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"R$ {value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"R$ {value / 1_000:.0f}K"
    return f"R$ {value:.0f}"


def _fmt_notional(value: float) -> str:
    """Formata nocional de forma curta."""
    if abs(value) >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.0f}B"
    if abs(value) >= 1_000_000:
        return f"R$ {value / 1_000_000:.0f}M"
    return f"R$ {value:,.0f}"


def detect_spread_drops(current_date: date) -> pd.DataFrame:
    """
    Compara spreads de hoje com o dia anterior.
    Retorna DataFrame com títulos cuja taxa indicativa caiu mais que o threshold.
    """
    today_df = db.get_spreads_for_date(current_date)
    prev_df = db.get_previous_spreads(current_date)

    if today_df.empty or prev_df.empty:
        logger.info("Dados insuficientes para comparação")
        return pd.DataFrame()

    # Merge por código — trazer todos os campos de hoje + taxa anterior
    merge_cols = ["codigo", "taxa_indicativa", "data"]
    merged = today_df.merge(
        prev_df[merge_cols],
        on="codigo",
        suffixes=("_hoje", "_ant"),
    )

    if merged.empty:
        return pd.DataFrame()

    # Calcular variação percentual
    merged["variacao_pct"] = (
        (merged["taxa_indicativa_hoje"] - merged["taxa_indicativa_ant"])
        / merged["taxa_indicativa_ant"].abs()
        * 100
    )

    # Filtrar quedas (variação negativa) maiores que o threshold
    drops = merged[merged["variacao_pct"] <= -config.SPREAD_DROP_THRESHOLD].copy()

    # Aplicar filtros de config
    if config.INDEXADORES_FILTRO:
        drops = drops[drops["indexador"].isin(config.INDEXADORES_FILTRO)]

    if config.EMPRESAS_FILTRO:
        pattern = "|".join(config.EMPRESAS_FILTRO)
        drops = drops[drops["nome"].str.contains(pattern, case=False, na=False)]

    drops = drops.sort_values("variacao_pct")
    logger.info(f"Detectadas {len(drops)} quedas de spread acima do threshold")
    return drops


def format_alert_message(drops: pd.DataFrame) -> list[str]:
    """Formata mensagem de alerta expandida para Telegram (HTML). Retorna lista de mensagens."""
    if drops.empty:
        return []

    current_date_str = drops["data_hoje"].iloc[0]

    header = (
        "🔔 <b>ALERTA: Quedas de Spread Detectadas</b>\n\n"
        f"📅 Data: {current_date_str}\n"
        f"📊 Threshold: {config.SPREAD_DROP_THRESHOLD}%\n"
    )

    entries = []
    for _, row in drops.iterrows():
        nome = html.escape(str(row["nome"]))
        codigo = row["codigo"]
        indexador = row.get("indexador", "")
        venc = row.get("vencimento", "")
        taxa_ant = row["taxa_indicativa_ant"]
        taxa_hoje = row["taxa_indicativa_hoje"]
        var_pct = row["variacao_pct"]

        # Linha principal
        entry = f"\n📉 <b>{nome}</b> (<code>{codigo}</code>)\n"
        entry += f"   {indexador} | Venc: {venc}"

        if pd.notna(row.get("duration")):
            entry += f" | Duration: {row['duration'] / 252:.1f}a"
        entry += "\n"

        # Spread
        entry += f"   Spread: {taxa_ant:.4f} → {taxa_hoje:.4f} ({var_pct:+.2f}%)\n"

        # Bid-Ask
        if pd.notna(row.get("bid_ask_spread")):
            ba_parts = f"   Bid-Ask: {row['bid_ask_spread']:.4f}"
            if pd.notna(row.get("taxa_compra")) and pd.notna(row.get("taxa_venda")):
                ba_parts += f" (compra: {row['taxa_compra']:.4f} / venda: {row['taxa_venda']:.4f})"
            entry += ba_parts + "\n"

        # Desvio e PU Par
        extras = []
        if pd.notna(row.get("desvio_padrao")):
            extras.append(f"Desvio: {row['desvio_padrao']:.2f}")
        if pd.notna(row.get("pct_pu_par")):
            extras.append(f"PU Par: {row['pct_pu_par']:.2f}%")
        if extras:
            entry += f"   {' | '.join(extras)}\n"

        # Multi-período
        variations = db.get_multi_period_variation(codigo, taxa_hoje, current_date_str)
        var_parts = []
        for label in ["1D", "5D", "1M", "3M", "6M", "1Y"]:
            v = variations.get(label)
            if v is not None:
                var_parts.append(f"{label} {v:+.0f}%")
        if var_parts:
            entry += f"   Variação: {' | '.join(var_parts)}\n"

        # Rank histórico
        rank = db.get_historical_rank(codigo, taxa_hoje)
        if rank.get("menor_em_dias") and rank["menor_em_dias"] > 5:
            entry += f"   ⭐ Menor spread em {rank['menor_em_dias']} dias!\n"

        # Análise de refinanciamento
        volume_real = db.get_volume(codigo)
        has_real_volume = volume_real is not None and volume_real > 0
        notional = volume_real if has_real_volume else config.REFINANCE_NOTIONAL
        duration_val = row.get("duration") if pd.notna(row.get("duration")) else None
        vol_label = _fmt_notional(notional)

        refi = _analyze_refinancing(taxa_ant, taxa_hoje, duration_val, notional)

        if refi["viable"] is not None:
            if has_real_volume:
                entry += f"   💰 <b>Refinanciamento</b> (vol: {vol_label}):\n"
            else:
                entry += f"   💰 <b>Refinanciamento</b> (vol est: {vol_label}):\n"
            entry += f"      Ganho bruto: {_fmt_brl(refi['gross_saving'])}"
            entry += f" | Custos: {_fmt_brl(refi['total_cost'])}\n"
            entry += f"      Líquido: {_fmt_brl(refi['net_saving'])}"
            if refi["viable"]:
                entry += " ✅ VIÁVEL\n"
            else:
                entry += " ❌ NÃO COMPENSA\n"
        else:
            savings = _estimate_savings(taxa_ant, taxa_hoje, notional)
            if savings > 0:
                label = vol_label if has_real_volume else f"est. {vol_label}"
                entry += f"   💰 Economia: ~{_fmt_brl(savings)}/ano ({label}) — sem duration p/ análise completa\n"

        entries.append(entry)

    footer = "\n💡 <i>Oportunidade de refinanciamento — spread caiu no secundário!</i>"

    # Dividir em múltiplas mensagens se necessário
    messages = []
    current = header
    for entry in entries:
        if len(current) + len(entry) + len(footer) > TELEGRAM_MAX_LENGTH:
            current += footer
            messages.append(current)
            current = "🔔 <b>ALERTA (continuação)</b>\n"
        current += entry
    current += footer
    messages.append(current)

    return messages


def format_daily_summary(current_date: str) -> list[str]:
    """
    Formata resumo diário com estatísticas e top movers.
    Retorna lista de mensagens Telegram (HTML).
    """
    today_df = db.get_spreads_for_date(date.fromisoformat(current_date))

    if today_df.empty:
        return []

    total = len(today_df)

    # Médias por indexador
    medias = {}
    for idx in ["DI", "IPCA", "IGPM", "PRE"]:
        subset = today_df[today_df["indexador"] == idx]["taxa_indicativa"]
        if not subset.empty:
            medias[idx] = subset.mean()

    media_str = " | ".join(f"{k}: {v:.2f}%" for k, v in medias.items())

    header = (
        f"📊 <b>RESUMO DIÁRIO — {current_date}</b>\n"
        f"Títulos monitorados: {total:,}\n"
    )
    if media_str:
        header += f"Média spread: {media_str}\n"

    # Top movers
    movers = db.get_top_movers(current_date, config.SUMMARY_TOP_N)

    body = ""

    # Compressões
    if movers["compressoes"]:
        body += "\n🔽 <b>TOP COMPRESSÕES:</b>\n"
        for i, m in enumerate(movers["compressoes"], 1):
            nome = html.escape(str(m["nome"]))
            body += (
                f"{i}. {nome} (<code>{m['codigo']}</code>): "
                f"{m['taxa_ant']:.4f} → {m['taxa_hoje']:.4f} "
                f"({m['variacao_pct']:+.1f}%)\n"
            )

    # Aberturas
    if movers["aberturas"]:
        n_aberturas = min(5, len(movers["aberturas"]))
        body += f"\n🔼 <b>TOP ABERTURAS:</b>\n"
        for i, m in enumerate(movers["aberturas"][:n_aberturas], 1):
            nome = html.escape(str(m["nome"]))
            body += (
                f"{i}. {nome} (<code>{m['codigo']}</code>): "
                f"{m['taxa_ant']:.4f} → {m['taxa_hoje']:.4f} "
                f"({m['variacao_pct']:+.1f}%)\n"
            )

    full = header + body

    # Dividir se necessário
    if len(full) <= TELEGRAM_MAX_LENGTH:
        return [full]

    messages = []
    lines = full.split("\n")
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > TELEGRAM_MAX_LENGTH:
            messages.append(current)
            current = ""
        current += line + "\n"
    if current.strip():
        messages.append(current)

    return messages


def send_telegram(messages: list[str]) -> bool:
    """Envia mensagens via Telegram Bot API."""
    if not messages:
        return False

    if not config.TELEGRAM_BOT_TOKEN:
        logger.warning("Token do Telegram não configurado! Exibindo no console:")
        for msg in messages:
            print("\n" + "=" * 60)
            print(msg)
            print("=" * 60 + "\n")
        return False

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    success = True

    for msg in messages:
        payload = {
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Mensagem enviada com sucesso via Telegram")
        except requests.RequestException as e:
            logger.error(f"Falha ao enviar Telegram: {e}")
            success = False

    return success
