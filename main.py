#!/usr/bin/env python3
"""
Robô de Monitoramento de Spreads de Debêntures — ANBIMA
Monitora quedas de spread no mercado secundário e envia alertas via Telegram.

Uso:
    python main.py              # Executa para hoje (com auto-backfill de 7 dias)
    python main.py --date 2026-03-09  # Executa para uma data específica
    python main.py --backfill 5       # Carrega últimos 5 dias úteis
    python main.py --dry-run          # Roda sem enviar Telegram
    python main.py --no-backfill      # Pula auto-backfill
    python main.py --redownload       # Re-baixa datas existentes (preencher colunas novas)
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta

import config
import db
import anbima_scraper
import alerts
import snd_scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config.LOG_PATH),
    ],
)
logger = logging.getLogger(__name__)


def auto_backfill(days: int = 7):
    """Backfill automático dos últimos N dias se não estão no banco."""
    stored = db.get_all_stored_dates()
    dt = date.today()

    for _ in range(days * 2):  # multiplicar por 2 para cobrir fins de semana
        if dt.weekday() < 5:  # dias úteis
            dt_str = dt.isoformat()
            if dt_str not in stored:
                df = anbima_scraper.download_spreads(dt)
                if df is not None:
                    db.save_spreads(df)
                    logger.info(f"Auto-backfill: carregado {dt}")
        dt -= timedelta(days=1)
        if dt < date.today() - timedelta(days=config.BACKFILL_MAX_DAYS):
            break


def run_for_date(dt: date, dry_run: bool = False, redownload: bool = False) -> bool:
    """Executa o pipeline completo para uma data."""
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Processando data: {dt}")

    # 1. Baixar dados
    has_data = db.has_data_for_date(dt)
    if has_data and not redownload:
        logger.info(f"Dados para {dt} já existem no banco")
    else:
        df = anbima_scraper.download_spreads(dt)
        if df is None:
            logger.warning(f"Sem dados disponíveis para {dt}")
            return False
        db.save_spreads(df)

    # 2. Detectar quedas de spread
    drops = alerts.detect_spread_drops(dt)

    if drops.empty:
        logger.info(f"Nenhuma queda significativa detectada para {dt}")
    else:
        # 3. Formatar e enviar alerta
        messages = alerts.format_alert_message(drops)

        if dry_run:
            for msg in messages:
                print("\n" + "=" * 60)
                print("[DRY-RUN] Alerta que seria enviado:")
                print("=" * 60)
                print(msg)
                print("=" * 60 + "\n")
        else:
            alerts.send_telegram(messages)

    # 4. Resumo diário
    if config.SUMMARY_ENABLED:
        summary_msgs = alerts.format_daily_summary(dt.isoformat())
        if summary_msgs:
            if dry_run:
                for msg in summary_msgs:
                    print("\n" + "=" * 60)
                    print("[DRY-RUN] Resumo diário:")
                    print("=" * 60)
                    print(msg)
                    print("=" * 60 + "\n")
            else:
                alerts.send_telegram(summary_msgs)

    return True


def backfill(days: int, redownload: bool = False):
    """Carrega dados dos últimos N dias úteis."""
    logger.info(f"Backfill: carregando últimos {days} dias")
    stored = db.get_all_stored_dates()
    dt = date.today()
    loaded = 0

    while loaded < days:
        if dt.weekday() < 5:  # dias úteis
            dt_str = dt.isoformat()
            if dt_str not in stored or redownload:
                df = anbima_scraper.download_spreads(dt)
                if df is not None:
                    db.save_spreads(df)
                    loaded += 1
                    logger.info(f"Backfill: {loaded}/{days} — {dt}")
        dt -= timedelta(days=1)
        if dt < date.today() - timedelta(days=config.BACKFILL_MAX_DAYS):
            break

    logger.info(f"Backfill concluído: {loaded} dias carregados")


def main():
    parser = argparse.ArgumentParser(
        description="Robô de Monitoramento de Spreads ANBIMA"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Data específica (YYYY-MM-DD). Default: hoje"
    )
    parser.add_argument(
        "--backfill", type=int, default=0,
        help="Carregar últimos N dias úteis"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Executar sem enviar Telegram"
    )
    parser.add_argument(
        "--threshold", type=float, default=None,
        help=f"Override do threshold de queda (%%). Default: {config.SPREAD_DROP_THRESHOLD}%%"
    )
    parser.add_argument(
        "--no-backfill", action="store_true",
        help="Pular auto-backfill"
    )
    parser.add_argument(
        "--redownload", action="store_true",
        help="Re-baixar datas existentes (preencher colunas novas)"
    )
    parser.add_argument(
        "--update-volumes", action="store_true",
        help="Forçar atualização dos volumes SND"
    )
    parser.add_argument(
        "--skip-volumes", action="store_true",
        help="Pular atualização de volumes SND"
    )
    args = parser.parse_args()

    if args.threshold is not None:
        config.SPREAD_DROP_THRESHOLD = args.threshold

    db.init_db()

    # Backfill explícito
    if args.backfill > 0:
        backfill(args.backfill, redownload=args.redownload)
        return

    # Auto-backfill (7 dias) para garantir dados recentes
    if not args.no_backfill:
        auto_backfill(days=7)

    if args.date:
        dt = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        dt = anbima_scraper.find_last_available_date()
        if dt is None:
            logger.error("Nenhuma data disponível nos últimos 10 dias")
            sys.exit(1)

    # Atualizar volumes SND se necessário
    if not args.skip_volumes:
        if args.update_volumes:
            # Refresh forçado: re-atualiza todos os códigos ativos
            codigos = db.get_outdated_codigos(max_age_days=0)
        else:
            # Rotina normal: busca apenas códigos novos (ausentes do SND)
            codigos = db.get_new_codigos()
            # Também inclui códigos desatualizados (> SND_REFRESH_DAYS dias)
            outdated = db.get_outdated_codigos(max_age_days=config.SND_REFRESH_DAYS)
            codigos = list(set(codigos + outdated))

        if codigos:
            logger.info(f"Atualizando volumes SND para {len(codigos)} códigos")
            vol_df = snd_scraper.fetch_all_volumes(codigos)
            if not vol_df.empty:
                db.save_volumes(vol_df)
        else:
            logger.info("Volumes SND atualizados (nenhum código novo ou desatualizado)")

    run_for_date(dt, dry_run=args.dry_run, redownload=args.redownload)


if __name__ == "__main__":
    main()
