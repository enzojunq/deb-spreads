# DCM — Robô de Monitoramento de Spreads de Debêntures

Monitora spreads de debêntures no mercado secundário brasileiro (ANBIMA) e envia alertas via Telegram quando detecta quedas significativas — sinalizando oportunidades de refinanciamento.

## O que faz

- **Coleta diária** de spreads de ~1.200 debêntures da ANBIMA
- **Detecção de quedas** de spread acima de um threshold configurável
- **Análise de refinanciamento** com cálculo de ganho bruto, custos de emissão/resgate e viabilidade líquida
- **Volume real** via scraping do SND (debentures.com.br) para cálculos com nocional real
- **Resumo diário** com top compressões e aberturas
- **Histórico multi-período** (1D, 5D, 1M, 3M, 6M, 1Y)
- **Backfill automático** de até 45 dias para preencher lacunas

## Arquitetura

```
main.py             # Orquestração e CLI
├── anbima_scraper.py  # Download e parsing dos dados ANBIMA
├── snd_scraper.py     # Scraping de volumes do SND
├── alerts.py          # Detecção de quedas, formatação e envio Telegram
├── db.py              # Persistência SQLite (spreads + volumes SND)
└── config.py          # Configurações (lê .env para credenciais)
```

**Banco de dados:** SQLite (`spreads.db`) com duas tabelas:
- `spreads` — histórico de spreads por código/data
- `snd_volumes` — volume em circulação por código (atualização incremental)

## Setup

### 1. Dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar Telegram

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite `.env`:

```
TELEGRAM_BOT_TOKEN=seu_token_do_botfather
TELEGRAM_CHAT_ID=id_do_chat_ou_grupo
```

Para obter o token, fale com [@BotFather](https://t.me/BotFather) no Telegram.
Para obter o chat ID, envie uma mensagem ao bot e acesse `https://api.telegram.org/bot<TOKEN>/getUpdates`.

### 3. Ajustar parâmetros (opcional)

Edite `config.py` para ajustar:

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `SPREAD_DROP_THRESHOLD` | `5.0` | Queda mínima (%) para alerta |
| `INDEXADORES_FILTRO` | `None` | Filtrar por indexador (`"DI"`, `"IPCA"`, etc.) |
| `EMPRESAS_FILTRO` | `None` | Filtrar por emissor (`["VALE", "PETROBRAS"]`) |
| `SUMMARY_ENABLED` | `True` | Enviar resumo diário |
| `SUMMARY_TOP_N` | `10` | Quantidade de top movers no resumo |
| `SND_REFRESH_DAYS` | `30` | Intervalo para refresh completo de volumes |
| `REFINANCE_NOTIONAL` | `500M` | Nocional fallback quando SND indisponível |
| `REFINANCE_EMISSION_COST_PCT` | `1.0%` | Custo de emissão estimado |
| `REFINANCE_CALL_PREMIUM_PCT` | `0.5%` | Prêmio de resgate antecipado |

## Uso

```bash
# Execução padrão (hoje, com auto-backfill de 7 dias)
python main.py

# Dry-run (não envia Telegram, exibe no console)
python main.py --dry-run

# Data específica
python main.py --date 2026-03-10

# Backfill dos últimos 30 dias úteis
python main.py --backfill 30

# Re-baixar dados existentes (preencher colunas novas após atualização)
python main.py --redownload

# Forçar atualização de todos os volumes SND
python main.py --update-volumes

# Pular volumes SND (execução mais rápida)
python main.py --skip-volumes

# Override do threshold de queda
python main.py --threshold 3.0

# Pular auto-backfill
python main.py --no-backfill
```

## Lógica de Volumes SND

Para evitar ~1.200 requests diários ao SND (que levam ~10 min), a rotina é incremental:

1. **Primeira execução:** carga completa de todos os códigos (inevitável)
2. **Rotina diária:** busca SND apenas para códigos **novos** (presentes na ANBIMA mas ausentes no banco)
3. **Refresh mensal:** códigos com `updated_at` > 30 dias são re-atualizados automaticamente
4. **Refresh forçado:** `--update-volumes` re-atualiza todos os códigos

Resultado: de ~1.200 requests/dia para ~0-5/dia na rotina normal.

## Alertas

Quando uma debênture apresenta queda de spread acima do threshold, o alerta inclui:

- Spread anterior vs. atual com variação percentual
- Bid-ask spread e taxas de compra/venda
- Variação multi-período (1D, 5D, 1M, 3M, 6M, 1Y)
- Contexto histórico (menor spread em N dias)
- **Análise de refinanciamento:**
  - Ganho bruto = (spread antigo - spread novo) x duration x volume
  - Custos = emissão + resgate antecipado
  - Resultado líquido com indicação de viabilidade

## Licença

Uso interno.
