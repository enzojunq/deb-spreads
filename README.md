# DCM — Monitoramento de Spreads de Debentures

Monitora spreads de debentures no mercado secundario brasileiro (ANBIMA) e envia alertas via Telegram quando detecta quedas significativas, sinalizando oportunidades de refinanciamento.

> **Aviso legal:** Este projeto e apenas para fins educacionais e informativos. Nao constitui recomendacao de investimento, consultoria financeira ou oferta de valores mobiliarios. Os dados sao obtidos de fontes publicas e podem conter erros ou atrasos. Use por sua conta e risco.

## Funcionalidades

- **Coleta diaria** de spreads de ~1.200 debentures da ANBIMA
- **Deteccao de quedas** de spread acima de um threshold configuravel
- **Analise de refinanciamento** comparando spread de emissao (SND) vs. spread atual
- **Volume real** via scraping do SND (debentures.com.br) para calculos com nocional real
- **Resumo diario** com top compressoes e aberturas
- **Historico multi-periodo** (1D, 5D, 1M, 3M, 6M, 1Y)
- **Backfill automatico** de ate 45 dias para preencher lacunas

## Arquitetura

```
main.py                # Orquestracao e CLI
├── anbima_scraper.py  # Download e parsing dos dados ANBIMA
├── snd_scraper.py     # Scraping de volumes e spread de emissao do SND
├── alerts.py          # Deteccao de quedas, formatacao e envio Telegram
├── db.py              # Persistencia SQLite (spreads + volumes SND)
└── config.py          # Configuracoes (le .env para credenciais)
```

**Banco de dados:** SQLite local (`spreads.db`) com duas tabelas:
- `spreads` — historico de spreads por codigo/data
- `snd_volumes` — volume em circulacao e spread de emissao por codigo

Todos os dados ficam armazenados localmente. Nenhuma informacao e enviada a terceiros, exceto as mensagens de alerta ao Telegram (quando configurado).

## Fontes de dados

| Fonte | Dados | Tipo |
|-------|-------|------|
| [ANBIMA](https://www.anbima.com.br/) | Spreads indicativos, duration, PU, bid-ask | Arquivo publico diario |
| [SND / debentures.com.br](https://www.debentures.com.br/) | Volume em circulacao, VNA, spread de emissao | Scraping de pagina publica |

**Limitacoes:**
- Dados ANBIMA sao publicados D+1 (dia util seguinte)
- O scraping do SND depende do layout da pagina — pode quebrar se o site mudar
- Dados podem estar indisponiveis em feriados ou por instabilidade das fontes
- O delay entre requests ao SND (`0.1s`) e intencional para nao sobrecarregar o servidor

## Setup

### Requisitos

- Python 3.11+

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar credenciais

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

> **Nunca commite o arquivo `.env` no repositorio.** Ele ja esta no `.gitignore`.

### 3. Ajustar parametros (opcional)

Edite `config.py`:

| Parametro | Default | Descricao |
|-----------|---------|-----------|
| `SPREAD_DROP_THRESHOLD` | `5.0` | Queda minima (%) para disparar alerta |
| `INDEXADORES_FILTRO` | `None` | Filtrar por indexador (`"DI"`, `"IPCA"`, etc.) |
| `EMPRESAS_FILTRO` | `None` | Filtrar por emissor (`["VALE", "PETROBRAS"]`) |
| `SUMMARY_ENABLED` | `True` | Enviar resumo diario |
| `SUMMARY_TOP_N` | `10` | Quantidade de top movers no resumo |
| `SND_REFRESH_DAYS` | `30` | Intervalo para refresh de volumes do SND |
| `REFINANCE_NOTIONAL` | `R$ 500M` | Nocional fallback quando SND indisponivel |
| `REFINANCE_EMISSION_COST_PCT` | `1.0%` | Custo estimado de emissao |
| `REFINANCE_CALL_PREMIUM_PCT` | `0.5%` | Premio de resgate antecipado |

## Uso

```bash
# Execucao padrao (hoje, com auto-backfill de 7 dias)
python main.py

# Dry-run (nao envia Telegram, exibe no console)
python main.py --dry-run

# Data especifica
python main.py --date 2026-03-10

# Backfill dos ultimos 30 dias uteis
python main.py --backfill 30

# Re-baixar dados existentes (preencher colunas novas apos atualizacao)
python main.py --redownload

# Forcar atualizacao de todos os volumes SND
python main.py --update-volumes

# Pular volumes SND (execucao mais rapida)
python main.py --skip-volumes

# Override do threshold de queda
python main.py --threshold 3.0
```

## Como funciona

### Pipeline diario

1. **Backfill automatico** — verifica e preenche lacunas dos ultimos 7 dias
2. **Download ANBIMA** — baixa spreads do dia e persiste no SQLite
3. **Atualizacao SND** — busca volumes/spread de emissao para codigos novos ou desatualizados
4. **Deteccao de quedas** — compara spreads de hoje com o dia anterior
5. **Alertas Telegram** — envia mensagens formatadas com analise completa
6. **Resumo diario** — top compressoes e aberturas do dia

### Logica de volumes SND

Para evitar ~1.200 requests diarios ao SND, a rotina e incremental:

1. **Primeira execucao:** carga completa de todos os codigos
2. **Rotina diaria:** busca apenas codigos **novos** (presentes na ANBIMA mas ausentes no banco)
3. **Refresh mensal:** codigos com `updated_at` > 30 dias sao re-atualizados
4. **Refresh forcado:** `--update-volumes` re-atualiza todos

Resultado: de ~1.200 requests/dia para ~0-5/dia na rotina normal.

### Analise de refinanciamento

A analise compara o **spread de emissao** (taxa original da debenture, obtida do SND) com o **spread atual** no mercado secundario:

- **Spread comprimiu** (atual < emissao): calcula ganho bruto, desconta custos de emissao e resgate, e indica se o refinanciamento e viavel
- **Spread abriu** (atual >= emissao): sinaliza que refinanciamento nao e indicado
- **Spread de emissao indisponivel**: mostra o breakeven ("compensa se emitiu acima de DI+X%")

Formula:
```
ganho_bruto = (spread_emissao - spread_atual) / 100 x duration_anos x volume
custos = (custo_emissao% + premio_resgate%) / 100 x volume
liquido = ganho_bruto - custos
```

### Conteudo dos alertas

Cada alerta inclui:
- Spread anterior vs. atual com variacao percentual
- Bid-ask spread e taxas de compra/venda
- Variacao multi-periodo (1D, 5D, 1M, 3M, 6M, 1Y)
- Contexto historico (menor spread em N dias)
- Comparacao com spread de emissao e viabilidade de refinanciamento

## Estrutura do banco de dados

### Tabela `spreads`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | TEXT | Data do registro (PK) |
| `codigo` | TEXT | Codigo da debenture (PK) |
| `nome` | TEXT | Nome do emissor |
| `indexador` | TEXT | DI, IPCA, IGPM, PRE |
| `taxa_indicativa` | REAL | Spread indicativo ANBIMA |
| `duration` | REAL | Duration em dias uteis |
| `pu` | REAL | Preco unitario |
| `taxa_compra` / `taxa_venda` | REAL | Taxas de compra e venda |
| `bid_ask_spread` | REAL | Diferenca bid-ask |

### Tabela `snd_volumes`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `codigo` | TEXT | Codigo da debenture (PK) |
| `quantidade_mercado` | INTEGER | Debentures em circulacao |
| `vna` | REAL | Valor nominal atualizado |
| `volume_outstanding` | REAL | Volume total (qtd x VNA) |
| `spread_emissao` | REAL | Spread original da emissao |
| `updated_at` | TEXT | Data/hora da ultima atualizacao |

## Contribuindo

1. Fork o repositorio
2. Crie uma branch (`git checkout -b minha-feature`)
3. Faca commit das mudancas (`git commit -m 'Adiciona feature X'`)
4. Push para a branch (`git push origin minha-feature`)
5. Abra um Pull Request

Por favor, mantenha o estilo do codigo existente e teste com `--dry-run` antes de abrir o PR.

## Licenca

MIT License — veja [LICENSE](LICENSE) para detalhes.
