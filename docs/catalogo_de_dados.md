# Catálogo de Dados

Catálogo utilizado: `workspace` (Databricks Free Edition). Schemas: `bronze`, `silver`, `gold`.

## Camada Bronze

### `bronze.partidas`
Contexto: uma linha por partida do Campeonato Brasileiro Série A, como recebida do arquivo `campeonato-brasileiro-full.csv`, sem transformação de conteúdo (apenas metadados de ingestão adicionados).
Linhagem: arquivo bruto do Kaggle → Volume `bronze.dados_brutos` → tabela Delta.

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| ID | int | Identificador único da partida | inteiro positivo |
| rodata | int | Número da rodada do campeonato | 1–38 (temporada de pontos corridos) |
| data | date | Data em que a partida ocorreu | datas entre 2003 e o ano mais recente do dataset |
| hora | time | Horário de início da partida | HH:MM:SS |
| mandante | string | Time mandante (jogando em casa) | nomes de clubes |
| visitante | string | Time visitante | nomes de clubes |
| formacao_mandante | string | Esquema tático do mandante | ex: "4-4-2" (pode ter nulos em temporadas antigas) |
| formacao_visitante | string | Esquema tático do visitante | ex: "4-3-3" (pode ter nulos em temporadas antigas) |
| tecnico_mandante | string | Técnico do mandante | nomes de treinadores |
| tecnico_visitante | string | Técnico do visitante | nomes de treinadores |
| vencedor | string | Nome do time vencedor, ou "-" para empate (bruto) | nome de clube ou "-" |
| arena | string | Estádio onde a partida ocorreu | nomes de estádios |
| mandante_Placar | int | Gols marcados pelo mandante | ≥ 0 |
| visitante_Placar | int | Gols marcados pelo visitante | ≥ 0 |
| mandante_Estado | string | UF do mandante | siglas de estado (SP, RJ, MG...) |
| visitante_Estado | string | UF do visitante | siglas de estado |
| arrecadacao | double | Renda/arrecadação da partida (quando disponível) | valores em R$, pode ter nulos |
| _data_ingestao | timestamp | Metadado de controle: quando o registro foi carregado | timestamp de carga |
| _fonte | string | Metadado de controle: arquivo de origem | "campeonato-brasileiro-full.csv" |

### `bronze.gols`
Contexto: um registro por gol marcado em cada partida (`campeonato-brasileiro-gols.csv`).

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| partida_id | int | Referência à partida (junta com `partidas.ID`) | inteiro positivo |
| rodata | int | Rodada da partida | 1–38 |
| clube | string | Time que marcou o gol | nomes de clubes |
| atleta | string | Jogador que marcou | nomes de jogadores |
| minuto | string | Minuto do gol (pode incluir acréscimo, ex: "45+2") | texto numérico |
| tipo_de_gol | string | Tipo do gol (pênalti, contra etc., quando informado) | categorias, pode ter nulos |
| _data_ingestao | timestamp | Metadado de controle | timestamp de carga |
| _fonte | string | Metadado de controle | "campeonato-brasileiro-gols.csv" |

### `bronze.cartoes`
Contexto: um registro por cartão (amarelo/vermelho) aplicado em cada partida (`campeonato-brasileiro-cartoes.csv`).

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| partida_id | int | Referência à partida | inteiro positivo |
| rodata | int | Rodada da partida | 1–38 |
| clube | string | Time do jogador punido | nomes de clubes |
| cartao | string | Tipo de cartão | "Amarelo" (19.867 registros), "Vermelho" (1.086 registros) — confirmado na etapa de Qualidade de Dados |
| atleta | string | Jogador punido | nomes de jogadores |
| num_camisa | int | Número da camisa do jogador | 1–99, pode ter nulos |
| posicao | string | Posição do jogador em campo | categorias (ex: Zagueiro, Meio-campo) |
| minuto | string | Minuto do cartão | texto numérico |
| _data_ingestao | timestamp | Metadado de controle | timestamp de carga |
| _fonte | string | Metadado de controle | "campeonato-brasileiro-cartoes.csv" |

### `bronze.estatisticas`
Contexto: estatísticas avançadas por jogo/time (`campeonato-brasileiro-estatisticas-full.csv`), disponível apenas para parte das temporadas.

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| partida_id | int | Referência à partida | inteiro positivo |
| rodata | int | Rodada da partida | 1–38 |
| clube | string | Time a que a estatística se refere | nomes de clubes |
| chutes | int | Total de chutes | ≥ 0 |
| chutes_no_alvo | int | Chutes no alvo | ≥ 0, ≤ chutes |
| posse_de_bola | string | % de posse de bola (formato texto, ex: "55%") | 0-100% |
| passes | int | Total de passes certos | ≥ 0 |
| precisao_passes | string | % de precisão de passe (formato texto) | 0-100% |
| faltas | int | Faltas cometidas | ≥ 0 |
| cartao_amarelo | int | Total de cartões amarelos do time na partida | ≥ 0 |
| cartao_vermelho | int | Total de cartões vermelhos do time na partida | ≥ 0 |
| impedimentos | int | Total de impedimentos marcados | ≥ 0 |
| escanteios | int | Total de escanteios | ≥ 0 |
| _data_ingestao | timestamp | Metadado de controle | timestamp de carga |
| _fonte | string | Metadado de controle | "campeonato-brasileiro-estatisticas-full.csv" |

## Camada Silver

Mesma estrutura de colunas das tabelas Bronze correspondentes, com as seguintes transformações:
- `silver.partidas`: `mandante`, `visitante` e `vencedor` padronizados (trim + mapa de correção de grafias); `vencedor` com `"-"` convertido para `"Empate"`; duplicatas removidas por `ID`.
- `silver.gols`, `silver.cartoes`, `silver.estatisticas`: coluna `clube` padronizada (trim + mapa de correção); duplicatas removidas.

Linhagem: derivadas das tabelas `bronze.*` de mesmo nome via notebook `02_transformacao_silver.py`.

## Camada Gold

### `gold.fato_partidas`
Contexto: partidas enriquecidas com colunas derivadas para análise.
Linhagem: derivada de `silver.partidas`.

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| (todas as colunas de `silver.partidas`) | | | |
| resultado | string | Resultado calculado a partir do placar | "mandante", "visitante", "empate" |
| ano | int | Ano da partida, extraído de `data` | 2003–ano mais recente |
| sem_publico | boolean | Indica se a partida ocorreu em 2020 ou 2021 (pandemia) | true / false |
| total_gols | int | Soma de `mandante_Placar` + `visitante_Placar` | ≥ 0 |

### `gold.agg_aproveitamento_mandante_visitante`
Contexto: aproveitamento (% de vitórias) de cada time, por ano, separando jogos em casa e fora.
Linhagem: agregação de `gold.fato_partidas`.

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| time | string | Nome do clube | nomes de clubes padronizados |
| ano | int | Temporada | 2003–ano mais recente |
| jogos_em_casa | int | Jogos como mandante no ano | ≥ 0 |
| vitorias_em_casa | int | Vitórias como mandante no ano | ≥ 0 |
| aproveitamento_em_casa | double | vitorias_em_casa / jogos_em_casa | 0.0–1.0 |
| jogos_fora | int | Jogos como visitante no ano | ≥ 0 |
| vitorias_fora | int | Vitórias como visitante no ano | ≥ 0 |
| aproveitamento_fora | double | vitorias_fora / jogos_fora | 0.0–1.0 |

### `gold.agg_cartoes_resultado`
Contexto: cruzamento entre cartão vermelho recebido (mandante e/ou visitante) e o resultado da partida.
Linhagem: junção de `silver.cartoes` (filtrado por cartão vermelho) com `gold.fato_partidas`.

| Coluna | Tipo | Descrição | Domínio de valores |
|---|---|---|---|
| ID | int | Identificador da partida | inteiro positivo |
| mandante | string | Time mandante | nomes de clubes |
| visitante | string | Time visitante | nomes de clubes |
| resultado | string | Resultado da partida | "mandante", "visitante", "empate" |
| ano | int | Temporada | 2003–ano mais recente |
| mandante_recebeu_vermelho | boolean | Se o mandante recebeu cartão vermelho na partida | true / false |
| visitante_recebeu_vermelho | boolean | Se o visitante recebeu cartão vermelho na partida | true / false |
