# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise Final
# MAGIC
# MAGIC Responde as 6 perguntas de negócio definidas na Etapa 1, usando as tabelas Gold.
# MAGIC Para cada pergunta: consulta técnica + discussão do que o resultado significa.

# COMMAND ----------

catalog = "workspace"
schema_gold = "gold"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_fato = spark.table(f"{catalog}.{schema_gold}.fato_partidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 - O mando de campo garante vantagem estatisticamente relevante?

# COMMAND ----------

display(
    df_fato.groupBy("resultado")
    .count()
    .withColumn("percentual", F.round(F.col("count") / df_fato.count() * 100, 1))
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** mandante venceu 49,6% das partidas, contra 23,9% do visitante e 26,4% de
# MAGIC empates. A vantagem é clara e consistente: o mandante vence mais que o dobro das vezes em
# MAGIC relação ao visitante, ao longo de mais de duas décadas de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 - Essa vantagem mudou durante 2020-2021 (jogos sem público)?

# COMMAND ----------

display(
    df_fato.groupBy("sem_publico", "resultado")
    .count()
    .withColumn(
        "percentual_no_grupo",
        F.round(F.col("count") / F.sum("count").over(Window.partitionBy("sem_publico")) * 100, 1)
    )
    .orderBy("sem_publico", "resultado")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** sim, e de forma reveladora: com público, o mandante venceu 50% das partidas;
# MAGIC sem público (2020-2021), essa taxa caiu para 45,4% — uma queda de 4,6 pontos percentuais. A
# MAGIC taxa de empates também subiu (de 26,2% para 29,1%). Isso sugere que parte da vantagem de
# MAGIC mando vem do apoio da torcida, embora o mandante ainda mantivesse vantagem sobre o visitante
# MAGIC mesmo sem público (45,4% vs. 25,5%).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 - Cartão vermelho reduz a chance de vitória?

# COMMAND ----------

df_cartoes_resultado = spark.table(f"{catalog}.{schema_gold}.agg_cartoes_resultado")

# Aproveitamento do mandante quando ELE recebeu vermelho vs quando NÃO recebeu
display(
    df_cartoes_resultado
    .groupBy("mandante_recebeu_vermelho")
    .agg(
        F.count("*").alias("total_jogos"),
        F.sum(F.when(F.col("resultado") == "mandante", 1).otherwise(0)).alias("vitorias_mandante"),
    )
    .withColumn("pct_vitoria_mandante", F.round(F.col("vitorias_mandante") / F.col("total_jogos") * 100, 1))
)

# COMMAND ----------

# Mesma lógica para o visitante
display(
    df_cartoes_resultado
    .groupBy("visitante_recebeu_vermelho")
    .agg(
        F.count("*").alias("total_jogos"),
        F.sum(F.when(F.col("resultado") == "visitante", 1).otherwise(0)).alias("vitorias_visitante"),
    )
    .withColumn("pct_vitoria_visitante", F.round(F.col("vitorias_visitante") / F.col("total_jogos") * 100, 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** de forma acentuada. Quando o mandante recebe cartão vermelho, sua taxa de
# MAGIC vitória cai de 50,7% para 27% (queda de quase 24 pontos percentuais). Quando o visitante
# MAGIC recebe vermelho, sua taxa cai de 24,5% para 15% (queda de 9,5 pontos). O efeito é mais forte
# MAGIC para o mandante — provavelmente porque, jogando em casa, ele tende a se lançar mais ao
# MAGIC ataque, e ficar com um jogador a menos custa mais caro nesse contexto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 - Sair na frente no placar aumenta a chance de vencer?
# MAGIC
# MAGIC Requer dados gol a gol (tabela `gols`, coluna `minuto`) cruzados com o resultado final.
# MAGIC Nota: `minuto` é string na tabela bronze/silver — pode ter valores como "45+2", então
# MAGIC extraia só a parte numérica antes de comparar.

# COMMAND ----------

schema_silver = "silver"
df_gols = spark.table(f"{catalog}.{schema_silver}.gols")

# extrai o minuto numérico (ignora acréscimos tipo "45+2" -> pega 45)
df_gols_minuto = df_gols.withColumn(
    "minuto_num",
    F.regexp_extract(F.col("minuto"), r"(\d+)", 1).cast("int")
)

# time que fez o primeiro gol de cada partida
w = Window.partitionBy("partida_id").orderBy("minuto_num")

df_primeiro_gol = (
    df_gols_minuto
    .withColumn("ordem", F.row_number().over(w))
    .filter(F.col("ordem") == 1)
    .select(F.col("partida_id"), F.col("clube").alias("time_primeiro_gol"), F.col("minuto_num"))
)

df_partidas_com_primeiro_gol = (
    df_fato
    .join(df_primeiro_gol, df_fato.ID == df_primeiro_gol.partida_id, "inner")
    .withColumn(
        "primeiro_gol_de",
        F.when(F.col("time_primeiro_gol") == F.col("mandante"), F.lit("mandante"))
         .when(F.col("time_primeiro_gol") == F.col("visitante"), F.lit("visitante"))
         .otherwise(F.lit("desconhecido"))
    )
)

display(
    df_partidas_com_primeiro_gol
    .groupBy("primeiro_gol_de")
    .agg(
        F.count("*").alias("total_jogos"),
        F.sum(F.when(F.col("resultado") == F.col("primeiro_gol_de"), 1).otherwise(0)).alias("venceu_apos_abrir_placar"),
    )
    .withColumn("pct_venceu", F.round(F.col("venceu_apos_abrir_placar") / F.col("total_jogos") * 100, 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** sim, fortemente: quando o mandante marca o primeiro gol, vence 77,1% dessas
# MAGIC partidas; quando o visitante marca primeiro, vence 59,3%. O dado reforça a vantagem de mando
# MAGIC observada na Pergunta 1: mesmo abrindo o placar fora de casa, o visitante converte essa
# MAGIC vantagem em vitória com menos frequência que o mandante na mesma situação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 - Quais times têm melhor aproveitamento como mandante e visitante?

# COMMAND ----------

df_aproveitamento = spark.table(f"{catalog}.{schema_gold}.agg_aproveitamento_mandante_visitante")

display(
    df_aproveitamento
    .groupBy("time")
    .agg(
        F.round(F.avg("aproveitamento_em_casa"), 3).alias("aproveitamento_medio_em_casa"),
        F.round(F.avg("aproveitamento_fora"), 3).alias("aproveitamento_medio_fora"),
        F.sum("jogos_em_casa").alias("total_jogos_em_casa"),
    )
    .filter(F.col("total_jogos_em_casa") >= 38)  # filtra times com amostra pequena (menos de 1 temporada em casa)
    .orderBy(F.desc("aproveitamento_medio_em_casa"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** o ranking de aproveitamento médio em casa (considerando apenas times com
# MAGIC pelo menos 38 jogos em casa no ano, para excluir amostras pequenas) é liderado por Grêmio
# MAGIC (58,7%), Palmeiras (58,3%) e Internacional (58%), seguidos por São Paulo, Flamengo,
# MAGIC Athletico-PR, Santos, Atlético-MG, Cruzeiro e Corinthians. O padrão é claro: os clubes
# MAGIC tradicionalmente mais fortes e com maiores torcidas concentram os melhores aproveitamentos em
# MAGIC casa, sugerindo que a vantagem de mando é amplificada pelo poder do elenco/torcida desses
# MAGIC clubes. Vale notar também casos como Paysandu e São Caetano — clubes menores com passagens
# MAGIC curtas pela Série A que mostram uma disparidade ainda maior entre o desempenho em casa e fora.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 - A média de gols por partida mudou ao longo das temporadas?

# COMMAND ----------

display(
    df_fato
    .groupBy("ano")
    .agg(F.round(F.avg("total_gols"), 2).alias("media_gols_por_partida"))
    .orderBy("ano")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** sim, houve uma tendência de queda: de 2,88 gols/partida em 2003 para 2,26 em
# MAGIC 2014, com leve recuperação depois (2,36-2,43 entre 2015-2017). Isso pode indicar uma evolução
# MAGIC tática para um futebol mais defensivo/pragmático ao longo do tempo, embora o dado precise ser
# MAGIC conferido também nas temporadas mais recentes do dataset para confirmar se a tendência se
# MAGIC mantém.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bônus - Posse de bola x resultado
# MAGIC
# MAGIC Pergunta extra (fora das 6 originais): o time com mais posse de bola em uma partida tem
# MAGIC mais chance de vencer? Usa a tabela `estatisticas`, que traz posse de bola por time por partida.
# MAGIC
# MAGIC Nota: `estatisticas` não estava no objetivo original — trate esta seção como uma exploração
# MAGIC adicional a mencionar na autoavaliação como "trabalho futuro" ou aprofundamento espontâneo,
# MAGIC e não como uma das 6 perguntas formais.

# COMMAND ----------

schema_silver = "silver"
df_estatisticas = spark.table(f"{catalog}.{schema_silver}.estatisticas")

# posse_de_bola vem como texto (ex: "55%"), mas algumas linhas trazem o texto literal "None"
# em vez de um nulo de verdade. Usamos try_cast para converter o que for válido e transformar
# em nulo (em vez de erro) o que não for um número - o modo ANSI do Databricks travaria com
# um .cast("double") comum diante de um valor como "None".
df_estat_num = df_estatisticas.withColumn(
    "posse_de_bola_pct",
    F.expr("try_cast(regexp_replace(posse_de_bola, '%', '') as double)")
)

# posse do mandante e do visitante, uma linha por partida
df_posse_mandante = (
    df_estat_num.alias("e")
    .join(df_fato.select("ID", "mandante", "visitante", "resultado").alias("p"),
          (F.col("e.partida_id") == F.col("p.ID")) & (F.col("e.clube") == F.col("p.mandante")))
    .select(F.col("p.ID"), F.col("e.posse_de_bola_pct").alias("posse_mandante"))
)

df_posse_visitante = (
    df_estat_num.alias("e")
    .join(df_fato.select("ID", "mandante", "visitante", "resultado").alias("p"),
          (F.col("e.partida_id") == F.col("p.ID")) & (F.col("e.clube") == F.col("p.visitante")))
    .select(F.col("p.ID"), F.col("e.posse_de_bola_pct").alias("posse_visitante"))
)

df_posse_resultado = (
    df_fato.select("ID", "resultado")
    .join(df_posse_mandante, "ID")
    .join(df_posse_visitante, "ID")
    .withColumn(
        "time_com_mais_posse",
        F.when(F.col("posse_mandante") > F.col("posse_visitante"), F.lit("mandante"))
         .when(F.col("posse_mandante") < F.col("posse_visitante"), F.lit("visitante"))
         .otherwise(F.lit("empate_posse"))
    )
)

total_com_join = df_posse_resultado.count()
total_com_posse_valida = df_posse_resultado.filter(
    F.col("posse_mandante").isNotNull() & F.col("posse_visitante").isNotNull()
).count()
print(f"Partidas com registro de posse de bola para os dois times: {total_com_join} (de {df_fato.count()} no total)")
print(f"Dessas, com valor de posse numericamente válido para os dois times (excluindo 'None'/nulos): {total_com_posse_valida}")

display(
    df_posse_resultado
    .filter(F.col("time_com_mais_posse") != "empate_posse")
    .groupBy("time_com_mais_posse")
    .agg(
        F.count("*").alias("total_jogos"),
        F.sum(F.when(F.col("resultado") == F.col("time_com_mais_posse"), 1).otherwise(0)).alias("venceu"),
    )
    .withColumn("pct_venceu", F.round(F.col("venceu") / F.col("total_jogos") * 100, 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão:** a tabela `estatisticas` tem uma linha para cada time em todas as 9.165 partidas
# MAGIC (cobertura de junção de 100%), mas o campo `posse_de_bola` só traz um valor numérico válido em
# MAGIC 3.784 partidas (41,3% do total) — nas demais, o campo vem como o texto literal `"None"`. Ou
# MAGIC seja, a limitação aqui não é de cobertura temporal (como a encontrada na etapa de Qualidade de
# MAGIC Dados entre `partidas` e `gols`/`cartoes`), e sim de preenchimento do próprio campo de posse
# MAGIC de bola dentro da tabela `estatisticas`.
# MAGIC
# MAGIC Dentro dessa amostra de 3.784 partidas com dado válido para os dois lados, o resultado é
# MAGIC contraintuitivo: quando o **mandante** teve mais posse de bola, ele venceu em apenas **43,1%**
# MAGIC dos casos (2.155 jogos) — abaixo dos 49,6% de aproveitamento geral do mandante encontrados na
# MAGIC Pergunta 1. Quando o **visitante** teve mais posse, venceu em **19,5%** dos casos (1.613 jogos) —
# MAGIC também abaixo do seu aproveitamento geral de 23,9%. Ou seja, nesta amostra, ter mais posse de
# MAGIC bola não aumentou a chance de vitória de nenhum dos dois lados; pelo contrário, esteve associado
# MAGIC a uma taxa de vitória *menor* que a média histórica de cada papel (mandante/visitante).
# MAGIC
# MAGIC Uma explicação plausível (comum na literatura de análise de futebol) é que posse de bola é, em
# MAGIC parte, sintoma de estar em desvantagem no placar: o time que está perdendo ou empatando tende a
# MAGIC ficar mais tempo com a bola tentando construir uma jogada de empate/virada, enquanto o time que
# MAGIC já está ganhando pode optar por jogar mais recuado e ceder posse deliberadamente. Isso sugere que
# MAGIC, pelo menos neste recorte de dados, a eficiência em converter chances (finalizações certeiras,
# MAGIC por exemplo) pode ser um fator mais determinante para o resultado do que o volume bruto de posse
# MAGIC de bola — uma boa pergunta para uma futura iteração deste MVP.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC Os dados mostram que a vantagem de jogar em casa no Brasileirão é real e mensurável (quase
# MAGIC 50% de vitórias do mandante), e que essa vantagem tem pelo menos duas fontes identificáveis:
# MAGIC o apoio da torcida (cai quando não há público) e a superioridade técnica dos clubes
# MAGIC tradicionalmente fortes (que dominam o ranking de aproveitamento em casa). O fator
# MAGIC disciplinar (cartão vermelho) é o que mais isoladamente impacta o resultado — perder um
# MAGIC jogador reduz a chance de vitória em até 24 pontos percentuais. O jogo parece ter ficado mais
# MAGIC fechado ao longo do tempo, com menos gols por partida nas temporadas mais recentes em relação
# MAGIC ao início dos anos 2000. E, de forma inesperada, a exploração bônus mostra que posse de bola
# MAGIC por si só não é um bom preditor de vitória — reforçando que fatores como mando de campo e
# MAGIC disciplina (cartões) têm um sinal mais forte sobre o resultado do que a estatística de posse.
