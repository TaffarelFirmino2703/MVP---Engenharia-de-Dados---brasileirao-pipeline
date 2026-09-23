# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Modelagem (Gold)
# MAGIC
# MAGIC Aqui construímos as tabelas modeladas para responder diretamente as perguntas de negócio
# MAGIC definidas na Etapa 1 (objetivo).
# MAGIC
# MAGIC Tabelas planejadas:
# MAGIC - `fato_partidas`: uma linha por partida, com colunas derivadas úteis para as análises
# MAGIC - `agg_aproveitamento_mandante_visitante`: aproveitamento por time, por ano, separando mando/fora
# MAGIC - `agg_cartoes_resultado`: cruzamento entre cartão vermelho recebido e resultado da partida

# COMMAND ----------

catalog = "workspace"
schema_silver = "silver"
schema_gold = "gold"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fato_partidas`
# MAGIC
# MAGIC Enriquece a tabela de partidas com colunas derivadas:
# MAGIC - `resultado` (mandante / visitante / empate) — calculado a partir do placar, para garantir
# MAGIC   consistência independente do campo `vencedor` original.
# MAGIC - `ano` (extraído de `data`, para agrupar por temporada)
# MAGIC - `sem_publico` (flag para 2020-2021, útil na pergunta sobre efeito da pandemia)
# MAGIC - `total_gols` (soma dos dois placares, útil para a pergunta 6)

# COMMAND ----------

df_partidas = spark.table(f"{catalog}.{schema_silver}.partidas")

df_fato_partidas = (
    df_partidas
    .withColumn(
        "resultado",
        F.when(F.col("mandante_Placar") > F.col("visitante_Placar"), F.lit("mandante"))
         .when(F.col("mandante_Placar") < F.col("visitante_Placar"), F.lit("visitante"))
         .otherwise(F.lit("empate"))
    )
    .withColumn("ano", F.year("data"))
    .withColumn("sem_publico", F.col("ano").isin(2020, 2021))
    .withColumn("total_gols", F.col("mandante_Placar") + F.col("visitante_Placar"))
)

(
    df_fato_partidas.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_gold}.fato_partidas")
)

print(f"{catalog}.{schema_gold}.fato_partidas: {df_fato_partidas.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agg_aproveitamento_mandante_visitante`
# MAGIC
# MAGIC Aproveitamento (% de vitórias) de cada time jogando em casa e fora, por temporada.
# MAGIC Alimenta as perguntas 1 (vantagem de mando) e 5 (melhor aproveitamento por time).

# COMMAND ----------

df_como_mandante = (
    df_fato_partidas
    .groupBy("mandante", "ano")
    .agg(
        F.count("*").alias("jogos_em_casa"),
        F.sum(F.when(F.col("resultado") == "mandante", 1).otherwise(0)).alias("vitorias_em_casa"),
    )
    .withColumn("aproveitamento_em_casa", F.round(F.col("vitorias_em_casa") / F.col("jogos_em_casa"), 3))
    .withColumnRenamed("mandante", "time")
)

df_como_visitante = (
    df_fato_partidas
    .groupBy("visitante", "ano")
    .agg(
        F.count("*").alias("jogos_fora"),
        F.sum(F.when(F.col("resultado") == "visitante", 1).otherwise(0)).alias("vitorias_fora"),
    )
    .withColumn("aproveitamento_fora", F.round(F.col("vitorias_fora") / F.col("jogos_fora"), 3))
    .withColumnRenamed("visitante", "time")
)

df_aproveitamento = df_como_mandante.join(df_como_visitante, on=["time", "ano"], how="outer")

(
    df_aproveitamento.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_gold}.agg_aproveitamento_mandante_visitante")
)

print(f"{catalog}.{schema_gold}.agg_aproveitamento_mandante_visitante: {df_aproveitamento.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agg_cartoes_resultado`
# MAGIC
# MAGIC Cruza cada partida com a ocorrência de cartão vermelho (mandante e/ou visitante) e o resultado
# MAGIC final. Alimenta a pergunta 3 (cartão vermelho x chance de vitória).
# MAGIC
# MAGIC **Suposição a confirmar**: a coluna `cartao` da tabela `cartoes` usa o valor `"Vermelho"` para
# MAGIC cartão vermelho. Confira os valores distintos dessa coluna no notebook `04_qualidade_dados.py`
# MAGIC e ajuste o filtro abaixo se o valor real for diferente (ex: pode incluir "Vermelho2" para segundo amarelo).

# COMMAND ----------

df_cartoes = spark.table(f"{catalog}.{schema_silver}.cartoes")

# TODO: confirme os valores reais da coluna `cartao` e ajuste o filtro se necessário
df_vermelhos_por_partida = (
    df_cartoes
    .filter(F.col("cartao").contains("Vermelho"))
    .groupBy("partida_id")
    .agg(F.collect_set("clube").alias("times_com_vermelho"))
)

df_cartoes_resultado = (
    df_fato_partidas
    .join(df_vermelhos_por_partida, df_fato_partidas.ID == df_vermelhos_por_partida.partida_id, "left")
    .withColumn("mandante_recebeu_vermelho", F.coalesce(F.array_contains(F.col("times_com_vermelho"), F.col("mandante")), F.lit(False)))
    .withColumn("visitante_recebeu_vermelho", F.coalesce(F.array_contains(F.col("times_com_vermelho"), F.col("visitante")), F.lit(False)))
    .select(
        "ID", "mandante", "visitante", "resultado", "ano",
        "mandante_recebeu_vermelho", "visitante_recebeu_vermelho",
    )
)

(
    df_cartoes_resultado.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_gold}.agg_cartoes_resultado")
)

print(f"{catalog}.{schema_gold}.agg_cartoes_resultado: {df_cartoes_resultado.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

display(spark.table(f"{catalog}.{schema_gold}.fato_partidas").limit(10))

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema_gold}.agg_aproveitamento_mandante_visitante")
    .orderBy(F.desc("aproveitamento_em_casa"))
    .limit(10)
)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema_gold}.agg_cartoes_resultado").limit(10))
