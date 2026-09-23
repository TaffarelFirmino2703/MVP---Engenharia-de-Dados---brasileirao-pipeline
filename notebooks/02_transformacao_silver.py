# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Transformação (Silver)
# MAGIC
# MAGIC Aqui limpamos e padronizamos cada tabela Bronze: tipagem correta, remoção de duplicatas,
# MAGIC tratamento de nulos e padronização de nomes de times.
# MAGIC
# MAGIC Schema real confirmado (via `printSchema()` na camada Bronze):
# MAGIC - **partidas**: ID, rodata, data, hora, mandante, visitante, formacao_mandante, formacao_visitante,
# MAGIC   tecnico_mandante, tecnico_visitante, vencedor, arena, mandante_Placar, visitante_Placar,
# MAGIC   mandante_Estado, visitante_Estado, arrecadacao
# MAGIC - **gols**: partida_id, rodata, clube, atleta, minuto, tipo_de_gol
# MAGIC - **cartoes**: partida_id, rodata, clube, cartao, atleta, num_camisa, posicao, minuto
# MAGIC - **estatisticas**: partida_id, rodata, clube, chutes, chutes_no_alvo, posse_de_bola, passes,
# MAGIC   precisao_passes, faltas, cartao_amarelo, cartao_vermelho, impedimentos, escanteios
# MAGIC
# MAGIC Observação: `data` e os placares já vêm tipados corretamente pelo `inferSchema` da camada Bronze
# MAGIC (date / integer), então não precisamos converter tipo aqui — só limpar e padronizar conteúdo.

# COMMAND ----------

catalog = "workspace"
schema_bronze = "bronze"
schema_silver = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de padronização de nomes de time
# MAGIC
# MAGIC Complete o `mapa_nomes_times` assim que rodar o notebook `04_qualidade_dados.py` e identificar
# MAGIC grafias divergentes reais (ex: o mesmo time escrito de formas diferentes entre `partidas`,
# MAGIC `gols` e `cartoes`).

# COMMAND ----------

from pyspark.sql import functions as F

# TODO: preencha conforme grafias divergentes forem encontradas na etapa de Qualidade de Dados
mapa_nomes_times = {
    # "Atletico-MG": "Atlético-MG",
    # "Atletico-PR": "Athletico-PR",
}

def padronizar_time(col):
    expr = F.trim(col)
    for errado, certo in mapa_nomes_times.items():
        expr = F.when(expr == errado, F.lit(certo)).otherwise(expr)
    return expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 1 - Tabela `partidas`
# MAGIC
# MAGIC - Padroniza nomes de `mandante` e `visitante`.
# MAGIC - Padroniza `vencedor`: no dado bruto ele já vem como nome do time vencedor, ou `"-"` para empate.
# MAGIC   Convertendo `"-"` para `"Empate"` deixa o valor mais claro para a análise.
# MAGIC - Remove duplicatas.

# COMMAND ----------

df_partidas = spark.table(f"{catalog}.{schema_bronze}.partidas")

df_partidas_silver = (
    df_partidas
    .withColumn("mandante", padronizar_time(F.col("mandante")))
    .withColumn("visitante", padronizar_time(F.col("visitante")))
    .withColumn(
        "vencedor",
        F.when(F.trim(F.col("vencedor")) == "-", F.lit("Empate"))
         .otherwise(padronizar_time(F.col("vencedor")))
    )
    .dropDuplicates(["ID"])
)

(
    df_partidas_silver.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_silver}.partidas")
)

print(f"{catalog}.{schema_silver}.partidas: {df_partidas_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 2 - Tabela `gols`

# COMMAND ----------

df_gols = spark.table(f"{catalog}.{schema_bronze}.gols")

df_gols_silver = (
    df_gols
    .withColumn("clube", padronizar_time(F.col("clube")))
    .dropDuplicates()
)

(
    df_gols_silver.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_silver}.gols")
)

print(f"{catalog}.{schema_silver}.gols: {df_gols_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 3 - Tabela `cartoes`

# COMMAND ----------

df_cartoes = spark.table(f"{catalog}.{schema_bronze}.cartoes")

df_cartoes_silver = (
    df_cartoes
    .withColumn("clube", padronizar_time(F.col("clube")))
    .dropDuplicates()
)

(
    df_cartoes_silver.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_silver}.cartoes")
)

print(f"{catalog}.{schema_silver}.cartoes: {df_cartoes_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 4 - Tabela `estatisticas`

# COMMAND ----------

df_estatisticas = spark.table(f"{catalog}.{schema_bronze}.estatisticas")

df_estatisticas_silver = (
    df_estatisticas
    .withColumn("clube", padronizar_time(F.col("clube")))
    .dropDuplicates()
)

(
    df_estatisticas_silver.write
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema_silver}.estatisticas")
)

print(f"{catalog}.{schema_silver}.estatisticas: {df_estatisticas_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência
# MAGIC
# MAGIC Tire screenshot de uma das tabelas Silver como evidência da transformação.

# COMMAND ----------

display(spark.table(f"{catalog}.{schema_silver}.partidas").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferir nomes de times distintos (ajuda a identificar grafias divergentes)
# MAGIC
# MAGIC Rode isto e compare a lista de `partidas` com a de `gols`/`cartoes`/`estatisticas`
# MAGIC (célula equivalente está em `04_qualidade_dados.py`, seção "Consistência").

# COMMAND ----------

times_partidas = (
    spark.table(f"{catalog}.{schema_silver}.partidas")
    .select(F.col("mandante").alias("time")).distinct()
    .union(spark.table(f"{catalog}.{schema_silver}.partidas").select(F.col("visitante").alias("time")).distinct())
    .distinct()
    .orderBy("time")
)
display(times_partidas)
