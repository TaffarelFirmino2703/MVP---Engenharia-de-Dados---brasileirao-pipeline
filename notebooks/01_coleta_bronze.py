# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Coleta (Bronze)
# MAGIC
# MAGIC Este notebook lê os arquivos CSV brutos do Campeonato Brasileiro (enviados ao Volume do Unity Catalog)
# MAGIC e grava cada um como uma tabela Delta na camada **Bronze**, sem nenhuma transformação de conteúdo.
# MAGIC
# MAGIC Pré-requisitos:
# MAGIC - Schemas `bronze`, `silver`, `gold` já criados no catálogo.
# MAGIC - Volume `bronze.dados_brutos` criado e com os CSVs do dataset já enviados.
# MAGIC
# MAGIC Dataset: https://www.kaggle.com/datasets/adaoduque/campeonato-brasileiro-de-futebol

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

catalog = "workspace"
schema_bronze = "bronze"
volume = "dados_brutos"
path_volume = f"/Volumes/{catalog}/{schema_bronze}/{volume}"

# TODO: confira os nomes reais dos arquivos depois de extrair o .zip baixado do Kaggle
arquivos = {
    "partidas": "campeonato-brasileiro-full.csv",
    "gols": "campeonato-brasileiro-gols.csv",
    "cartoes": "campeonato-brasileiro-cartoes.csv",
    "estatisticas": "campeonato-brasileiro-estatisticas-full.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura e gravação na camada Bronze
# MAGIC
# MAGIC Cada tabela recebe metadados de controle (`_data_ingestao`, `_fonte`) mas o conteúdo original
# MAGIC não é alterado — isso preserva a rastreabilidade exigida na camada Bronze.

# COMMAND ----------

from pyspark.sql import functions as F

resumo = []

for nome_tabela, nome_arquivo in arquivos.items():
    caminho = f"{path_volume}/{nome_arquivo}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(caminho)
        .withColumn("_data_ingestao", F.current_timestamp())
        .withColumn("_fonte", F.lit(nome_arquivo))
    )

    (
        df.write
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema_bronze}.{nome_tabela}")
    )

    resumo.append((nome_tabela, nome_arquivo, df.count(), len(df.columns)))
    print(f"Tabela {catalog}.{schema_bronze}.{nome_tabela} criada com {df.count()} linhas e {len(df.columns)} colunas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência rápida
# MAGIC
# MAGIC Rode as células abaixo, confira o schema inferido de cada tabela e tire um screenshot
# MAGIC de pelo menos uma delas para usar como evidência no README (tópico "Carga dos Dados").

# COMMAND ----------

display(spark.table(f"{catalog}.{schema_bronze}.partidas").limit(10))

# COMMAND ----------

spark.table(f"{catalog}.{schema_bronze}.partidas").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da carga

# COMMAND ----------

display(spark.createDataFrame(resumo, ["tabela", "arquivo_origem", "linhas", "colunas"]))
