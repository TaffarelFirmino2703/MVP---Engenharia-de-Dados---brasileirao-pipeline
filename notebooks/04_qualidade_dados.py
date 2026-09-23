# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de Dados
# MAGIC
# MAGIC Checagens de completude, consistência, unicidade, acurácia e outliers sobre as tabelas Bronze.
# MAGIC Documente aqui os problemas encontrados e como foram tratados (esse texto vai quase direto
# MAGIC para o README, tópico "Qualidade de Dados").

# COMMAND ----------

catalog = "workspace"
schema_bronze = "bronze"
schema_silver = "silver"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completude
# MAGIC
# MAGIC Percentual de nulos por coluna, tabela a tabela.

# COMMAND ----------

def relatorio_nulos(df, nome_tabela):
    total = df.count()
    exprs = [
        (F.round(F.sum(F.col(c).isNull().cast("int")) / total * 100, 2)).alias(c)
        for c in df.columns
    ]
    print(f"--- % de nulos em {nome_tabela} (total de {total} linhas) ---")
    display(df.select(exprs))

for nome in ["partidas", "gols", "cartoes", "estatisticas"]:
    relatorio_nulos(spark.table(f"{catalog}.{schema_bronze}.{nome}"), nome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidade
# MAGIC
# MAGIC `partidas.ID` é a chave natural — não deveria haver dois registros com o mesmo ID.

# COMMAND ----------

df_partidas = spark.table(f"{catalog}.{schema_bronze}.partidas")

duplicadas = (
    df_partidas.groupBy("ID")
    .count()
    .filter(F.col("count") > 1)
)
print(f"IDs de partida duplicados: {duplicadas.count()}")
display(duplicadas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência - grafia de nomes de times
# MAGIC
# MAGIC Lista os nomes distintos de time em `partidas` (mandante+visitante) e em `gols`/`cartoes`/`estatisticas`
# MAGIC (coluna `clube`) para comparar e identificar grafias divergentes entre as tabelas.

# COMMAND ----------

times_partidas = (
    df_partidas.select(F.col("mandante").alias("time")).distinct()
    .union(df_partidas.select(F.col("visitante").alias("time")).distinct())
    .distinct()
    .orderBy("time")
)
print("--- Times em partidas (mandante/visitante) ---")
display(times_partidas)

# COMMAND ----------

times_gols = spark.table(f"{catalog}.{schema_bronze}.gols").select(F.col("clube").alias("time")).distinct().orderBy("time")
print("--- Times em gols (clube) ---")
display(times_gols)

# COMMAND ----------

times_cartoes = spark.table(f"{catalog}.{schema_bronze}.cartoes").select(F.col("clube").alias("time")).distinct().orderBy("time")
print("--- Times em cartoes (clube) ---")
display(times_cartoes)

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparação**: rode a célula abaixo para ver nomes que aparecem em `gols`/`cartoes` mas não
# MAGIC em `partidas` (ou vice-versa) — isso aponta diretamente as grafias divergentes.

# COMMAND ----------

nomes_partidas_set = set(r["time"] for r in times_partidas.collect())
nomes_gols_set = set(r["time"] for r in times_gols.collect())
nomes_cartoes_set = set(r["time"] for r in times_cartoes.collect())

print("Em gols mas não em partidas:", sorted(nomes_gols_set - nomes_partidas_set))
print("Em cartoes mas não em partidas:", sorted(nomes_cartoes_set - nomes_partidas_set))
print("Em partidas mas não em gols:", sorted(nomes_partidas_set - nomes_gols_set))
print("Em partidas mas não em cartoes:", sorted(nomes_partidas_set - nomes_cartoes_set))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ação**: para cada divergência encontrada acima, adicione uma entrada no `mapa_nomes_times`
# MAGIC do notebook `02_transformacao_silver.py` (ex: `"Atletico-MG": "Atlético-MG"`) e rode o notebook
# MAGIC 02 novamente para aplicar a correção na camada Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência - valores da coluna `cartao`
# MAGIC
# MAGIC Confirma os valores usados para cartão amarelo/vermelho (usado no notebook `03_modelagem_gold.py`).

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema_bronze}.cartoes")
    .groupBy("cartao").count().orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acurácia / Outliers
# MAGIC
# MAGIC Placares negativos ou anormalmente altos.

# COMMAND ----------

display(
    df_partidas.filter(
        (F.col("mandante_Placar") < 0) | (F.col("visitante_Placar") < 0) |
        (F.col("mandante_Placar") > 10) | (F.col("visitante_Placar") > 10)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo (vai para o README, seção "Qualidade de Dados")
# MAGIC
# MAGIC - **Completude**: as colunas-chave de todas as tabelas (identificadores, times, datas,
# MAGIC   placares) não têm nenhum valor nulo. Os nulos relevantes encontrados são pontuais e
# MAGIC   explicáveis pela natureza do dado, não por erro de coleta:
# MAGIC     - `partidas.formacao_mandante` / `formacao_visitante`: 54,28% nulos — esquema tático não
# MAGIC       era registrado em boa parte das temporadas mais antigas.
# MAGIC     - `gols.tipo_de_gol`: 88,05% nulos — só é preenchido para gols "especiais" (pênalti, contra
# MAGIC       etc.), a maioria dos gols é do tipo padrão e fica sem essa marcação.
# MAGIC     - `cartoes.atleta` (0,03%), `num_camisa` (1,84%) e `posicao` (5,72%): pequenas lacunas de
# MAGIC       preenchimento, não comprometem as análises de time/resultado.
# MAGIC   Nenhum desses nulos afeta as perguntas de negócio definidas na Etapa 1, já que elas usam
# MAGIC   principalmente times, placares e datas — colunas 100% completas.
# MAGIC - **Unicidade**: confirmado **0 IDs de partida duplicados** em `partidas` — a coluna `ID` é
# MAGIC   uma chave primária confiável.
# MAGIC - **Consistência (nomes de times)**: não foram encontradas divergências de grafia entre as
# MAGIC   tabelas (todo nome usado em `gols`/`cartoes` também existe em `partidas`, com a mesma grafia).
# MAGIC   Encontramos, porém, uma diferença de **cobertura temporal**: times como `Ipatinga`, `Paysandu`,
# MAGIC   `Barueri`, `Brasiliense`, `Santo Andre`, `Gremio Prudente`, `America-RN`, `Sao Caetano`,
# MAGIC   `Guarani` e `Nautico` aparecem em `partidas` mas nunca em `gols`/`cartoes` — indício de que
# MAGIC   esses arquivos têm cobertura temporal mais recente que `partidas` (esses times jogaram
# MAGIC   principalmente entre 2003-2010). Isso não exigiu correção de dados, mas é importante
# MAGIC   documentar como limitação: análises que cruzam `partidas` com `gols`/`cartoes` cobrem
# MAGIC   apenas o recorte temporal em que ambos os arquivos coincidem.
# MAGIC - **Consistência (valores de `cartao`)**: confirmado apenas 2 valores — `Amarelo` (19.867
# MAGIC   registros) e `Vermelho` (1.086 registros). Nenhum valor inesperado.
# MAGIC - **Acurácia / Outliers**: nenhuma partida com placar negativo ou acima de 10 gols — os
# MAGIC   valores de `mandante_Placar`/`visitante_Placar` estão dentro do intervalo esperado para
# MAGIC   uma partida de futebol.
