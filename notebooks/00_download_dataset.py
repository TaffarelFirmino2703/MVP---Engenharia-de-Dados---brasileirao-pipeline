# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Download do Dataset (opcional, via API do Kaggle)
# MAGIC
# MAGIC Este notebook baixa o dataset diretamente de dentro do Databricks, sem precisar baixar
# MAGIC no seu computador e depois subir manualmente. É opcional: se preferir, baixe o .zip do
# MAGIC Kaggle pelo navegador e suba os CSVs direto no Volume (ver COMO_USAR.md).
# MAGIC
# MAGIC ## Pré-requisito: token da API do Kaggle
# MAGIC 1. Acesse https://www.kaggle.com/settings
# MAGIC 2. Na seção "API", clique em **Create New Token** — isso baixa um arquivo `kaggle.json`
# MAGIC    com `username` e `key`.
# MAGIC 3. Preencha esses dois valores nas variáveis abaixo (ou, melhor, use Databricks Secrets
# MAGIC    em vez de deixar a chave em texto puro no notebook — veja nota ao final).

# COMMAND ----------

# MAGIC %pip install kaggle
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

# TODO: preencha com seus dados do kaggle.json (ou use dbutils.secrets, ver nota no final)
os.environ["KAGGLE_USERNAME"] = "SEU_USUARIO_KAGGLE"
os.environ["KAGGLE_KEY"] = "SUA_CHAVE_API_KAGGLE"

catalog = "workspace"
schema_bronze = "bronze"
volume = "dados_brutos"
path_volume = f"/Volumes/{catalog}/{schema_bronze}/{volume}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download e extração direto no Volume

# COMMAND ----------

import kaggle

kaggle.api.authenticate()
kaggle.api.dataset_download_files(
    "adaoduque/campeonato-brasileiro-de-futebol",
    path=path_volume,
    unzip=True,
)

print("Download concluído. Arquivos em:", path_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

display(dbutils.fs.ls(path_volume))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nota de segurança
# MAGIC
# MAGIC Não deixe usuário/chave do Kaggle em texto puro em um notebook que vai para um
# MAGIC repositório **público** no GitHub. Antes de commitar, prefira usar
# MAGIC [Databricks Secrets](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/):
# MAGIC
# MAGIC ```python
# MAGIC os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(scope="kaggle", key="username")
# MAGIC os.environ["KAGGLE_KEY"] = dbutils.secrets.get(scope="kaggle", key="key")
# MAGIC ```
# MAGIC
# MAGIC Ou simplesmente rode este notebook, confirme que os arquivos chegaram no Volume,
# MAGIC e depois apague as duas linhas com usuário/chave antes de dar commit no Git.
