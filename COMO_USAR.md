# Como usar este pacote

Este é o esqueleto do repositório do seu MVP. Ele já vem no formato que o Databricks reconhece
como notebook ao importar (`# Databricks notebook source` + `# COMMAND ----------`).

## Opção A1 — Sem usar terminal (tudo pelo navegador + Databricks)

Use este caminho se você não tem `git` instalado ou não quer mexer com linha de comando.

1. Crie uma conta no GitHub (se ainda não tiver) e, logado, clique em **New repository**
   (botão "+" no canto superior direito → **New repository**).
   - Nome sugerido: `mvp-brasileirao-pipeline`
   - Marque como **Public** (o trabalho exige repositório público)
   - **Não** marque "Add a README file" — deixe o repositório vazio
   - Clique em **Create repository** e copie a URL que aparece (ex.: `https://github.com/SEU_USUARIO/mvp-brasileirao-pipeline.git`)
2. No Databricks: **Workspace → Repos → Add Repo**, cole essa URL e confirme. Isso cria um
   Databricks Repo vinculado ao seu repositório (ainda vazio) no GitHub.
3. Dentro desse Repo no Databricks, use **Import** (clique direito → Import) para trazer os
   arquivos da pasta `notebooks/` deste pacote (os `.py`), e também suba manualmente o
   `README.md`, `docs/catalogo_de_dados.md`, `screenshots/` (com os prints já salvos) e os
   demais arquivos — pode arrastar/soltar ou usar o botão de upload do próprio navegador de
   arquivos do Databricks.
4. Com tudo dentro do Repo, use o ícone de **Git** no canto superior do Databricks (mostra o
   nome da branch, ex. `main`) → escreva uma mensagem de commit (ex. "Estrutura inicial do MVP")
   → **Commit & Push**. Isso envia os arquivos direto para o GitHub, sem precisar de terminal.

## Opção A2 — Levar para o GitHub via terminal (se você já usa git)

1. Crie um repositório público no GitHub (mesmo passo 1 da Opção A1).
2. Extraia este pacote e suba o conteúdo para o repositório:
   ```
   cd mvp_brasileirao_repo
   git init
   git add .
   git commit -m "Estrutura inicial do MVP"
   git branch -M main
   git remote add origin https://github.com/SEU_USUARIO/mvp-brasileirao-pipeline.git
   git push -u origin main
   ```
3. No Databricks: **Workspace → Repos → Add Repo**, cole a URL do seu repositório.
4. Os notebooks da pasta `notebooks/` já vão aparecer prontos para abrir e rodar.

## Opção B — Importar direto no Databricks (mais rápido para testar)

1. No Databricks: **Workspace → Import**.
2. Selecione "File" e envie cada arquivo `.py` da pasta `notebooks/` (um de cada vez, ou em lote).
3. O Databricks já reconhece o formato e recria as células automaticamente.

## Ordem de execução

1. `01_coleta_bronze.py` — depois de já ter criado os schemas/volume e feito upload dos CSVs (veja o guia de implantação).
2. `02_transformacao_silver.py` — ajuste os `# TODO` com os nomes reais de coluna que aparecerem no schema.
3. `03_modelagem_gold.py`
4. `04_qualidade_dados.py`
5. `05_analise.py`

Conforme for rodando e ajustando os `# TODO`, vá preenchendo o `README.md` e o `docs/catalogo_de_dados.md` — ambos já estão estruturados na mesma ordem exigida pelos critérios de avaliação do trabalho.
