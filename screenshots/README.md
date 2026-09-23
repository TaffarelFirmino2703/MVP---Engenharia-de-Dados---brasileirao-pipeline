# Screenshots

Salve aqui os prints do seu workspace Databricks, exatamente com os nomes de arquivo abaixo.
O `README.md` principal já referencia essas imagens — assim que você salvar cada arquivo com o
nome certo nesta pasta, elas aparecem automaticamente ao visualizar o README (no GitHub ou no
VS Code/editor com preview de markdown).

Formato: PNG ou JPG, não precisa redimensionar. Pode ser print de tela inteira ou só da área
relevante (célula + resultado).

| Arquivo | O que capturar |
|---|---|
| `01_bronze_tabelas.png` | Resultado do notebook `01_coleta_bronze.py` mostrando as 4 tabelas criadas (`partidas`, `gols`, `cartoes`, `estatisticas`) com a contagem de linhas de cada uma. |
| `02_catalogo_unity.png` | Painel do **Catalog** no Databricks mostrando o catálogo `workspace` expandido, com os schemas `bronze`, `silver` e `gold` e suas tabelas visíveis na árvore. |
| `03_silver_gold_tabelas.png` | Um print do notebook `02_transformacao_silver.py` (tabela `silver.partidas` exibida) e/ou do `03_modelagem_gold.py` (tabela `fato_partidas` ou uma das agregações) — pode ser dois prints ou um só com as duas células visíveis. |
| `04_qualidade_dados.png` | Resultado das checagens do `04_qualidade_dados.py` — o print de completude (% de nulos) e/ou o de unicidade (0 IDs duplicados) já servem, pode reaproveitar os que você já tirou. |
| `05_pergunta1.png` até `05_pergunta6.png` | Um print por pergunta do `05_analise.py`, mostrando a célula com o resultado (a tabela/gráfico do `display()`). Pode reaproveitar os prints que você já me mandou nesta conversa. |
| `05_bonus_posse.png` | Print do resultado da seção "Bônus - Posse de bola x resultado" (o mesmo print que você já mandou, com a tabela `time_com_mais_posse`). |

Se preferir usar nomes ou uma organização diferente, só ajuste os links de imagem correspondentes
no `README.md` (procure por `screenshots/` no arquivo).
