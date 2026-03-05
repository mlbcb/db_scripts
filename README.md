# DB Scripts — Benchmarking e Análise de Bases de Dados

Conjunto de scripts Python para testar, comparar e analisar o desempenho de 16 bases de dados (SQL e NoSQL). Todas as credenciais e configurações são geridas com uso do [Doppler](https://www.doppler.com/).

## Estrutura dos Ficheiros

| Ficheiro | Descrição |
|---|---|
| `test_connection.py` | Testa a conectividade com todas as bases de dados configuradas no Doppler |
| `script_escrita.py` | Insere dados (a partir de um ficheiro SQL) em todas as bases de dados e mede tempos de conexão e inserção |
| `script_leitura.py` | Executa consultas de leitura (cálculo de consumo de água) em todas as bases de dados e mede tempos |
| `check_storage.py` | Verifica o espaço de armazenamento utilizado por cada base de dados |
| `build_graph.py` | Gera gráficos dos tempos de **inserção** (a partir de `insert_times.csv`) |
| `build_graph_connection.py` | Gera gráficos dos tempos de **conexão** (a partir de `connection_times.csv`) |
| `build_graph_consumption.py` | Gera gráficos dos tempos de **consulta/consumo** (a partir de `consumption_times.csv`) |
| `test.sql` | Ficheiro SQL de exemplo com dados de leitura de contadores de água (tabela `aqualog`) |

## Bases de Dados Suportadas

- **SQL**: PostgreSQL, CrateDB, MySQL, MariaDB, TiDB
- **NoSQL**: MongoDB, Neo4j, HarperDB, AstraDB (Cassandra)
- **Vetoriais**: Milvus (Zilliz Cloud), Qdrant
- **Séries Temporais**: InfluxDB

## Variáveis de Ambiente

### Obrigatórias (todos os scripts)

| Variável | Descrição |
|---|---|
| `DOPPLER_TOKEN` | Token de acesso à API do Doppler (formato: `dp.xxx`) |
| `DOPPLER_PROJECT` | Nome do projeto no Doppler que contém as configurações das bases de dados |

### Adicionais (apenas `script_escrita.py`)

| Variável | Descrição |
|---|---|
| `SQL_FILE` | Caminho para o ficheiro SQL com os dados a inserir (ex: `test.sql`) |
| `TABLE_NAME` | Nome da tabela de destino nas bases de dados (ex: `aqualog`) |
| `COLUMNS` | *(Opcional)* Nomes das colunas separados por vírgula, caso não sejam detetados automaticamente |

### Adicionais (apenas `script_leitura.py`)

| Variável | Descrição |
|---|---|
| `TABLE_NAME` | Nome da tabela/coleção a consultar |

### Adicionais (apenas `check_storage.py`)

| Variável | Descrição |
|---|---|
| `TABLE_NAME` | *(Opcional)* Nome da tabela para bases de dados que requerem (HarperDB, Qdrant, Milvus, AstraDB) |

> **Nota:** Os scripts também suportam execução no **Google Colab**, obtendo as variáveis através de `google.colab.userdata` caso as variáveis de ambiente não estejam definidas.

## Configuração no Doppler

Cada **config** (configuração/environment) no projeto Doppler deve conter as seguintes variáveis (conforme o tipo de base de dados):

| Variável Doppler | Descrição |
|---|---|
| `DB_TYPE` | Tipo da base de dados (`postgres`, `mysql`, `mongodb`, `neo4j`, `influxdb`, `qdrant`, `milvus`, `astradb`, `harperdb`, `cratedb`, `tidb`, `mariadb`) |
| `DB_HOST` | Hostname do servidor (SQL) |
| `DB_PORT` | Porta (SQL — predefinição: `5432` para Postgres, `3306` para MySQL) |
| `DB_USER` | Utilizador |
| `DB_PASSWORD` | Palavra-passe |
| `DB_NAME` | Nome da base de dados |
| `DB_URL` | URL de conexão (MongoDB, Neo4j, HarperDB, AstraDB, Milvus, Qdrant, InfluxDB) |
| `DB_TOKEN` | Token de autenticação (AstraDB, Milvus, InfluxDB) |
| `DB_ORG` | Organização (InfluxDB) |
| `DB_BUCKET` | Bucket (InfluxDB) |
| `DB_KEYSPACE` | Keyspace (AstraDB) |
| `DB_SCHEMA` | Schema (HarperDB — predefinição: `data`) |
| `API_KEY` | Chave de API (Qdrant) |
| `CA_CERT` | Certificado CA em texto (para conexões SSL que requerem verificação) |
| `SSLMODE` | Modo SSL (`verify-full` por predefinição; `require` para conexões sem verificação de certificado) |

## Como Utilizar

### 1. Testar Conectividade

```bash
DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=meu_projeto python test_connection.py
```

Testa a ligação a todas as bases de dados configuradas e reporta quais tiveram sucesso ou falharam.

### 2. Inserir Dados (Escrita)

```bash
DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=meu_projeto SQL_FILE=test.sql TABLE_NAME=aqualog python script_escrita.py
```

- Faz o parsing do ficheiro SQL, extrai os dados das instruções `INSERT`
- Para bases NoSQL, converte os dados SQL em dicionários Python
- Insere os dados (limitados a 100 linhas) em **todas** as bases de dados configuradas
- Mede e regista os tempos de conexão e inserção em `insert_times.csv`

### 3. Consultar Dados (Leitura)

```bash
DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=meu_projeto TABLE_NAME=aqualog python script_leitura.py
```

- Calcula o consumo de água (`MAX(leitura_l) - MIN(leitura_l)`) agrupado por `alias`
- Adapta a consulta ao tipo de base de dados (SQL, Cypher, aggregation pipeline, Flux, REST API)
- Regista os tempos de consulta em `consumption_times.csv`

### 4. Verificar Armazenamento

```bash
DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=meu_projeto python check_storage.py
```

Mostra o espaço ocupado (em bytes legíveis) ou contagem de registos para cada base de dados.

### 5. Gerar Gráficos

```bash
python build_graph.py              # Gráficos de tempos de inserção
python build_graph_connection.py   # Gráficos de tempos de conexão
python build_graph_consumption.py  # Gráficos de tempos de consulta
```

Cada script gera **dois gráficos** (ficheiros `.png`):
1. **Gráfico de linhas** — Evolução dos tempos ao longo das execuções
2. **Gráfico de barras** — Média e desvio padrão, ordenados do menor para o maior

## Dependências

As dependências são instaladas **automaticamente** pelos scripts quando necessário. As principais são:

```
requests
pandas
matplotlib
clts_pcp
pg8000
pymysql
neo4j
harperdb
astrapy
pymongo
```

Para instalar manualmente:

```bash
pip install requests pandas matplotlib clts_pcp pg8000 pymysql neo4j harperdb astrapy pymongo
```

## Ficheiros Gerados

| Ficheiro | Gerado por |
|---|---|
| `insert_times.csv` | `script_escrita.py` |
| `connection_times.csv` | `script_escrita.py` |
| `consumption_times.csv` | `script_leitura.py` |
| `insert_times_graph.png` | `build_graph.py` |
| `insert_stats_graph.png` | `build_graph.py` |
| `connection_times_graph.png` | `build_graph_connection.py` |
| `connection_stats_graph.png` | `build_graph_connection.py` |
| `consumption_times_graph.png` | `build_graph_consumption.py` |
| `consumption_stats_graph.png` | `build_graph_consumption.py` |
