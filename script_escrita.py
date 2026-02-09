"""
Este script analisa um ficheiro SQL e extrai as instruções INSERT e os dados contidos nelas.
Ele cria um ficheiro CSV com os dados extraídos e uma lista de instruções SQL.

Requisitos:
- pandas
- clts_pcp

Variáveis de ambiente:
- DOPPLER_TOKEN: Token de acesso Doppler (obrigatório)
- DOPPLER_PROJECT: Nome do projeto (obrigatório)
- SQL_FILE: Caminho para o ficheiro SQL (obrigatório)
- TABLE_NAME: Nome da tabela destino (obrigatório)

Uso: 
    python insert_analytics.py

O script gera um ficheiro CSV com os dados extraídos e uma lista de instruções SQL.
"""

import os, sys, subprocess, csv, random, re

# Tenta importar userdata do Colab (só funciona no Colab)
try:
    from google.colab import userdata
    IN_COLAB = True
except ImportError:
    IN_COLAB = False

try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests', '-q'])
    import requests

try:
    import clts_pcp as clts
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'clts_pcp', '-q'])
    import clts_pcp as clts

# Obter tokens: primeiro tenta variáveis de ambiente, depois Colab secrets
DOPPLER_TOKEN = os.environ.get('DOPPLER_TOKEN')
DOPPLER_PROJECT = os.environ.get('DOPPLER_PROJECT')
CSV_FILE = None
SQL_FILE = os.environ.get('SQL_FILE')
TABLE_NAME = os.environ.get('TABLE_NAME')

if not DOPPLER_TOKEN and IN_COLAB:
    DOPPLER_TOKEN = userdata.get('DOPPLER_TOKEN')
    DOPPLER_PROJECT = userdata.get('DOPPLER_PROJECT')
    DOPPLER_PROJECT = userdata.get('DOPPLER_PROJECT')
    CSV_FILE = None
    SQL_FILE = userdata.get('SQL_FILE')
    TABLE_NAME = userdata.get('TABLE_NAME')

if not DOPPLER_TOKEN: raise ValueError("DOPPLER_TOKEN não definida")
if not DOPPLER_PROJECT: raise ValueError("DOPPLER_PROJECT não definida")
if not SQL_FILE: raise ValueError("SQL_FILE deve indicar o caminho")
if not TABLE_NAME: raise ValueError("TABLE_NAME não definida")


"""
Para as bases de dados não relacionais (NoSQL), foi preciso fazer o parsing manual dos comandos 
INSERT do ficheiro SQL de origem, uma vez que estas bases de dados não interpretam SQL nativamente 
nem suportam a ingestão direta deste formato.

O script implementa uma lógica para processar o conteúdo textual do ficheiro, tendo em especial 
atenção o formato dos comandos SQL agrupados (bulk inserts), onde um único INSERT INTO contém 
múltiplos registos (como mostrado abaixo).

INSERT INTO aqualog VALUES (609346,'2024-11-11 12:27:47','00PC503015','131803',
'2024-11-08 23:00:00',0,3462110),(609355,'2024-11-11 12:27:51','00PC503015','131803',
'2024-11-08 22:00:00',0,3462110),(609366,'2024-11-11 12:27:56','00PC503015','131803',
'2024-11-08 21:00:00',0,3462110)...;

Para tal, a solução recorreu a expressões regulares (regex) e ao módulo csv para identificar e 
isolar cada instrução INSERT no meio do texto; iterar sobre os grupos de valores dentro de cada 
comando, extraindo cada linha individualmente, mesmo quando agrupadas; e converter os dados de 
tuplos SQL para dicionários Python (chave-valor), mapeando-os com os nomes das colunas detetados, 
tornando os dados compatíveis com as drivers das bases de dados NoSQL.
"""

insert_data = []
sql_statements = []
columns = []

def get_columns_from_header(content):
    """
    Tenta extrair os nomes das colunas da primeira linha do conteúdo SQL.
    A primeira linha pode ser um comentário (e.g., "-- id, nome, data") ou apenas os nomes.
    Ignora se a primeira linha for um comando INSERT.
    """
    line = content.splitlines()[0].strip()
    if line.upper().startswith("INSERT"): return []
    # Identificar cabeçalho comentado: -- col1,col2 ou apenas col1,col2
    return [c.strip() for c in line.lstrip('-').strip().split(',')]


def parse_sql_content(content, fallback_columns=None):
    """
    Analisa o conteúdo SQL para extrair instruções INSERT e dados estruturados.
    Separa os statements, identifica os INSERTs e converte os valores para dicionários.
    Retorna um tuplo: (lista de colunas, lista de dados, lista de statements SQL).
    """
    stmts, data, cols = [], [], fallback_columns or []
    
    # Regex para separar instruções por ponto e vírgula, respeitando aspas (simples/duplas/escapadas)
    # Corresponde a trechos de: não-ponto-e-vírgula-não-aspas OU string entre aspas
    stmt_pattern = re.compile(r"((?:[^;\"']|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*')+)")
    insert_pattern = re.compile(r"INSERT\s+INTO\s+([\w\.\"`']+)(?:\s*\((.*?)\))?\s*VALUES\s*(.*)", re.IGNORECASE | re.DOTALL)
    row_pattern = re.compile(r"\((?:[^()']|'(?:''|\\'|[^'])*')*\)")

    for stmt in [s.strip() for s in stmt_pattern.findall(content) if s.strip()]:
        stmts.append(stmt)
        match = insert_pattern.search(stmt)
        if not match: continue
        
        cols_str, values_part = match.group(2), match.group(3)
        current_cols = [c.strip().strip('"\'`') for c in cols_str.split(',')] if cols_str else cols
        
        # Analisar valores: (val1, val2), (val3, val4)
        for row_match in row_pattern.finditer(values_part):
            try:
                # Usar csv reader para lidar com valores respeitando aspas
                reader = csv.reader([row_match.group(0)[1:-1]], delimiter=',', quotechar="'", skipinitialspace=True)
                vals = next(reader)
                
                if current_cols and len(vals) == len(current_cols):
                    # criação concisa de dicionário
                    data.append({k: (None if v.upper() == 'NULL' else v) for k, v in zip(current_cols, vals)})
            except: pass

    return cols, data, stmts

if SQL_FILE and os.path.exists(SQL_FILE):
    print(f"Lendo SQL: {SQL_FILE}")
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Detetar colunas do header
    header_cols = get_columns_from_header(content)
    if header_cols:
         print(f"Colunas detetadas no cabeçalho: {header_cols}")
    
    # Checar se as colunas estão definidas na variável de ambiente COLUMNS
    if not header_cols and os.environ.get('COLUMNS'):
        header_cols = [c.strip() for c in os.environ.get('COLUMNS').split(',')]
        
    columns, insert_data, sql_statements = parse_sql_content(content, fallback_columns=header_cols)
    insert_data = insert_data[:100] # Limite para o número de linhas (para facilitar a análise)
    print(f"SQL parsed: {len(sql_statements)} statements, {len(insert_data)} inserts extracted.")

else:
    raise FileNotFoundError("Ficheiro SQL não encontrado")

if not insert_data and not sql_statements:
    raise ValueError("Nenhum dado encontrado para inserir. Verifique se o SQL contém INSERTs válidos e se as colunas estão definidas (no cabeçalho ou explicitamente).")

print(f"Dados carregados | Linhas: {len(insert_data) if insert_data else 0} | Stmts SQL: {len(sql_statements)} | Colunas: {columns}")

HEADERS = {"Authorization": f"Bearer {DOPPLER_TOKEN}"}

# Listar todas as configs do projeto
resp = requests.get("https://api.doppler.com/v3/configs", params={"project": DOPPLER_PROJECT}, headers=HEADERS)
resp.raise_for_status()
all_configs = resp.json().get("configs", [])

print(f"Projeto: {DOPPLER_PROJECT} | Configs: {len(all_configs)}\n")

clts.setcontext('Tempos de conexão e operação de inserção de 100 linhas')
tstart = clts.getts()

succeeded = 0
failed = 0

# Iterar sobre cada configuração do projeto
for cfg in all_configs:
    name = cfg["name"]
    print(f"\nConfig: {name}")
    
    # Obter os segredos (variáveis de ambiente) desta configuração no Doppler
    resp = requests.get("https://api.doppler.com/v3/configs/config/secrets", 
                        params={"project": DOPPLER_PROJECT, "config": name}, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Erro HTTP {resp.status_code} ao buscar secrets\n")
        failed += 1
        continue
    
    # Processar a resposta para um dicionário de segredos e obter o tipo de base de dados
    secrets = {k: v.get("computed", "") for k, v in resp.json().get("secrets", {}).items()}
    db_type = secrets.get("DB_TYPE", "").lower()
    
    if not db_type:
        print(f"DB_TYPE não definido")
        continue
    
    # Instalar dependências
    deps = {'postgres': 'pg8000', 'cratedb': 'pg8000', 'mysql': 'pymysql',
            'tidb': 'pymysql', 'mariadb': 'pymysql', 'neo4j': 'neo4j',
            'harperdb': 'harperdb', 'astradb': 'astrapy', 'mongodb': 'pymongo'}
    if db_type in deps:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', deps[db_type], '-q'], 
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except: pass
    
    try:
        # Definir a ligação SSL
        import ssl
        ca_cert = secrets.get("CA_CERT")
        ssl_mode = secrets.get("SSLMODE", "verify-full").lower()  # verify-full, verify-ca, require
        
        ssl_context = ssl.create_default_context()
        
        if ssl_mode == "require":
            # SSL encriptado mas sem verificação de certificado (menos seguro, mas foi 
            # a solução encontrada para o TimescaleDB)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            if ca_cert:
                # Carregar o certificado CA diretamente da string guardada no Doppler
                ssl_context.load_verify_locations(cadata=ca_cert)

        if db_type in ['postgres', 'cratedb']:
            import pg8000.native
            host = secrets.get("DB_HOST")
            port = int(secrets.get("DB_PORT", 5432))
            user = secrets.get("DB_USER")
            password = secrets.get("DB_PASSWORD")
            database = secrets.get("DB_NAME", "")
            
            t_conn = clts.getts()
            conn = pg8000.native.Connection(
                host=host, port=port, user=user,
                password=password, database=database, ssl_context=ssl_context,
                timeout=10)
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            # Inserir dados (tabela deve existir previamente)
            t_load = clts.getts()
            
            # Priorizar inserção estruturada se dados foram extraídos (resolve compatibilidade de dialectos)
            if insert_data:
                # Inserir dados linha a linha (no caso de failback ou NoSQL simulado em SQL)
                placeholders = ", ".join([f":{col}" for col in columns])
                cols_str = ", ".join([f'"{col}"' for col in columns])
                for row in insert_data:
                    conn.run(f'INSERT INTO "{TABLE_NAME}" ({cols_str}) VALUES ({placeholders})', **row)
            elif sql_statements:
                # Executar SQL diretamente (fallback)
                for stmt in sql_statements:
                    conn.run(stmt)
            
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)
            
            conn.close()

        elif db_type in ['mysql', 'tidb', 'mariadb']:
            import pymysql
            t_conn = clts.getts()
            conn = pymysql.connect(
                host=secrets.get("DB_HOST"),
                port=int(secrets.get("DB_PORT", 3306)),
                user=secrets.get("DB_USER"),
                password=secrets.get("DB_PASSWORD"),
                database=secrets.get("DB_NAME") or None,
                ssl=ssl_context,
                connect_timeout=10)
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            cursor = conn.cursor()
            
            # Inserir dados (tabela deve existir previamente)
            t_load = clts.getts()
            
            if insert_data:
                # Inserir dados
                placeholders = ", ".join(["%s"] * len(columns))
                cols_str = ", ".join([f'`{col}`' for col in columns])
                for row in insert_data:
                    values = [row.get(col, "") for col in columns]
                    cursor.execute(f'INSERT INTO `{TABLE_NAME}` ({cols_str}) VALUES ({placeholders})', values)
            elif sql_statements:
                 # Executar SQL diretamente
                 for stmt in sql_statements:
                     cursor.execute(stmt)
            
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)
            
            conn.commit()
            conn.close()

        elif db_type == 'neo4j':
            from neo4j import GraphDatabase
            db_url = secrets.get("DB_URL")
            db_user = secrets.get("DB_USER")
            db_password = secrets.get("DB_PASSWORD")
            
            t_conn = clts.getts()
            driver = GraphDatabase.driver(db_url, auth=(db_user, db_password),
                                          connection_timeout=10)
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            t_load = clts.getts()
            # Criar sessão e inserir nós
            with driver.session() as session:
                for row in insert_data:
                    # Construir propriedades do nó dinamicamente
                    props = ", ".join([f"{k}: ${k}" for k in row.keys()])
                    session.run(f"CREATE (n:{TABLE_NAME} {{{props}}})", **row)
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)
            
            driver.close()

        elif db_type == 'harperdb':
            import harperdb
            db_url = secrets.get("DB_URL")
            db_user = secrets.get("DB_USER")
            db_password = secrets.get("DB_PASSWORD")
            db_schema = secrets.get("DB_SCHEMA", "data")
            
            t_conn = clts.getts()
            hdb = harperdb.HarperDB(url=db_url, username=db_user, password=db_password)
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            t_load = clts.getts()
            
            # Inserir dados (schema e tabela devem existir previamente)
            hdb.insert(db_schema, TABLE_NAME, insert_data)
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)

        elif db_type == 'astradb':
            from astrapy import DataAPIClient
            db_url = secrets.get("DB_URL")
            db_token = secrets.get("DB_TOKEN")
            db_keyspace = secrets.get("DB_KEYSPACE")
            
            t_conn = clts.getts()
            db = DataAPIClient(db_token).get_database(db_url, keyspace=db_keyspace)
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            # Inserir dados (coleção deve existir previamente)
            t_load = clts.getts()
            
            collection = db.get_collection(TABLE_NAME)
            collection.insert_many(insert_data)
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)

        elif db_type == 'milvus':
            db_url = secrets.get("DB_URL")
            headers = {"Authorization": f"Bearer {secrets.get('DB_TOKEN')}",
                       "Content-Type": "application/json", "Accept": "application/json"}
            
            t_conn = clts.getts()
            r = requests.post(f"{db_url}/v2/vectordb/collections/describe", headers=headers, json={"collectionName": TABLE_NAME})
            if r.status_code != 200: print(f"Erro Milvus: {r.text}"); failed += 1; continue
            
            # Extrair nome do campo vetorial e dimensão
            f = next((x for x in r.json().get('data', {}).get('fields', []) if x.get('type') == 'FloatVector' or x.get('vectorField')), {})
            v_name, p = f.get('name'), f.get('params', {})
            v_dim = int(({x['key']: x['value'] for x in p} if isinstance(p, list) else p).get('dim', 0))

            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            t_load = clts.getts()
            
            # Inserção em lotes (batches) de 100 para otimizar a performance de rede e evitar limites de payload da API.
            for i in range(0, len(insert_data), 100):
                # Se o campo vetorial estiver em falta, gera um vetor aleatório
                batch = [{**row, v_name: [random.random() for _ in range(v_dim)]} 
                         if v_name and v_name not in row and v_dim else row 
                         for row in insert_data[i:i+100]]
                requests.post(f"{db_url}/v2/vectordb/entities/insert", headers=headers,
                              json={"collectionName": TABLE_NAME, "data": batch}).raise_for_status()

            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)

        elif db_type == 'mongodb':
            from pymongo import MongoClient
            db_url = secrets.get("DB_URL")
            db_name = secrets.get("DB_NAME", "default")
            
            t_conn = clts.getts()
            client = MongoClient(db_url, serverSelectionTimeoutMS=10000)
            clts.elapt[f"{name} ({db_type}) - connection"] = clts.deltat(t_conn)
            db = client[db_name]
            collection = db[TABLE_NAME]
            t_load = clts.getts()
            # Passar uma cópia para evitar verificar/modificar a lista original (pymongo adiciona _id in-place)
            collection.insert_many([row.copy() for row in insert_data])
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)
            client.close()

        elif db_type == 'influxdb':
            # InfluxDB usa line protocol - cada linha CSV vira um ponto
            db_host = secrets.get("DB_HOST", "").rstrip("/")
            db_org = secrets.get("DB_ORG")
            db_bucket = secrets.get("DB_BUCKET")
            db_token = secrets.get("DB_TOKEN")
            headers = {"Authorization": f"Token {db_token}", "Content-Type": "text/plain"}
            
            t_conn = clts.getts()
            # Verificar ligação com health check
            r = requests.get(f"{db_host}/health", headers=headers, timeout=10)
            r.raise_for_status()
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            # Converter CSV para line protocol
            t_load = clts.getts()
            lines = []
            
            # Auxiliar para escapar caracteres
            def escape_line_protocol(value):
                return str(value).replace('\\', '\\\\').replace('"', '\\"')

            import time
            # Usa o tempo atual em nanoseconds como base
            # Incrementa ligeiramente para cada linha para garantir exclusividade e evitar overwrite
            base_time_ns = time.time_ns()

            for i, row in enumerate(insert_data):
                fields = ",".join([f'{k}="{escape_line_protocol(v)}"' for k, v in row.items() if k != "time"])
                
                # Se o CSV tiver tempo, este é usado. Caso contrário, gera um timestamp único
                if "time" in row and row["time"]:
                     timestamp = row["time"]
                else:
                     timestamp = str(base_time_ns + i)
                
                line = f"{TABLE_NAME} {fields} {timestamp}"
                lines.append(line)
            
            # Escritas em lote para evitar limites de payload
            batch_size = 500
            for i in range(0, len(lines), batch_size):
                batch_data = "\n".join(lines[i:i+batch_size])
                r = requests.post(f"{db_host}/api/v2/write", 
                                  headers=headers, 
                                  params={"org": db_org, "bucket": db_bucket},
                                  data=batch_data)
                if r.status_code != 204:
                     print(f"Erro ao escrever lote {i//batch_size} no InfluxDB: {r.status_code} - {r.text}")
                     r.raise_for_status()

            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)


        elif db_type == 'qdrant':
            import uuid
            db_url = secrets.get("DB_URL", "").rstrip("/")
            headers = {"api-key": secrets.get("API_KEY"), "Content-Type": "application/json"}
            
            t_conn = clts.getts()
            # Obter configuração da coleção para entender a estrutura dos vetores esperados.
            # Isto é crucial porque o Qdrant suporta vetores únicos (sem nome) ou múltiplos vetores nomeados.
            r = requests.get(f"{db_url}/collections/{TABLE_NAME}", headers=headers)
            r.raise_for_status()
            cfg = r.json().get("result", {}).get("config", {}).get("params", {}).get("vectors", {})
            
            # Normalizar a configuração dos vetores para um formato padrão {nome: tamanho}.
            # O Qdrant pode devolver a configuração de três formas:
            # 1. Um inteiro (apenas o tamanho do vetor padrão).
            # 2. Um dicionário com "size" (configuração detalhada do vetor padrão).
            # 3. Um dicionário de dicionários (múltiplos vetores nomeados).
            # Usamos "" como chave para o vetor padrão (sem nome).
            targets = ({"" : cfg} if isinstance(cfg, int) else
                       {"" : int(cfg["size"])} if "size" in cfg else
                       {k: int(v["size"]) for k, v in cfg.items() if isinstance(v, dict) and "size" in v})
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            t_load = clts.getts()
            points = []
            
            # Preparar os pontos para inserção
            for row in insert_data:
                vec = row.get("vector")
                
                # Se os dados de entrada não tiverem vetores, geramos vetores aleatórios
                # com as dimensões corretas baseadas na configuração da coleção (targets).
                # Isto permite testar a inserção mesmo sem dados vetoriais reais.
                if not vec and targets:
                    vec = [random.random() for _ in range(targets[""])] if "" in targets else \
                          {k: [random.random() for _ in range(s)] for k, s in targets.items()}
                
                # Construir o objeto do ponto. O Qdrant exige um ID.
                # Se não houver ID nos dados, geramos um UUID aleatório.
                points.append({"id": row.get("id") or str(uuid.uuid4()), "payload": row, "vector": vec or {}})
            
            # Inserção em lotes (batches) de 100 pontos para eficiência.
            # O parâmetro 'wait=true' garante que a operação termina antes de continuarmos,
            # o que é importante para medir o tempo corretamente.
            for i in range(0, len(points), 100):
                requests.put(f"{db_url}/collections/{TABLE_NAME}/points", 
                             headers=headers, params={"wait": "true"},
                             json={"points": points[i:i+100]}).raise_for_status()
            clts.elapt[f"{name} ({db_type}) - carregar"] = clts.deltat(t_load)

        # Determinar contagem baseada no que foi realmente usado
        count = len(insert_data) if insert_data else (len(sql_statements) if sql_statements else 0)
        print(f"Dados inseridos em {db_type} com sucesso ({count} registos)")
        succeeded += 1
        
    except requests.exceptions.HTTPError as e:
        print(f"Erro HTTP em {db_type}: {e}")
        if e.response is not None:
             print(f"Detalhes: {e.response.text}")
        failed += 1

    except Exception as e:
        print(f"Erro ao inserir em {db_type}: {str(e)[:80]}")
        failed += 1

print(f"\nSucesso: {succeeded} | Erros: {failed}")
clts.elapt["Tempo total de execução"] = clts.deltat(tstart)
print("\n--- Resultados de Tempo ---")
clts.listtimes()

# ==============================================================================
# REGISTO CSV PARA ANÁLISE
# ==============================================================================

def update_csv_timings(filename, new_timings_dict):
    """
    Atualiza um ficheiro CSV com uma nova coluna de tempos.
    Linhas: Nome da Base de Dados
    Colunas: Database, Execution_1, Execution_2, ...
    """
    rows = {} # nome_bd -> lista de valores
    header = ["Database"]
    
    # Leitura do ficheiro existente
    if os.path.exists(filename):
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                pass # Ficheiro vazio
            
            for row in reader:
                if row:
                    rows[row[0]] = row[1:]

    # Determinar número da execução
    # Se o header só tem "Database", a próxima execução é a 1
    # Len(header) - 1 dá o número de execuções já registadas
    execution_num = len(header)
    new_header_col = f"Execution_{execution_num}"
    header.append(new_header_col)
    
    # Processar novas colunas
    all_dbs = set(rows.keys()) | set(new_timings_dict.keys())
    sorted_dbs = sorted(list(all_dbs))
    
    final_rows = []
    
    for db in sorted_dbs:
        current_vals = rows.get(db, [])
        
        # Preencher com valor vazio para execuções anteriores se for uma nova BD
        # O número de colunas de valores deve ser execution_num - 1 (a nova ainda não foi adicionada)
        missing_cols = (execution_num - 1) - len(current_vals)
        if missing_cols > 0:
             current_vals.extend([""] * missing_cols)
             
        new_val = new_timings_dict.get(db, "")
        if isinstance(new_val, (int, float)):
            new_val = round(new_val, 4)
            
        current_vals.append(new_val)
        final_rows.append([db] + current_vals)
        
    # Escrever ficheiro atualizado
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(final_rows)
    
    print(f"Atualizado {filename} com {new_header_col}")

# Separar dados de conexão e inserção
connection_data = {}
insert_data_metrics = {} # Renomeado para evitar conflito com insert_data (dados do SQL)

for key, value in clts.elapt.items():
    if isinstance(value, dict) and 'tt' in value:
        value = value['tt']
    
    # Chave esperada: "ConfigName (DbType) - operação"
    # Exemplo: "Production (postgres) - ligação"
    if " - " in key:
        db_info, op = key.rsplit(" - ", 1)
        
        if "ligação" in op.lower() or "connection" in op.lower():
            connection_data[db_info] = value
        elif "carregar" in op.lower() or "load" in op.lower():
            insert_data_metrics[db_info] = value

print("\n--- A guardar os resultados num ficheiro csv ---")
if connection_data:
    update_csv_timings("insert_times.csv", connection_data)
else:
    print("Sem dados de conexão para gravar.")

if insert_data_metrics:
    update_csv_timings("insert_times.csv", insert_data_metrics)
else:
    print("Sem dados de inserção para gravar.")
