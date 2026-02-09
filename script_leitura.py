"""
Este script calcula o consumo de água de diferentes bases de dados.
Ele itera sobre as configurações do projeto Doppler, identifica o tipo de base de dados e executa a consulta apropriada para calcular o consumo.

Requisitos:
- pandas
- matplotlib
- clts_pcp
- pg8000
- pymysql
- neo4j
- influxdb-client
- harperdb

Uso:
    python calculate_consumption.py

O script gera um ficheiro CSV com os resultados e imprime os resultados no console.
"""

import os, sys, subprocess, requests, warnings, datetime, csv
import pandas as pd

# Suprimir avisos
warnings.filterwarnings("ignore")

# Instalar dependências se necessário
def install_package(package):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', package, '-q'], 
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

try:
    import clts_pcp as clts
except ImportError:
    install_package('clts_pcp')
    import clts_pcp as clts



# Configuração do Doppler
DOPPLER_TOKEN = os.environ.get('DOPPLER_TOKEN')
DOPPLER_PROJECT = os.environ.get('DOPPLER_PROJECT')
TABLE_NAME = os.environ.get('TABLE_NAME')

if not DOPPLER_TOKEN:
    try:
        from google.colab import userdata
        DOPPLER_TOKEN = userdata.get('DOPPLER_TOKEN')
        DOPPLER_PROJECT = userdata.get('DOPPLER_PROJECT')
        TABLE_NAME = userdata.get('TABLE_NAME')
    except ImportError:
        pass

if not DOPPLER_TOKEN: raise ValueError("DOPPLER_TOKEN não definida")
if not DOPPLER_PROJECT: raise ValueError("DOPPLER_PROJECT não definida")
if not TABLE_NAME: raise ValueError("TABLE_NAME não definida")

HEADERS = {"Authorization": f"Bearer {DOPPLER_TOKEN}"}

# Inicializar CLTS
clts.setcontext('Cronometragem de Cálculo de Consumo da Base de Dados')
tstart = clts.getts()

# Listar configs
print(f"A obter configurações do projeto {DOPPLER_PROJECT}...")
resp = requests.get("https://api.doppler.com/v3/configs", params={"project": DOPPLER_PROJECT}, headers=HEADERS)
resp.raise_for_status()
all_configs = resp.json().get("configs", [])

results = []

for cfg in all_configs:
    name = cfg["name"]
    # Obter Segredos
    resp = requests.get("https://api.doppler.com/v3/configs/config/secrets", 
                        params={"project": DOPPLER_PROJECT, "config": name}, headers=HEADERS)
    if resp.status_code != 200: continue
    
    secrets = {k: v.get("computed", "") for k, v in resp.json().get("secrets", {}).items()}
    db_type = secrets.get("DB_TYPE", "").lower()
    if not db_type: continue

    print(f"\n[{name}] Processando {db_type}...")

    try:
        # Configuração de Contexto SSL (Partilhado)
        import ssl
        ssl_context = ssl.create_default_context()
        if secrets.get("SSLMODE") == "require":
             ssl_context.check_hostname = False
             ssl_context.verify_mode = ssl.CERT_NONE
        elif secrets.get("CA_CERT"):
             ssl_context.load_verify_locations(cadata=secrets.get("CA_CERT"))

        t_conn = clts.getts()
        connected = False

        # --- SQL DATABASES ---
        if db_type in ['postgres', 'cratedb', 'mysql', 'mariadb', 'tidb', 'harperdb']:
            query = f"SELECT alias, MAX(leitura_l) - MIN(leitura_l) AS consumo_litros FROM {TABLE_NAME} GROUP BY alias"
            df = pd.DataFrame()
            
            if db_type in ['postgres', 'cratedb']:
                try: import pg8000.native
                except: install_package('pg8000')
                import pg8000.native
                
                conn = pg8000.native.Connection(
                    host=secrets.get("DB_HOST"), port=int(secrets.get("DB_PORT", 5432)),
                    user=secrets.get("DB_USER"), password=secrets.get("DB_PASSWORD"),
                    database=secrets.get("DB_NAME"), ssl_context=ssl_context
                )
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
                
                t_query = clts.getts()
                res = conn.run(query)
                df = pd.DataFrame(res, columns=['alias', 'consumo_litros'])
                clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
                conn.close()

            elif db_type in ['mysql', 'mariadb', 'tidb']:
                try: import pymysql
                except: install_package('pymysql')
                import pymysql
                
                conn = pymysql.connect(
                    host=secrets.get("DB_HOST"), port=int(secrets.get("DB_PORT", 3306)),
                    user=secrets.get("DB_USER"), password=secrets.get("DB_PASSWORD"),
                    database=secrets.get("DB_NAME"), ssl=ssl_context
                )
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
                
                t_query = clts.getts()
                df = pd.read_sql(query, conn)
                clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
                conn.close()

            elif db_type == 'harperdb':
                try: import harperdb
                except: install_package('harperdb')
                import harperdb
                
                # A conexão ao HarperDB é tecnicamente stateless, mas é possível cronometrar a inicialização
                hdb = harperdb.HarperDB(url=secrets.get("DB_URL"), 
                                      username=secrets.get("DB_USER"), 
                                      password=secrets.get("DB_PASSWORD"))
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
                
                t_query = clts.getts()
                # HarperDB requer esquema.tabela
                schema = secrets.get("DB_SCHEMA", "data")
                hdb_query = query.replace(TABLE_NAME, f"{schema}.{TABLE_NAME}")
                res = hdb.sql(hdb_query)
                df = pd.DataFrame(res)
                clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)

            if not df.empty:
                print(df.to_string(index=False))
            else:
                print("Sem resultados.")

        # --- INFLUXDB ---
        # Base de dados de séries temporais. Requer tratamento específico para Flux query language.
        elif db_type == 'influxdb':
            db_host = secrets.get("DB_HOST").rstrip("/")
            headers_db = {"Authorization": f"Token {secrets.get('DB_TOKEN')}", 
                          "Content-Type": "application/vnd.flux"}
            
            # Verificação de estado como 'ligação'
            try:
                requests.get(f"{db_host}/health", headers=headers_db, timeout=10)
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            except Exception as e:
                print(f"Erro na conexão InfluxDB: {e}")
                continue

            t_query = clts.getts()
            # Obter dados brutos e agregar no Pandas para fiabilidade
            # A query Flux filtra pelo bucket, measurement e campos relevantes (leitura_l e alias)
            # O pivot transforma os dados de formato "long" para "wide", facilitando o uso no Pandas
            query = f'''
            from(bucket: "{secrets.get('DB_BUCKET')}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement == "{TABLE_NAME}" and (r._field == "leitura_l" or r._field == "alias"))
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''
            r = requests.post(f"{db_host}/api/v2/query", headers=headers_db, 
                              params={"org": secrets.get("DB_ORG")}, data=query)
            
            if r.status_code != 200:
                print(f"Erro InfluxDB ({r.status_code}): {r.text}")
                r.raise_for_status()
            
            from io import StringIO
            # InfluxDB retorna CSV anotado com linhas começadas por #. Precisamos de ignorá-las.
            cleaned_csv = "\n".join([line for line in r.text.splitlines() if not line.strip().startswith("#")])
            csv_data = StringIO(cleaned_csv)
            df_flux = pd.read_csv(csv_data)
            
            clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
            
            if not df_flux.empty:
                # Normalizar colunas se necessário
                if 'alias' not in df_flux.columns:
                     print(f"Colunas InfluxDB: {df_flux.columns.tolist()}")
                
                if 'alias' in df_flux.columns and 'leitura_l' in df_flux.columns:
                    # Converter leitura para numérico
                    df_flux['leitura_l'] = pd.to_numeric(df_flux['leitura_l'], errors='coerce')
                    
                    # Agrupar por alias e calcular diferença
                    res = df_flux.groupby('alias')['leitura_l'].agg(lambda x: x.max() - x.min()).reset_index()
                    res.columns = ['alias', 'consumo_litros']
                    
                    print(res.to_string(index=False))
                else:
                    print("Colunas 'alias' ou 'leitura_l' não encontradas.")

        # --- MONGODB ---
        elif db_type == 'mongodb':
            try: import pymongo
            except: install_package('pymongo')
            from pymongo import MongoClient
            
            client = MongoClient(secrets.get("DB_URL"))
            db = client[secrets.get("DB_NAME", "default")]
            coll = db[TABLE_NAME]
            # Ping para verificar ligação
            client.admin.command('ping')
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            t_query = clts.getts()
            pipeline = [
                {"$addFields": {
                    "leitura_l_num": {"$toDouble": "$leitura_l"}
                }},
                {"$group": {
                    "_id": "$alias",
                    "max_l": {"$max": "$leitura_l_num"},
                    "min_l": {"$min": "$leitura_l_num"}
                }},
                {"$project": {
                    "alias": "$_id",
                    "consumo_litros": {"$subtract": ["$max_l", "$min_l"]},
                    "_id": 0
                }}
            ]
            res = list(coll.aggregate(pipeline))
            clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
            
            if res:
                print(pd.DataFrame(res).to_string(index=False))
            client.close()

        # --- NEO4J ---
        elif db_type == 'neo4j':
            try: from neo4j import GraphDatabase
            except: install_package('neo4j')
            from neo4j import GraphDatabase
            
            driver = GraphDatabase.driver(secrets.get("DB_URL"), 
                                        auth=(secrets.get("DB_USER"), secrets.get("DB_PASSWORD")))
            driver.verify_connectivity()
            clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
            
            t_query = clts.getts()
            with driver.session() as session:
                # Query Cypher para calcular o consumo (max - min) agrupado por alias
                q = f"MATCH (n:{TABLE_NAME}) RETURN n.alias as alias, max(toFloat(n.leitura_l)) - min(toFloat(n.leitura_l)) as consumo_litros"
                res = session.run(q).data()
                clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
                if res:
                    print(pd.DataFrame(res).to_string(index=False))
            driver.close()

        # --- VECTORES E OUTROS (RECURSO AO PANDAS) ---
        # Para bases de dados onde agregação no lado do servidor é complexa ou não suportada facilmente,
        # optou-se por extrair os dados e processar com Pandas.
        elif db_type in ['milvus', 'qdrant', 'astradb', 'couchbase']:
            rows = []
            
            if db_type == 'astradb':
                try: from astrapy import DataAPIClient
                except: install_package('astrapy')
                from astrapy import DataAPIClient
                
                db = DataAPIClient(secrets.get("DB_TOKEN")).get_database(secrets.get("DB_URL"), keyspace=secrets.get("DB_KEYSPACE"))
                # Verificação simples de disponibilidade (listar coleções ou similar, ou assumir conectado após init da API)
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)

                t_query = clts.getts()
                rows = list(db.get_collection(TABLE_NAME).find({}))
                
            elif db_type == 'qdrant':
                headers_db = {"api-key": secrets.get("API_KEY"), "Content-Type": "application/json"}
                base_url = secrets.get("DB_URL").rstrip("/")
                requests.get(f"{base_url}/collections", headers=headers_db) # Testar conectividade
                clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
                
                t_query = clts.getts()
                # Percorrer todos os pontos
                next_page = None
                while True:
                    p = {"limit": 100, "with_payload": True}
                    if next_page: p["offset"] = next_page
                    r = requests.post(f"{base_url}/collections/{TABLE_NAME}/points/scroll", headers=headers_db, json=p)
                    data = r.json().get("result", {})
                    rows.extend([x['payload'] for x in data.get("points", [])])
                    next_page = data.get("next_page_offset")
                    if not next_page: break
            
            elif db_type == 'couchbase':
                # Couchbase simplificado
                # Tenta importar as bibliotecas necessárias, instalando se faltarem
                from couchbase.cluster import Cluster, ClusterOptions
                from couchbase.auth import PasswordAuthenticator
                try:
                    from couchbase.options import ClusterOptions
                except ImportError:
                     install_package('couchbase')
                     from couchbase.cluster import Cluster, ClusterOptions
                
                # A conexão Couchbase pode precisar de tratamento mais específico, usando bloco try genérico
                # Assumindo string de conexão padrão
                pass

            elif db_type == 'milvus':
                 clts.elapt[f"{name} ({db_type}) - ligação"] = clts.deltat(t_conn)
                 t_query = clts.getts()
                 
                 db_url = secrets.get("DB_URL")
                 headers = {"Authorization": f"Bearer {secrets.get('DB_TOKEN')}",
                            "Content-Type": "application/json", "Accept": "application/json"}
                 
                 # Obter dados
                 # Consulta Milvus V2 REST API
                 # A usar um filtro abrangente para obter todos os documentos. Assumindo que 'id' existe ou apenas filtro vazio se suportado.
                 # Se filtragem básica for necessária e não soubermos a PK, tentamos uma verificação comum de campo.
                 payload = {
                     "collectionName": TABLE_NAME,
                     "outputFields": ["alias", "leitura_l"],
                     "limit": 1000, # Ajustar limite conforme necessário
                     "filter": "alias != ''" # Filtro simples para corresponder a alias não vazio
                 }
                 
                 r = requests.post(f"{db_url}/v2/vectordb/entities/query", headers=headers, json=payload)
                 if r.status_code == 200:
                     res_json = r.json()
                     if res_json.get('code') == 0:
                         rows = res_json.get('data', [])
                     else:
                         print(f"Erro Milvus API: {res_json}")
                 else:
                     print(f"Erro HTTP Milvus: {r.status_code} {r.text}")

            # Calcular métricas com Pandas
            # Converte a coluna de leitura para numérico e calcula max - min agrupado por alias
            if rows:
                df = pd.DataFrame(rows)
                df['leitura_l'] = pd.to_numeric(df['leitura_l'], errors='coerce')
                res = df.groupby('alias')['leitura_l'].agg(lambda x: x.max() - x.min()).reset_index()
                res.columns = ['alias', 'consumo_litros']
                
                clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
                print(res.to_string(index=False))
            else:
                 clts.elapt[f"{name} ({db_type}) - consulta"] = clts.deltat(t_query)
                 print("Sem dados.")


    except Exception as e:
        print(f"Erro em {name}: {e}")

clts.elapt["Tempo total de execução"] = clts.deltat(tstart)
print("\n--- Resultados de Tempo ---")
clts.listtimes()

# ==============================================================================
# REGISTO CSV PARA ANÁLISE
# ==============================================================================

def update_csv_timings(filename, new_timings_dict):
    """
    Atualiza um ficheiro CSV com uma nova coluna de tempos de execução.
    Se o ficheiro não existir, cria-o. Se existir, adiciona uma nova coluna 'Execution_N'.
    
    Args:
        filename (str): Caminho para o ficheiro CSV.
        new_timings_dict (dict): Dicionário {nome_bd: tempo}.
    
    Formato do CSV:
    Database, Execution_1, Execution_2, ...
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

# Separar dados de conexão e consulta
connection_data = {}
query_data = {}

for key, value in clts.elapt.items():
    if isinstance(value, dict) and 'tt' in value:
        value = value['tt']
    
    # Chave esperada: "ConfigName (DbType) - operação"
    if " - " in key:
        db_info, op = key.rsplit(" - ", 1)
        
        if "ligação" in op.lower() or "connection" in op.lower():
            connection_data[db_info] = value
        elif "query" in op.lower() or "consulta" in op.lower():
            query_data[db_info] = value

print("\n--- A guardar os resultados num ficheiro csv ---")
# if connection_data:
#     update_csv_timings("consumption_times.csv", connection_data)
# else:
#     print("Sem dados de conexão para gravar.")

if query_data:
    update_csv_timings("consumption_times.csv", query_data)
else:
    print("Sem dados de consulta para gravar.")
