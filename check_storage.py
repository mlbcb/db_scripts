#
# Verifica o uso de armazenamento das bases de dados configuradas no Doppler
#
# Uso: DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=projeto python check_storage.py
#

import os, sys, subprocess
import time

# Tenta importar userdata do Colab
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

# Obter tokens
DOPPLER_TOKEN = os.environ.get('DOPPLER_TOKEN')
DOPPLER_PROJECT = os.environ.get('DOPPLER_PROJECT')

if not DOPPLER_TOKEN and IN_COLAB:
    DOPPLER_TOKEN = userdata.get('DOPPLER_TOKEN')
    DOPPLER_PROJECT = userdata.get('DOPPLER_PROJECT')

if not DOPPLER_TOKEN: raise ValueError("DOPPLER_TOKEN não definida")
if not DOPPLER_PROJECT: raise ValueError("DOPPLER_PROJECT não definida")

HEADERS = {"Authorization": f"Bearer {DOPPLER_TOKEN}"}

# Listar configs
resp = requests.get("https://api.doppler.com/v3/configs", params={"project": DOPPLER_PROJECT}, headers=HEADERS)
resp.raise_for_status()
all_configs = resp.json().get("configs", [])

print(f"Projeto: {DOPPLER_PROJECT} | Configs: {len(all_configs)}\n")

stats = []

def format_size(size_bytes):
    if size_bytes is None: return "N/A"
    if isinstance(size_bytes, str): return size_bytes
    try:
        size_bytes = float(size_bytes)
    except:
        return str(size_bytes)
        
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

for cfg in all_configs:
    name = cfg["name"]
    print(f"A verificar: {name}...", end=" ", flush=True)
    
    resp = requests.get("https://api.doppler.com/v3/configs/config/secrets", 
                        params={"project": DOPPLER_PROJECT, "config": name}, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Erro ao buscar secrets: {resp.status_code}")
        continue
    
    secrets = {k: v.get("computed", "") for k, v in resp.json().get("secrets", {}).items()}
    db_type = secrets.get("DB_TYPE", "").lower()
    
    if not db_type:
        print("DB_TYPE não definido")
        continue

    # Instalar deps se necessário
    deps = {'postgres': 'pg8000', 'cratedb': 'pg8000', 'mysql': 'pymysql',
            'tidb': 'pymysql', 'mariadb': 'pymysql', 'neo4j': 'neo4j',
            'harperdb': 'harperdb', 'astradb': 'astrapy', 'mongodb': 'pymongo'}
    if db_type in deps:
        try:
            # Verificar se está instalado primeiro para evitar spam do pip
            __import__(deps[db_type].replace('pg8000', 'pg8000').replace('pymysql', 'pymysql').replace('neo4j', 'neo4j').replace('pymongo', 'pymongo'))
        except ImportError:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', deps[db_type], '-q'], 
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    size = None
    info = ""
    error = None

    try:
        # Configurar SSL
        import ssl
        ca_cert = secrets.get("CA_CERT")
        ssl_mode = secrets.get("SSLMODE", "verify-full").lower()
        ssl_context = ssl.create_default_context()
        if ssl_mode == "require":
             ssl_context.check_hostname = False
             ssl_context.verify_mode = ssl.CERT_NONE
        else:
             ssl_context.verify_mode = ssl.CERT_REQUIRED
             if ca_cert:
                 ssl_context.load_verify_locations(cadata=ca_cert)

        if db_type in ['postgres', 'cratedb']:
            import pg8000.native
            conn = pg8000.native.Connection(
                host=secrets.get("DB_HOST"), port=int(secrets.get("DB_PORT", 5432)),
                user=secrets.get("DB_USER"), password=secrets.get("DB_PASSWORD"),
                database=secrets.get("DB_NAME", ""), ssl_context=ssl_context, timeout=10)
            
            # CrateDB usa uma tabela de sistema diferente se não for compatível com pg_database_size,
            # mas geralmente pg_database_size funciona para Postgres. CrateDB pode precisar de `sys.shards`.
            if db_type == 'cratedb':
                # Verificação específica para CrateDB
                res = conn.run("SELECT sum(size) FROM sys.shards")
                size = res[0][0]
            else:
                res = conn.run("SELECT pg_database_size(current_database())")
                size = res[0][0]
                if size is None:
                    # Fallback para Yugabyte ou outros onde pg_database_size é nulo
                    # Somar tamanhos das tabelas no esquema public
                    res = conn.run("SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) FROM pg_tables WHERE schemaname = 'public'")
                    size = res[0][0]
            conn.close()

        elif db_type in ['mysql', 'tidb', 'mariadb']:
            import pymysql
            conn = pymysql.connect(
                host=secrets.get("DB_HOST"), port=int(secrets.get("DB_PORT", 3306)),
                user=secrets.get("DB_USER"), password=secrets.get("DB_PASSWORD"),
                database=secrets.get("DB_NAME"), ssl=ssl_context, connect_timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = DATABASE()")
            result = cursor.fetchone()
            size = result[0] if result else 0
            conn.close()

        elif db_type == 'mongodb':
            from pymongo import MongoClient
            client = MongoClient(secrets.get("DB_URL"), serverSelectionTimeoutMS=10000)
            db = client[secrets.get("DB_NAME", "default")]
            s = db.command("dbstats")
            size = s.get("dataSize") or s.get("storageSize")
            client.close()

        elif db_type == 'neo4j':
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(secrets.get("DB_URL"), 
                                          auth=(secrets.get("DB_USER"), secrets.get("DB_PASSWORD")),
                                          connection_timeout=10)
            with driver.session() as session:
                # Tamanho aproximado via contagens, pois sys info é restrito em alguns ambientes cloud
                r = session.run("MATCH (n) RETURN count(n) as nodes")
                nodes = r.single()["nodes"]
                r = session.run("MATCH ()-[r]->() RETURN count(r) as rels")
                rels = r.single()["rels"]
                info = f"{nodes} nodes, {rels} rels"
                # Não é possível obter o tamanho do disco de forma fiável via driver para Aura/Cloud sem acesso de sistema
            driver.close()

        elif db_type == 'harperdb':
            import harperdb
            hdb = harperdb.HarperDB(url=secrets.get("DB_URL"), 
                                    username=secrets.get("DB_USER"), 
                                    password=secrets.get("DB_PASSWORD"))
            # HarperDB não tem uma API direta de "tamanho total da bd" no cliente facilmente,
            # mas podemos tentar describe_schema ou info do sistema se disponível.
            # Recurso a contagem de registos para a tabela
            try:
                table_name = userdata.get('TABLE_NAME') if IN_COLAB else os.environ.get('TABLE_NAME', 'aqualog')
                res = hdb.sql(f"SELECT COUNT(*) FROM data.{table_name}")
                info = f"{res[0]['COUNT(*)']} records"
            except:
                info = "Size check not supported"

        elif db_type == 'qdrant':
            headers = {"api-key": secrets.get("API_KEY"), "Content-Type": "application/json"}
            url = secrets.get("DB_URL", "").rstrip("/")
            table_name = os.environ.get('TABLE_NAME', 'aqualog')
            r = requests.get(f"{url}/collections/{table_name}", headers=headers)
            if r.status_code == 200:
                data = r.json().get("result", {})
                pts = data.get("points_count", 0)
                segs = data.get("segments_count", 0)
                # vectors_count pode ser aproximado
                info = f"{pts} points"
            else:
                info = "Collection not found"
        
        elif db_type == 'milvus':
             url = secrets.get("DB_URL")
             token = secrets.get("DB_TOKEN")
             table_name = os.environ.get('TABLE_NAME', 'aqualog')
             headers = {"Authorization": f"Bearer {token}"}
             r = requests.post(f"{url}/v2/vectordb/collections/describe", headers=headers, json={"collectionName": table_name})
             if r.status_code == 200:
                 # Milvus describe não fornece tamanho diretamente na API v2 sempre, tentar stats
                 r2 = requests.post(f"{url}/v2/vectordb/collections/get_stats", headers=headers, json={"collectionName": table_name})
                 if r2.status_code == 200:
                     rows = r2.json().get("data", {}).get("rowCount", 0)
                     info = f"{rows} rows"
                 else:
                     info = "Stats unavailable"
             else:
                 info = "Check failed"

        elif db_type == 'astradb':
             from astrapy import DataAPIClient
             client = DataAPIClient(secrets.get("DB_TOKEN"))
             keyspace = secrets.get("DB_KEYSPACE")
             db = client.get_database(secrets.get("DB_URL"), keyspace=keyspace)
             coll_name = os.environ.get('TABLE_NAME', 'aqualog')
             try:
                 coll = db.get_collection(coll_name)
                 # count_documents pode ser dispendioso ou limitado
                 count = coll.count_documents({}, upper_bound=10000) # verificação de limite
                 info = f"{count if count < 10000 else '10000+'} docs"
             except:
                 info = "Erro ao obter estatísticas"

    except Exception as e:
        error = str(e)
        # print(f"\nErro em {name}: {e}")

    result_str = format_size(size) if size is not None else (info if info else "Unknown")
    if error: result_str = f"Error: {error}"
    
    print(f"[{db_type}] -> {result_str}")
    
    stats.append({
        "Config": name,
        "Type": db_type,
        "Storage": result_str,
        "RawBytes": size if size is not None else 0
    })

print("\n--- Resumo de Armazenamento ---")
print(f"{'Config':<25} | {'Type':<10} | {'Storage/Info'}")
print("-" * 60)
for s in stats:
    print(f"{s['Config']:<25} | {s['Type']:<10} | {s['Storage']}")


