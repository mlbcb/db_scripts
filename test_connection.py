#
# Testa conexão para todos os configs (e environments) de um projeto Doppler
#
# Uso: DOPPLER_TOKEN=dp.xxx DOPPLER_PROJECT=projeto python test_connection.py
#
# Variáveis de ambiente:
#   DOPPLER_TOKEN: Token de acesso Doppler (obrigatório)
#   DOPPLER_PROJECT: Nome do projeto (obrigatório)
#

import os, sys, subprocess

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

# Obter tokens: primeiro tenta variáveis de ambiente, depois Colab secrets
DOPPLER_TOKEN = os.environ.get('DOPPLER_TOKEN')
DOPPLER_PROJECT = os.environ.get('DOPPLER_PROJECT')

if not DOPPLER_TOKEN and IN_COLAB:
    DOPPLER_TOKEN = userdata.get('DOPPLER_TOKEN')
    DOPPLER_PROJECT = userdata.get('DOPPLER_PROJECT')

if not DOPPLER_TOKEN: raise ValueError("DOPPLER_TOKEN não definida")
if not DOPPLER_PROJECT: raise ValueError("DOPPLER_PROJECT não definida")

HEADERS = {"Authorization": f"Bearer {DOPPLER_TOKEN}"}

# Listar todas as configs do projeto
resp = requests.get("https://api.doppler.com/v3/configs", params={"project": DOPPLER_PROJECT}, headers=HEADERS)
resp.raise_for_status()
all_configs = resp.json().get("configs", [])

print(f"Projeto: {DOPPLER_PROJECT} | Configs: {len(all_configs)}\n")

suceeded = 0
failed = 0

for cfg in all_configs:
    name = cfg["name"]
    print(f"\nConfig: {name}")
    
    resp = requests.get("https://api.doppler.com/v3/configs/config/secrets", 
                        params={"project": DOPPLER_PROJECT, "config": name}, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Erro HTTP {resp.status_code} ao buscar secrets\n")
        failed += 1
        continue
    
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
        
        print(f"  SSLMODE: {ssl_mode}")
        
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
            # Usa parâmetros individuais (DB_HOST, DB_PORT, etc.) em vez da connection string
            # que era usada com a biblioteca usada anteriormente (psycopg2)
            host = secrets.get("DB_HOST")
            port = int(secrets.get("DB_PORT", 5432)) #Escolhe a porta 5432 caso DB_PORT não seja 
            # especificada, que é o caso do Koyeb e do Neon
            user = secrets.get("DB_USER")
            password = secrets.get("DB_PASSWORD")
            database = secrets.get("DB_NAME", "")
            
            conn = pg8000.native.Connection(
                host=host, port=port, user=user,
                password=password, database=database, ssl_context=ssl_context)
            conn.run("SELECT 1")
            conn.close()

        elif db_type in ['mysql', 'tidb', 'mariadb']:
            import pymysql
            conn = pymysql.connect(
                host=secrets.get("DB_HOST"),
                port=int(secrets.get("DB_PORT", 3306)),
                user=secrets.get("DB_USER"),
                password=secrets.get("DB_PASSWORD"),
                database=secrets.get("DB_NAME") or None,
                ssl=ssl_context)
            conn.cursor().execute("SELECT 1")
            conn.close()

        elif db_type == 'neo4j':
            from neo4j import GraphDatabase
            db_url = secrets.get("DB_URL")
            db_user = secrets.get("DB_USER")
            db_password = secrets.get("DB_PASSWORD")
            driver = GraphDatabase.driver(db_url, auth=(db_user, db_password))
            with driver.session() as s: s.run("RETURN 1")
            driver.close()

        elif db_type == 'harperdb':
            import harperdb
            db_url = secrets.get("DB_URL")
            db_user = secrets.get("DB_USER")
            db_password = secrets.get("DB_PASSWORD")
            harperdb.HarperDB(url=db_url, username=db_user, password=db_password).describe_all()

        elif db_type == 'astradb':
            from astrapy import DataAPIClient
            db_url = secrets.get("DB_URL")
            db_token = secrets.get("DB_TOKEN")
            db_keyspace = secrets.get("DB_KEYSPACE")
            db = DataAPIClient(db_token).get_database(db_url, keyspace=db_keyspace)
            db.list_collection_names()

        elif db_type == 'milvus':
            # Usa REST API em vez de pymilvus (evita problemas de compatibilidade com o Windows)
            # Zilliz Cloud usa POST para listar coleções
            db_url = secrets.get("DB_URL")
            db_token = secrets.get("DB_TOKEN")
            headers = {"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"}
            r = requests.post(f"{db_url}/v2/vectordb/collections/list", headers=headers, json={})
            r.raise_for_status()

        elif db_type == 'mongodb':
            from pymongo import MongoClient
            db_url = secrets.get("DB_URL")
            client = MongoClient(db_url)
            client.admin.command('ping')
            client.close()

        elif db_type == 'influxdb':
            # InfluxDB usa REST API com token de autenticação
            db_host = secrets.get("DB_HOST", "")
            db_org = secrets.get("DB_ORG")
            db_token = secrets.get("DB_TOKEN")
            headers = {"Authorization": f"Token {db_token}", "Content-Type": "application/json"}
            
            # Verifica saúde do servidor
            r = requests.get(f"{db_host}/health", headers=headers)
            r.raise_for_status()
            
            # Verifica se a organização é acessível
            r = requests.get(f"{db_host}/api/v2/orgs", headers=headers, params={"org": db_org})
            r.raise_for_status()

        elif db_type == 'qdrant':
            # Conexão por REST API
            db_url = secrets.get("DB_URL", "")
            api_key = secrets.get("API_KEY")
            
            headers = {"api-key": api_key}
            
            # Lista collections para testar conexão
            r = requests.get(f"{db_url}/collections", headers=headers)
            r.raise_for_status()

        else:
            raise ValueError(f"Tipo de banco de dados não suportado ou driver não implementado: {db_type}")

        print(f"Conexão com {db_type} bem sucedida.")
        suceeded += 1
        
    except Exception as e:
        print(f"Conexão com {db_type} falhou: {str(e)[:50]}")
        failed += 1

print(f"\nConectadas: {suceeded} | Erros: {failed}")
