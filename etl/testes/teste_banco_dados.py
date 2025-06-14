"""
Teste de conexão com o banco de dados PostgreSQL
Este script testa se a conexão com o banco está funcionando corretamente
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from urllib.parse import urlparse

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Carregar variáveis de ambiente
load_dotenv()


def parse_database_url(database_url):
    """
    Parse da URL do banco de dados para extrair componentes
    """
    parsed = urlparse(database_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port,
        'database': parsed.path.lstrip('/'),
        'username': parsed.username,
        'password': parsed.password
    }

def testar_conexao_banco():
    """
    Testa a conexão com o banco de dados PostgreSQL
    """
    info("=== INICIANDO TESTE DO BANCO DE DADOS ===")
    
    # Obter URL do banco do .env
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        error("❌ DATABASE_URL não encontrada no .env")
        return False
    
    # Parse da URL
    db_config = parse_database_url(database_url)
    info(f"✅ Configuração carregada - Host: {db_config['host']}:{db_config['port']}")
    info(f"   Database: {db_config['database']}")
    info(f"   Username: {db_config['username']}")
    
    connection = None
    cursor = None
    
    try:
        # Teste 1: Conexão básica
        info("🔌 Testando conexão básica...")
        connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
        
        info("✅ Conexão estabelecida com sucesso!")
        
        # Teste 2: Verificar versão do PostgreSQL
        info("📋 Verificando versão do PostgreSQL...")
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        info(f"✅ Versão do PostgreSQL: {version}")
        
        # Teste 3: Listar tabelas existentes
        info("📊 Verificando tabelas existentes...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        if tables:
            info(f"✅ {len(tables)} tabelas encontradas:")
            for table in tables:
                info(f"   - {table[0]}")
        else:
            info("ℹ️ Nenhuma tabela encontrada (banco vazio)")
        
        # Teste 4: Testar criação e drop de tabela temporária
        info("🧪 Testando criação de tabela temporária...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teste_conexao (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100),
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Inserir dados de teste
        cursor.execute("""
            INSERT INTO teste_conexao (nome) VALUES ('Teste ETL Twitch');
        """)
        
        # Verificar se os dados foram inseridos
        cursor.execute("SELECT * FROM teste_conexao;")
        dados_teste = cursor.fetchall()
        info(f"✅ Dados inseridos: {len(dados_teste)} registros")
        
        # Limpar tabela de teste
        cursor.execute("DROP TABLE IF EXISTS teste_conexao;")
        info("✅ Tabela de teste removida com sucesso")
        
        # Teste 5: Verificar permissões
        info("🔐 Verificando permissões do usuário...")
        cursor.execute("""
            SELECT 
                has_database_privilege(current_user, current_database(), 'CREATE') as create_perm,
                has_database_privilege(current_user, current_database(), 'CONNECT') as connect_perm;
        """)
        perms = cursor.fetchone()
        
        if perms[0] and perms[1]:
            info("✅ Usuário tem permissões adequadas (CREATE e CONNECT)")
        else:
            error("⚠️ Usuário pode ter permissões limitadas")
        
        # Commit das operações
        connection.commit()
        
        info("🎉 TODOS OS TESTES DO BANCO DE DADOS PASSARAM!")
        return True
        
    except psycopg2.OperationalError as e:
        error(f"❌ Erro de conexão: {e}")
        return False
    except psycopg2.Error as e:
        error(f"❌ Erro do PostgreSQL: {e}")
        return False
    except Exception as e:
        error(f"❌ Erro inesperado: {e}")
        return False
    
    finally:
        # Fechar cursor e conexão
        if cursor:
            cursor.close()
            info("🔒 Cursor fechado")
        if connection:
            connection.close()
            info("🔒 Conexão fechada")

if __name__ == "__main__":
    sucesso = testar_conexao_banco()
    if sucesso:
        info("✅ Teste concluído com sucesso!")
        sys.exit(0)
    else:
        error("❌ Teste falhou!")
        sys.exit(1) 