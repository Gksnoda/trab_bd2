"""
Arquivo para testar a conexão com o banco de dados PostgreSQL.
"""

import sys
import psycopg2
from pathlib import Path

# Importar o logger
from logger import info, error

def test_database_connection():
    """
    Testa a conexão com o banco de dados
    """
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'twitch_analytics',
        'user': 'postgres',
        'password': 'admin'
    }
    
    try:
        info("Testando conexão com o banco de dados...")
        info("Host: {}", db_config['host'])
        info("Database: {}", db_config['database'])
        info("User: {}", db_config['user'])
        
        # Tentar conectar
        with psycopg2.connect(**db_config) as conn:
            info("✅ Conexão estabelecida com sucesso!")
            
            # Testar uma consulta simples
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                info("Versão do PostgreSQL: {}", version)
                
                # Listar tabelas existentes
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                
                tables = cursor.fetchall()
                if tables:
                    info("Tabelas encontradas:")
                    for table in tables:
                        info("  - {}", table[0])
                else:
                    info("Nenhuma tabela encontrada no banco")
        
        return True
        
    except psycopg2.OperationalError as e:
        error("❌ Erro de conexão: {}", str(e))
        return False
    except Exception as e:
        error("❌ Erro inesperado: {}", str(e))
        return False

def main():
    """
    Função principal
    """
    info("=== TESTE DE CONEXÃO COM BANCO DE DADOS ===")
    
    if test_database_connection():
        info("🎉 Teste de conexão bem-sucedido!")
        return True
    else:
        error("❌ Falha no teste de conexão!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 