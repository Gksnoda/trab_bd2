"""
Script para excluir completamente o banco de dados twitch_analytics.
"""

import sys
import psycopg2
from psycopg2 import sql

# Importar o logger
from logger import info, error

def drop_database():
    """
    Exclui completamente o banco de dados twitch_analytics
    """
    postgres_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'admin'
    }
    
    try:
        info("Conectando ao PostgreSQL...")
        
        conn = psycopg2.connect(**postgres_config)
        conn.autocommit = True
        
        try:
            cursor = conn.cursor()
            
            # Verifica se o banco existe
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", ('twitch_analytics',))
            
            if cursor.fetchone():
                info("Banco de dados 'twitch_analytics' encontrado. Excluindo...")
                
                # Terminar conexões ativas ao banco (se houver)
                cursor.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = 'twitch_analytics' AND pid <> pg_backend_pid()
                """)
                
                # Excluir o banco de dados
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier('twitch_analytics')))
                info("✅ Banco de dados 'twitch_analytics' excluído com sucesso!")
            else:
                info("⚠️  Banco de dados 'twitch_analytics' não existe")
            
            cursor.close()
            
        finally:
            conn.close()
        
        return True
        
    except Exception as e:
        error("❌ Erro ao excluir banco de dados: {}", str(e))
        return False

def confirm_deletion():
    """
    Pede confirmação do usuário antes de excluir o banco
    """
    info("⚠️  ATENÇÃO: Esta operação irá excluir COMPLETAMENTE o banco 'twitch_analytics'")
    info("⚠️  Todos os dados serão PERDIDOS permanentemente!")
    
    try:
        response = input("\nDeseja continuar? Digite 'SIM' para confirmar: ").strip().upper()
        return response == 'SIM'
    except KeyboardInterrupt:
        info("\nOperação cancelada pelo usuário")
        return False

def main():
    """
    Função principal
    """
    info("=== EXCLUSÃO DO BANCO DE DADOS ===")
    
    # Pedir confirmação
    if not confirm_deletion():
        info("❌ Operação cancelada")
        return False
    
    # Excluir banco
    if drop_database():
        info("🎉 Banco de dados excluído com sucesso!")
        return True
    else:
        error("❌ Falha ao excluir banco de dados!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 