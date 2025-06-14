"""
Schema do Banco de Dados - Criação das Tabelas PostgreSQL
Define estrutura das tabelas baseada no MER do projeto
"""

import sys
import os
from typing import List

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da configuração do banco
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from database_client import DatabaseClient

class DatabaseSchema:
    """Gerenciador de schema do banco de dados"""
    
    def __init__(self):
        """Inicializa o gerenciador de schema"""
        self.db_client = DatabaseClient()
    
    def get_create_tables_sql(self) -> List[str]:
        """
        Retorna lista de comandos SQL para criar tabelas
        
        Returns:
            Lista de comandos CREATE TABLE
        """
        return [
            # Tabela: users (baseada na entidade User do MER)
            """
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(200) PRIMARY KEY,
                login VARCHAR(100) NOT NULL,
                display_name VARCHAR(100),
                type VARCHAR(20),
                broadcaster_type VARCHAR(20),
                description TEXT,
                profile_image_url TEXT,
                created_at TIMESTAMP
            );
            """,
            
            # Tabela: games (baseada na entidade Game do MER)
            """
            CREATE TABLE IF NOT EXISTS games (
                id VARCHAR(200) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                box_art_url TEXT
            );
            """,
            
            # Tabela: streams (baseada na entidade Stream do MER)
            """
            CREATE TABLE IF NOT EXISTS streams (
                id VARCHAR(200) PRIMARY KEY,
                user_id VARCHAR(200) NOT NULL,
                title TEXT,
                viewer_count INTEGER DEFAULT 0,
                language VARCHAR(10),
                started_at TIMESTAMP,
                thumbnail_url TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            """,
            
            # Tabela: videos (baseada na entidade Video do MER)  
            """
            CREATE TABLE IF NOT EXISTS videos (
                id VARCHAR(200) PRIMARY KEY,
                stream_id VARCHAR(200),
                title TEXT,
                description TEXT,
                created_at TIMESTAMP,
                published_at TIMESTAMP,
                url TEXT,
                thumbnail_url TEXT,
                view_count BIGINT DEFAULT 0,
                language VARCHAR(10),
                duration VARCHAR(20),
                type VARCHAR(20),
                FOREIGN KEY (stream_id) REFERENCES streams(id)
            );
            """,
            
            # Tabela: clips (baseada na entidade Clip do MER)
            """
            CREATE TABLE IF NOT EXISTS clips (
                id VARCHAR(200) PRIMARY KEY,
                user_id VARCHAR(200) NOT NULL,
                video_id VARCHAR(200),
                game_id VARCHAR(200),
                title TEXT,
                view_count BIGINT DEFAULT 0,
                created_at TIMESTAMP,
                thumbnail_url TEXT,
                url TEXT,
                embed_url TEXT,
                duration REAL,
                language VARCHAR(10),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (video_id) REFERENCES videos(id),
                FOREIGN KEY (game_id) REFERENCES games(id)
            );
            """,
            
            # Tabela: game_stream (relacionamento N:M entre Game e Stream)
            """
            CREATE TABLE IF NOT EXISTS game_stream (
                game_id VARCHAR(200) NOT NULL,
                stream_id VARCHAR(200) NOT NULL,
                PRIMARY KEY (game_id, stream_id),
                FOREIGN KEY (game_id) REFERENCES games(id),
                FOREIGN KEY (stream_id) REFERENCES streams(id)
            );
            """
        ]
    
    # NOTA: Índices removidos - serão criados pela pessoa responsável pelos índices
    # após o ETL estar completo para melhor performance de inserção
    
    def create_database_schema(self) -> bool:
        """
        Cria todas as tabelas e índices no banco
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            info("🗄️ === CRIANDO SCHEMA DO BANCO DE DADOS ===")
            
            # Criar tabelas usando context manager
            info("📋 Criando tabelas...")
            table_sqls = self.get_create_tables_sql()
            
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                
                for i, sql in enumerate(table_sqls, 1):
                    try:
                        table_name = self._extract_table_name(sql)
                        info("📝 Criando tabela {}: {}", i, table_name)
                        
                        cursor.execute(sql)
                        info("✅ Tabela {} criada com sucesso", table_name)
                        
                    except Exception as e:
                        error("💥 Erro ao criar tabela {}: {}", table_name, e)
                        return False
                
                # Commit das mudanças
                conn.commit()
            
            # NOTA: Criação de índices removida - será feita pela pessoa responsável
            # após o ETL para melhor performance de inserção
            info("ℹ️ Índices serão criados posteriormente pela pessoa responsável")
            
            info("✅ === SCHEMA CRIADO COM SUCESSO ===")
            return True
            
        except Exception as e:
            error("💥 Erro geral ao criar schema: {}", e)
            return False
    
    def _extract_table_name(self, sql: str) -> str:
        """Extrai nome da tabela do comando SQL"""
        try:
            # Buscar padrão "CREATE TABLE IF NOT EXISTS nome"
            words = sql.upper().split()
            if 'EXISTS' in words:
                idx = words.index('EXISTS') + 1
                return words[idx].strip('(').lower()
            elif 'TABLE' in words:
                idx = words.index('TABLE') + 1
                return words[idx].strip('(').lower()
            return "desconhecida"
        except:
            return "desconhecida"
    
    # Método _extract_index_name removido - não é mais necessário
    
    def drop_all_tables(self) -> bool:
        """
        Remove todas as tabelas (cuidado!)
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            info("🗑️ === REMOVENDO TODAS AS TABELAS ===")
            
            # Ordem inversa para respeitar dependências
            tables = ['clips', 'videos', 'streams', 'games', 'users']
            
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                
                for table in tables:
                    try:
                        sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
                        info("🗑️ Removendo tabela: {}", table)
                        cursor.execute(sql)
                        info("✅ Tabela {} removida", table)
                        
                    except Exception as e:
                        error("💥 Erro ao remover tabela {}: {}", table, e)
                        return False
                
                # Commit das mudanças
                conn.commit()
            
            info("✅ === TODAS AS TABELAS REMOVIDAS ===")
            return True
            
        except Exception as e:
            error("💥 Erro geral ao remover tabelas: {}", e)
            return False

def main():
    """Função principal de teste"""
    info("🚀 === SCRIPT DE SCHEMA DO BANCO ===")
    
    schema = DatabaseSchema()
    
    # Criar schema
    success = schema.create_database_schema()
    
    if success:
        info("🎉 Schema criado com sucesso!")
    else:
        error("💥 Falha ao criar schema")

if __name__ == "__main__":
    main() 