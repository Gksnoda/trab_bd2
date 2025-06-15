"""
Módulo para criação do banco de dados e tabelas do sistema de análise do Twitch.
"""

import sys
import psycopg2
from psycopg2 import sql
from pathlib import Path

# Adicionar o diretório pai ao PATH para importar o logger
sys.path.append(str(Path(__file__).parent.parent))
from logger import info, error

def create_database():
    """
    Cria o banco de dados twitch_analytics se não existir
    """
    postgres_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'admin'
    }
    
    try:
        info("Criando banco de dados...")
        
        conn = psycopg2.connect(**postgres_config)
        conn.autocommit = True
        
        try:
            cursor = conn.cursor()
            
            # Verifica se o banco já existe
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", ('twitch_analytics',))
            
            if cursor.fetchone():
                info("Banco de dados 'twitch_analytics' já existe")
            else:
                # Cria o banco de dados
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('twitch_analytics')))
                info("Banco de dados 'twitch_analytics' criado!")
            
            cursor.close()
            
        finally:
            conn.close()
        
        return True
        
    except Exception as e:
        error("Erro ao criar banco de dados: {}", str(e))
        return False

def create_tables():
    """
    Cria todas as tabelas necessárias
    """
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'twitch_analytics',
        'user': 'postgres',
        'password': 'admin'
    }
    
    try:
        info("Criando tabelas...")
        
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                
                # Tabela USERS
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(25) PRIMARY KEY,
                        display_name VARCHAR(50),
                        broadcaster_type VARCHAR(15),
                        description VARCHAR(1000),
                        profile_image_url VARCHAR(130),
                        created_at DATE
                    )
                """)
                info("Tabela 'users' criada")
                
                # Tabela GAMES
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS games (
                        id VARCHAR(25) PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        box_art_url VARCHAR(150)
                    )
                """)
                info("Tabela 'games' criada")
                
                # Tabela STREAMS
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS streams (
                        id VARCHAR(25) PRIMARY KEY,
                        user_id VARCHAR(25) NOT NULL,
                        game_id VARCHAR(25),
                        title VARCHAR(256),
                        viewer_count INTEGER,
                        started_at TIMESTAMP,
                        language VARCHAR(8),
                        thumbnail_url VARCHAR(150),
                        tags VARCHAR(30)[],
                        FOREIGN KEY (user_id) REFERENCES users(id),
                        FOREIGN KEY (game_id) REFERENCES games(id)
                    )
                """)
                info("Tabela 'streams' criada")
                
                # Tabela VIDEOS
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS videos (
                        id VARCHAR(25) PRIMARY KEY,
                        stream_id VARCHAR(25),
                        user_id VARCHAR(25) NOT NULL,
                        title VARCHAR(256),
                        created_at DATE,
                        url VARCHAR(130),
                        view_count INTEGER,
                        language VARCHAR(6),
                        duration VARCHAR(15),
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
                info("Tabela 'videos' criada")
                
                # Tabela CLIPS
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clips (
                        id VARCHAR(60) PRIMARY KEY,
                        url VARCHAR(100) NOT NULL,
                        user_id VARCHAR(30) NOT NULL,
                        video_id VARCHAR(60),
                        game_id VARCHAR(60),
                        language VARCHAR(6),
                        title VARCHAR(256),
                        view_count INTEGER,
                        created_at DATE,
                        duration VARCHAR(15),
                        FOREIGN KEY (user_id) REFERENCES users(id),
                        FOREIGN KEY (video_id) REFERENCES videos(id),
                        FOREIGN KEY (game_id) REFERENCES games(id)
                    )
                """)
                info("Tabela 'clips' criada")
                
                # Tabela GAME_STREAM
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS game_stream (
                        game_id VARCHAR(25) NOT NULL,
                        stream_id VARCHAR(25) NOT NULL,
                        PRIMARY KEY (game_id, stream_id),
                        FOREIGN KEY (game_id) REFERENCES games(id),
                        FOREIGN KEY (stream_id) REFERENCES streams(id)
                    )
                """)
                info("Tabela 'game_stream' criada")
                
                conn.commit()
                info("Todas as tabelas foram criadas!")
                
        return True
        
    except Exception as e:
        error("Erro ao criar tabelas: {}", str(e))
        return False

def main():
    """
    Função principal
    """
    info("=== CRIANDO BANCO E TABELAS ===")
    
    # 1. Criar banco
    if not create_database():
        return False
    
    # 2. Criar tabelas
    if not create_tables():
        return False
    
    info("✅ Banco e tabelas criados com sucesso!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 