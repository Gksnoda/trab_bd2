"""
Cliente para banco PostgreSQL
Classe responsável por todas as operações do banco baseadas no MER.png
"""

import sys
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

from settings import DatabaseConfig, TableSchemas, ETLConfig

class DatabaseClient:
    """Cliente para operações no banco PostgreSQL"""
    
    def __init__(self):
        """Inicializa o cliente do banco"""
        self.config = DatabaseConfig()
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """
        Context manager para conexão com o banco
        Garante que a conexão seja fechada automaticamente
        """
        connection = None
        try:
            connection = psycopg2.connect(**self.config.get_connection_params())
            connection.autocommit = False  # Transações manuais
            yield connection
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            error("💥 Erro de banco de dados: {}", e)
            raise
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """
        Testa a conexão com o banco
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        info("🔌 Testando conexão com o banco...")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                info("✅ Conexão bem-sucedida - PostgreSQL: {}", version[:50] + "...")
                return True
        except Exception as e:
            error("❌ Falha na conexão: {}", e)
            return False
    
    def create_tables(self) -> bool:
        """
        Cria todas as tabelas baseadas no MER.png
        Respeita a ordem de dependências
        
        Returns:
            True se todas as tabelas foram criadas, False caso contrário
        """
        info("🏗️ Criando tabelas do banco baseadas no MER...")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Criar tabelas na ordem correta (respeitando foreign keys)
                for table_name, table_sql in TableSchemas.CREATION_ORDER:
                    info("📋 Criando tabela: {}", table_name)
                    cursor.execute(table_sql)
                    info("✅ Tabela {} criada com sucesso", table_name)
                
                # Confirmar todas as operações
                conn.commit()
                info("🎉 Todas as tabelas foram criadas com sucesso!")
                return True
                
        except psycopg2.Error as e:
            error("❌ Erro ao criar tabelas: {}", e)
            return False
    
    def drop_tables(self) -> bool:
        """
        Remove todas as tabelas (para reset completo)
        
        Returns:
            True se removeu com sucesso, False caso contrário
        """
        info("🗑️ Removendo todas as tabelas...")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Remover na ordem reversa para respeitar foreign keys
                table_names = [name for name, _ in reversed(TableSchemas.CREATION_ORDER)]
                
                for table_name in table_names:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                    info("🗑️ Tabela {} removida", table_name)
                
                conn.commit()
                info("✅ Todas as tabelas foram removidas")
                return True
                
        except psycopg2.Error as e:
            error("❌ Erro ao remover tabelas: {}", e)
            return False
    
    def get_table_info(self) -> Dict[str, int]:
        """
        Obtém informações sobre as tabelas existentes
        
        Returns:
            Dicionário com nome da tabela e número de registros
        """
        info("📊 Verificando informações das tabelas...")
        
        table_info = {}
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Listar todas as tabelas do esquema public
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                
                tables = cursor.fetchall()
                
                for (table_name,) in tables:
                    # Contar registros em cada tabela
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cursor.fetchone()[0]
                    table_info[table_name] = count
                    info("📋 Tabela {}: {} registros", table_name, count)
                
                return table_info
                
        except psycopg2.Error as e:
            error("❌ Erro ao verificar tabelas: {}", e)
            return {}
    
    def insert_users(self, users: List[Dict]) -> int:
        """
        Insere usuários na tabela users
        
        Args:
            users: Lista de dados de usuários da API Twitch
            
        Returns:
            Número de registros inseridos
        """
        if not users:
            return 0
        
        info("👤 Inserindo {} usuários...", len(users))
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar dados para inserção
                insert_data = []
                for user in users:
                    insert_data.append((
                        user.get('id'),
                        user.get('login'),
                        user.get('display_name'),
                        user.get('broadcaster_type'),
                        user.get('type'),
                        user.get('description'),
                        user.get('profile_image_url'),
                        user.get('view_count', 0),
                        user.get('created_at')
                    ))
                
                # Inserção em lote usando ON CONFLICT para evitar duplicatas
                execute_values(
                    cursor,
                    """
                    INSERT INTO users (id, login, display_name, broadcaster_type, type, 
                                     description, profile_image_url, view_count, created_at)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        login = EXCLUDED.login,
                        display_name = EXCLUDED.display_name,
                        broadcaster_type = EXCLUDED.broadcaster_type,
                        type = EXCLUDED.type,
                        description = EXCLUDED.description,
                        profile_image_url = EXCLUDED.profile_image_url,
                        view_count = EXCLUDED.view_count,
                        created_at = EXCLUDED.created_at
                    """,
                    insert_data
                )
                
                inserted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} usuários inseridos/atualizados", inserted_count)
                return inserted_count
                
        except psycopg2.Error as e:
            error("❌ Erro ao inserir usuários: {}", e)
            return 0
    
    def insert_games(self, games: List[Dict]) -> int:
        """
        Insere jogos na tabela games
        
        Args:
            games: Lista de dados de jogos da API Twitch
            
        Returns:
            Número de registros inseridos
        """
        if not games:
            return 0
        
        info("🎮 Inserindo {} jogos...", len(games))
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar dados para inserção
                insert_data = []
                for game in games:
                    insert_data.append((
                        game.get('id'),
                        game.get('name'),
                        game.get('box_art_url')
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO games (id, name, box_art_url)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        box_art_url = EXCLUDED.box_art_url
                    """,
                    insert_data
                )
                
                inserted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} jogos inseridos/atualizados", inserted_count)
                return inserted_count
                
        except psycopg2.Error as e:
            error("❌ Erro ao inserir jogos: {}", e)
            return 0
    
    def insert_streams(self, streams: List[Dict]) -> int:
        """
        Insere streams na tabela streams
        
        Args:
            streams: Lista de dados de streams da API Twitch
            
        Returns:
            Número de registros inseridos
        """
        if not streams:
            return 0
        
        info("📺 Inserindo {} streams...", len(streams))
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar dados para inserção
                insert_data = []
                for stream in streams:
                    insert_data.append((
                        stream.get('id'),
                        stream.get('user_id'),
                        stream.get('game_id'),
                        stream.get('title'),
                        stream.get('viewer_count', 0),
                        stream.get('started_at'),
                        stream.get('language'),
                        stream.get('thumbnail_url')
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO streams (id, user_id, game_id, title, viewer_count, 
                                       started_at, language, thumbnail_url)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        game_id = EXCLUDED.game_id,
                        title = EXCLUDED.title,
                        viewer_count = EXCLUDED.viewer_count,
                        started_at = EXCLUDED.started_at,
                        language = EXCLUDED.language,
                        thumbnail_url = EXCLUDED.thumbnail_url
                    """,
                    insert_data
                )
                
                inserted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} streams inseridas/atualizadas", inserted_count)
                return inserted_count
                
        except psycopg2.Error as e:
            error("❌ Erro ao inserir streams: {}", e)
            return 0
    
    def insert_videos(self, videos: List[Dict]) -> int:
        """
        Insere vídeos na tabela videos
        
        Args:
            videos: Lista de dados de vídeos da API Twitch
            
        Returns:
            Número de registros inseridos
        """
        if not videos:
            return 0
        
        info("📹 Inserindo {} vídeos...", len(videos))
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar dados para inserção
                insert_data = []
                for video in videos:
                    insert_data.append((
                        video.get('id'),
                        video.get('user_id'),
                        video.get('title'),
                        video.get('description'),
                        video.get('created_at'),
                        video.get('published_at'),
                        video.get('url'),
                        video.get('thumbnail_url'),
                        video.get('type'),
                        video.get('duration'),
                        video.get('language'),
                        video.get('view_count', 0)
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO videos (id, user_id, title, description, created_at, 
                                      published_at, url, thumbnail_url, type, duration, 
                                      language, view_count)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        created_at = EXCLUDED.created_at,
                        published_at = EXCLUDED.published_at,
                        url = EXCLUDED.url,
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        type = EXCLUDED.type,
                        duration = EXCLUDED.duration,
                        language = EXCLUDED.language,
                        view_count = EXCLUDED.view_count
                    """,
                    insert_data
                )
                
                inserted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} vídeos inseridos/atualizados", inserted_count)
                return inserted_count
                
        except psycopg2.Error as e:
            error("❌ Erro ao inserir vídeos: {}", e)
            return 0
    
    def insert_clips(self, clips: List[Dict]) -> int:
        """
        Insere clips na tabela clips
        
        Args:
            clips: Lista de dados de clips da API Twitch
            
        Returns:
            Número de registros inseridos
        """
        if not clips:
            return 0
        
        info("🎥 Inserindo {} clips...", len(clips))
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Preparar dados para inserção
                insert_data = []
                for clip in clips:
                    insert_data.append((
                        clip.get('id'),
                        clip.get('broadcaster_id'),
                        clip.get('creator_id'),
                        clip.get('video_id'),  # Nova coluna conforme novo MER
                        clip.get('game_id'),
                        clip.get('title'),
                        clip.get('view_count', 0),
                        clip.get('created_at'),
                        clip.get('thumbnail_url'),
                        clip.get('url'),
                        clip.get('embed_url'),
                        clip.get('duration'),
                        clip.get('language')
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO clips (id, broadcaster_id, creator_id, video_id, game_id, 
                                     title, view_count, created_at, thumbnail_url, url, 
                                     embed_url, duration, language)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        broadcaster_id = EXCLUDED.broadcaster_id,
                        creator_id = EXCLUDED.creator_id,
                        video_id = EXCLUDED.video_id,
                        game_id = EXCLUDED.game_id,
                        title = EXCLUDED.title,
                        view_count = EXCLUDED.view_count,
                        created_at = EXCLUDED.created_at,
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        url = EXCLUDED.url,
                        embed_url = EXCLUDED.embed_url,
                        duration = EXCLUDED.duration,
                        language = EXCLUDED.language
                    """,
                    insert_data
                )
                
                inserted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} clips inseridos/atualizados", inserted_count)
                return inserted_count
                
        except psycopg2.Error as e:
            error("❌ Erro ao inserir clips: {}", e)
            return 0
    
    def clear_table(self, table_name: str) -> bool:
        """
        Limpa todos os dados de uma tabela
        
        Args:
            table_name: Nome da tabela para limpar
            
        Returns:
            True se limpou com sucesso
        """
        info("🧹 Limpando tabela: {}", table_name)
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {table_name};")
                deleted_count = cursor.rowcount
                conn.commit()
                
                info("✅ {} registros removidos da tabela {}", deleted_count, table_name)
                return True
                
        except psycopg2.Error as e:
            error("❌ Erro ao limpar tabela {}: {}", table_name, e)
            return False

if __name__ == "__main__":
    # Teste básico do cliente
    db = DatabaseClient()
    
    if db.test_connection():
        info("🎯 Testando criação de tabelas...")
        if db.create_tables():
            info("🎯 Verificando informações das tabelas...")
            table_info = db.get_table_info()
            for table, count in table_info.items():
                info("📊 {}: {} registros", table, count)
    else:
        error("❌ Não é possível conectar ao banco - verifique as configurações") 