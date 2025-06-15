"""
Módulo para carregamento dos dados transformados no banco de dados PostgreSQL.
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Adicionar o diretório pai ao PATH para importar o logger
sys.path.append(str(Path(__file__).parent.parent))
from logger import info, error

class DataLoader:
    """
    Classe responsável pelo carregamento dos dados no banco
    """
    
    def __init__(self):
        """
        Inicializa o loader com as configurações do banco
        """
        self.db_config = {
            'host': 'localhost',
            'port': '5432',
            'database': 'twitch_analytics',
            'user': 'postgres',
            'password': 'admin'
        }
        
        # Diretório dos dados transformados
        self.data_dir = Path(__file__).parent.parent / "data" / "transformed"
    
    def connect_database(self):
        """
        Estabelece conexão com o banco de dados
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            info("✅ Conexão com banco de dados estabelecida")
            return conn
        except Exception as e:
            error("❌ Erro ao conectar com banco de dados: {}", str(e))
            return None
    
    def load_json_data(self, filename: str) -> Dict[str, Any]:
        """
        Carrega dados de um arquivo JSON
        """
        file_path = self.data_dir / filename
        
        try:
            if not file_path.exists():
                error("❌ Arquivo não encontrado: {}", file_path)
                return None
                
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            info("📂 Arquivo carregado: {} ({} registros)", filename, len(data.get('data', [])))
            return data
            
        except Exception as e:
            error("❌ Erro ao carregar arquivo {}: {}", filename, str(e))
            return None
    
    def parse_datetime(self, date_str: str) -> datetime:
        """
        Converte string de data para datetime
        """
        if not date_str:
            return None
            
        try:
            # Tentar diferentes formatos de data
            formats = [
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            return None
            
        except Exception:
            return None
    
    def load_users(self, conn, data: Dict[str, Any]) -> bool:
        """
        Carrega dados dos usuários
        """
        try:
            users = data.get('data', [])
            if not users:
                info("⚠️  Nenhum usuário para carregar")
                return True
                
            with conn.cursor() as cursor:
                # Preparar dados para inserção
                values = []
                for user in users:
                    values.append((
                        user.get('id'),
                        user.get('display_name'),
                        user.get('broadcaster_type'),
                        user.get('description'),
                        user.get('profile_image_url'),
                        self.parse_datetime(user.get('created_at'))
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO users (id, display_name, broadcaster_type, description, profile_image_url, created_at)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        broadcaster_type = EXCLUDED.broadcaster_type,
                        description = EXCLUDED.description,
                        profile_image_url = EXCLUDED.profile_image_url,
                        created_at = EXCLUDED.created_at
                    """,
                    values
                )
                
                conn.commit()
                info("✅ {} usuários carregados", len(values))
                
            return True
            
        except Exception as e:
            error("❌ Erro ao carregar usuários: {}", str(e))
            conn.rollback()
            return False
    
    def load_games(self, conn, data: Dict[str, Any]) -> bool:
        """
        Carrega dados dos jogos
        """
        try:
            games = data.get('data', [])
            if not games:
                info("⚠️  Nenhum jogo para carregar")
                return True
                
            with conn.cursor() as cursor:
                # Preparar dados para inserção
                values = []
                for game in games:
                    values.append((
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
                    values
                )
                
                conn.commit()
                info("✅ {} jogos carregados", len(values))
                
            return True
            
        except Exception as e:
            error("❌ Erro ao carregar jogos: {}", str(e))
            conn.rollback()
            return False

    def load_streams(self, conn, data: Dict[str, Any]) -> bool:
        """
        Carrega dados das streams
        """
        try:
            streams = data.get('data', [])
            if not streams:
                info("⚠️  Nenhuma stream para carregar")
                return True
                
            with conn.cursor() as cursor:
                # Preparar dados para inserção
                values = []
                game_stream_values = []
                
                for stream in streams:
                    # Dados da stream
                    values.append((
                        stream.get('id'),
                        stream.get('user_id'),
                        stream.get('game_id'),
                        stream.get('title'),
                        stream.get('viewer_count'),
                        self.parse_datetime(stream.get('started_at')),
                        stream.get('language'),
                        stream.get('thumbnail_url'),
                        stream.get('tags', [])
                    ))
                    
                    # Relacionamento game_stream (se tiver game_id)
                    if stream.get('game_id') and stream.get('id'):
                        game_stream_values.append((
                            stream.get('game_id'),
                            stream.get('id')
                        ))
                
                # Inserir streams
                execute_values(
                    cursor,
                    """
                    INSERT INTO streams (id, user_id, game_id, title, viewer_count, started_at, language, thumbnail_url, tags)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        game_id = EXCLUDED.game_id,
                        title = EXCLUDED.title,
                        viewer_count = EXCLUDED.viewer_count,
                        started_at = EXCLUDED.started_at,
                        language = EXCLUDED.language,
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        tags = EXCLUDED.tags
                    """,
                    values
                )
                
                # Inserir relacionamentos game_stream
                if game_stream_values:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO game_stream (game_id, stream_id)
                        VALUES %s
                        ON CONFLICT (game_id, stream_id) DO NOTHING
                        """,
                        game_stream_values
                    )
                    info("✅ {} relacionamentos game_stream criados", len(game_stream_values))
                
                conn.commit()
                info("✅ {} streams carregadas", len(values))
                
            return True
            
        except Exception as e:
            error("❌ Erro ao carregar streams: {}", str(e))
            conn.rollback()
            return False
    
    def load_videos(self, conn, data: Dict[str, Any]) -> bool:
        """
        Carrega dados dos vídeos
        """
        try:
            videos = data.get('data', [])
            if not videos:
                info("⚠️  Nenhum vídeo para carregar")
                return True
                
            with conn.cursor() as cursor:
                # Preparar dados para inserção
                values = []
                for video in videos:
                    values.append((
                        video.get('id'),
                        video.get('stream_id'),
                        video.get('user_id'),
                        video.get('title'),
                        self.parse_datetime(video.get('created_at')),
                        video.get('url'),
                        video.get('view_count'),
                        video.get('language'),
                        video.get('duration')
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO videos (id, stream_id, user_id, title, created_at, url, view_count, language, duration)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        stream_id = EXCLUDED.stream_id,
                        user_id = EXCLUDED.user_id,
                        title = EXCLUDED.title,
                        created_at = EXCLUDED.created_at,
                        url = EXCLUDED.url,
                        view_count = EXCLUDED.view_count,
                        language = EXCLUDED.language,
                        duration = EXCLUDED.duration
                    """,
                    values
                )
                
                conn.commit()
                info("✅ {} vídeos carregados", len(values))
                
            return True
            
        except Exception as e:
            error("❌ Erro ao carregar vídeos: {}", str(e))
            conn.rollback()
            return False
    
    def load_clips(self, conn, data: Dict[str, Any]) -> bool:
        """
        Carrega dados dos clips
        """
        try:
            clips = data.get('data', [])
            if not clips:
                info("⚠️  Nenhum clip para carregar")
                return True
                
            with conn.cursor() as cursor:
                # Preparar dados para inserção
                values = []
                for clip in clips:
                    values.append((
                        clip.get('id'),
                        clip.get('url'),
                        clip.get('user_id'),
                        clip.get('video_id'),
                        clip.get('game_id'),
                        clip.get('language'),
                        clip.get('title'),
                        clip.get('view_count'),
                        self.parse_datetime(clip.get('created_at')),
                        clip.get('duration')
                    ))
                
                # Inserção em lote
                execute_values(
                    cursor,
                    """
                    INSERT INTO clips (id, url, user_id, video_id, game_id, language, title, view_count, created_at, duration)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        url = EXCLUDED.url,
                        user_id = EXCLUDED.user_id,
                        video_id = EXCLUDED.video_id,
                        game_id = EXCLUDED.game_id,
                        language = EXCLUDED.language,
                        title = EXCLUDED.title,
                        view_count = EXCLUDED.view_count,
                        created_at = EXCLUDED.created_at,
                        duration = EXCLUDED.duration
                    """,
                    values
                )
                
                conn.commit()
                info("✅ {} clips carregados", len(values))
                
            return True
            
        except Exception as e:
            error("❌ Erro ao carregar clips: {}", str(e))
            conn.rollback()
            return False

def main():
    """
    Função principal para executar o carregamento
    """
    info("=== INICIANDO CARREGAMENTO DOS DADOS ===")
    
    loader = DataLoader()
    
    # Conectar ao banco
    conn = loader.connect_database()
    if not conn:
        error("❌ Não foi possível conectar ao banco de dados")
        return False
    
    try:
        # Ordem de carregamento (respeitando dependências das FK)
        load_order = [
            ('users_transformed.json', 'users', loader.load_users),
            ('games_transformed.json', 'games', loader.load_games),
            ('streams_transformed.json', 'streams', loader.load_streams),
            ('videos_transformed.json', 'videos', loader.load_videos),
            ('clips_transformed.json', 'clips', loader.load_clips)
        ]
        
        success_count = 0
        
        for filename, table_name, load_function in load_order:
            info("")
            info("📥 Carregando dados para tabela '{}'...", table_name)
            
            # Carregar dados do arquivo JSON
            data = loader.load_json_data(filename)
            if data is None:
                error("❌ Falha ao carregar dados do arquivo {}", filename)
                continue
            
            # Executar função de carregamento específica
            if load_function(conn, data):
                success_count += 1
                info("✅ Tabela '{}' carregada com sucesso", table_name)
            else:
                error("❌ Falha ao carregar tabela '{}'", table_name)
        
        if success_count > 0:
            info("")
            info("🎉 CARREGAMENTO CONCLUÍDO! {} tabelas carregadas", success_count)
            return True
        else:
            error("❌ Nenhuma tabela foi carregada")
            return False
            
    finally:
        conn.close()
        info("🔌 Conexão com banco de dados fechada")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 