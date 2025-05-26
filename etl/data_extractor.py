import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from api.twitch_client import TwitchAPIClient
from database.config import SessionLocal
from database.models import User, Game, Stream, Video, Clip
from logger import info, error
import json

class TwitchDataExtractor:
    """
    Extrator de dados da API Twitch com transformação e limpeza
    """
    
    def __init__(self):
        self.client = TwitchAPIClient()
        self.db = SessionLocal()
        info("Extrator de dados inicializado")
    
    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()
    
    def extract_and_load_games(self, limit: int = 100) -> int:
        """
        Extrair e carregar jogos populares
        """
        info(f"Iniciando extração de {limit} jogos...")
        
        try:
            # Buscar top games
            games_data = self.client.get_top_games(first=limit)
            
            games_loaded = 0
            for game_data in games_data:
                # Transformar e limpar dados
                game = self._transform_game_data(game_data)
                
                # Verificar se já existe
                existing_game = self.db.query(Game).filter(Game.id == game.id).first()
                if existing_game:
                    # Atualizar dados existentes
                    existing_game.name = game.name
                    existing_game.box_art_url = game.box_art_url
                    info(f"Jogo atualizado: {game.name}")
                else:
                    # Inserir novo jogo
                    self.db.add(game)
                    info(f"Novo jogo inserido: {game.name}")
                
                games_loaded += 1
            
            self.db.commit()
            info(f"✅ {games_loaded} jogos carregados com sucesso")
            return games_loaded
            
        except Exception as e:
            error(f"Erro ao extrair jogos: {e}")
            self.db.rollback()
            raise
    
    def extract_and_load_streams(self, limit: int = 100) -> int:
        """
        Extrair e carregar streams ao vivo
        """
        info(f"Iniciando extração de {limit} streams...")
        
        try:
            # Buscar streams ao vivo
            streams_data = self.client.get_streams(first=limit)
            
            # Extrair IDs únicos de usuários e jogos para buscar informações completas
            user_ids = list(set([stream['user_id'] for stream in streams_data]))
            game_ids = list(set([stream['game_id'] for stream in streams_data if stream.get('game_id')]))
            
            # Buscar informações dos usuários
            if user_ids:
                users_data = self.client.get_users(user_ids=user_ids)
                self._load_users(users_data)
            
            # Buscar informações dos jogos (se não estiverem no banco)
            if game_ids:
                games_data = self.client.get_games(game_ids=game_ids)
                self._load_games(games_data)
            
            streams_loaded = 0
            for stream_data in streams_data:
                # Transformar e limpar dados
                stream = self._transform_stream_data(stream_data)
                
                # Verificar se já existe
                existing_stream = self.db.query(Stream).filter(Stream.id == stream.id).first()
                if existing_stream:
                    # Atualizar dados da stream (viewer_count pode mudar)
                    existing_stream.viewer_count = stream.viewer_count
                    existing_stream.title = stream.title
                    info(f"Stream atualizada: {stream.title}")
                else:
                    # Inserir nova stream
                    self.db.add(stream)
                    info(f"Nova stream inserida: {stream.title}")
                
                streams_loaded += 1
            
            self.db.commit()
            info(f"✅ {streams_loaded} streams carregadas com sucesso")
            return streams_loaded
            
        except Exception as e:
            error(f"Erro ao extrair streams: {e}")
            self.db.rollback()
            raise
    
    def extract_and_load_videos_by_game(self, game_id: str, limit: int = 50) -> int:
        """
        Extrair vídeos de um jogo específico
        """
        info(f"Iniciando extração de vídeos para o jogo {game_id}...")
        
        try:
            videos_data = self.client.get_videos(game_id=game_id, first=limit)
            
            # Extrair IDs únicos de usuários
            user_ids = list(set([video['user_id'] for video in videos_data]))
            
            # Buscar informações dos usuários
            if user_ids:
                users_data = self.client.get_users(user_ids=user_ids)
                self._load_users(users_data)
            
            videos_loaded = 0
            for video_data in videos_data:
                # Transformar e limpar dados
                video = self._transform_video_data(video_data)
                
                # Verificar se já existe
                existing_video = self.db.query(Video).filter(Video.id == video.id).first()
                if not existing_video:
                    self.db.add(video)
                    info(f"Novo vídeo inserido: {video.title}")
                    videos_loaded += 1
            
            self.db.commit()
            info(f"✅ {videos_loaded} vídeos carregados para o jogo {game_id}")
            return videos_loaded
            
        except Exception as e:
            error(f"Erro ao extrair vídeos: {e}")
            self.db.rollback()
            raise
    
    def extract_and_load_clips_by_game(self, game_id: str, limit: int = 50) -> int:
        """
        Extrair clips de um jogo específico
        """
        info(f"Iniciando extração de clips para o jogo {game_id}...")
        
        try:
            # Buscar clips da última semana
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            clips_data = self.client.get_clips(
                game_id=game_id, 
                first=limit,
                started_at=start_date.isoformat() + 'Z',
                ended_at=end_date.isoformat() + 'Z'
            )
            
            # Extrair IDs únicos de usuários
            broadcaster_ids = list(set([clip['broadcaster_id'] for clip in clips_data]))
            creator_ids = list(set([clip['creator_id'] for clip in clips_data]))
            all_user_ids = list(set(broadcaster_ids + creator_ids))
            
            # Buscar informações dos usuários
            if all_user_ids:
                users_data = self.client.get_users(user_ids=all_user_ids)
                self._load_users(users_data)
            
            clips_loaded = 0
            for clip_data in clips_data:
                # Transformar e limpar dados
                clip = self._transform_clip_data(clip_data)
                
                # Verificar se já existe
                existing_clip = self.db.query(Clip).filter(Clip.id == clip.id).first()
                if not existing_clip:
                    self.db.add(clip)
                    info(f"Novo clip inserido: {clip.title}")
                    clips_loaded += 1
            
            self.db.commit()
            info(f"✅ {clips_loaded} clips carregados para o jogo {game_id}")
            return clips_loaded
            
        except Exception as e:
            error(f"Erro ao extrair clips: {e}")
            self.db.rollback()
            raise
    
    def _load_users(self, users_data: List[Dict]):
        """
        Carregar usuários no banco (método auxiliar)
        """
        for user_data in users_data:
            user = self._transform_user_data(user_data)
            existing_user = self.db.query(User).filter(User.id == user.id).first()
            if not existing_user:
                self.db.add(user)
                info(f"Novo usuário inserido: {user.display_name}")
    
    def _load_games(self, games_data: List[Dict]):
        """
        Carregar jogos no banco (método auxiliar)
        """
        for game_data in games_data:
            game = self._transform_game_data(game_data)
            existing_game = self.db.query(Game).filter(Game.id == game.id).first()
            if not existing_game:
                self.db.add(game)
                info(f"Novo jogo inserido: {game.name}")
    
    def _transform_user_data(self, data: Dict) -> User:
        """
        Transformar dados de usuário da API para modelo do banco
        """
        return User(
            id=data['id'],
            login=data['login'],
            display_name=data['display_name'],
            type=data.get('type', ''),
            broadcaster_type=data.get('broadcaster_type', ''),
            description=data.get('description', ''),
            profile_image_url=data.get('profile_image_url', ''),
            offline_image_url=data.get('offline_image_url', ''),
            view_count=data.get('view_count', 0),
            created_at=datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')) if data.get('created_at') else None
        )
    
    def _transform_game_data(self, data: Dict) -> Game:
        """
        Transformar dados de jogo da API para modelo do banco
        """
        return Game(
            id=data['id'],
            name=data['name'],
            box_art_url=data.get('box_art_url', '')
        )
    
    def _transform_stream_data(self, data: Dict) -> Stream:
        """
        Transformar dados de stream da API para modelo do banco,
        garantindo que game_id vazio seja inserido como NULL.
        """
        # Se não vier game_id ou vier string vazia, salva None
        game = data.get('game_id') or None

        return Stream(
            id=data['id'],
            user_id=data['user_id'],
            game_id=game,
            title=data.get('title', ''),
            viewer_count=data.get('viewer_count', 0),
            # Converte ISO8601 para datetime, ou deixa None se não informado
            started_at=(
                datetime.fromisoformat(data['started_at'].replace('Z', '+00:00'))
                if data.get('started_at')
                else None
            ),
            language=data.get('language', ''),
            thumbnail_url=data.get('thumbnail_url', ''),
            # Serializa a lista de tag_ids como JSON
            tag_ids=json.dumps(data.get('tag_ids', [])),
            is_mature=data.get('is_mature', False)
        )
    
    def _transform_video_data(self, data: Dict) -> Video:
        """
        Transformar dados de vídeo da API para modelo do banco
        """
        return Video(
            id=data['id'],
            stream_id=data.get('stream_id'),
            user_id=data['user_id'],
            title=data.get('title', ''),
            description=data.get('description', ''),
            created_at=datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')) if data.get('created_at') else None,
            published_at=datetime.fromisoformat(data['published_at'].replace('Z', '+00:00')) if data.get('published_at') else None,
            url=data.get('url', ''),
            thumbnail_url=data.get('thumbnail_url', ''),
            viewable=data.get('viewable', 'public'),
            view_count=data.get('view_count', 0),
            language=data.get('language', ''),
            type=data.get('type', ''),
            duration=data.get('duration', '')
        )
    
    def _transform_clip_data(self, data: Dict) -> Clip:
        """
        Transformar dados de clip da API para modelo do banco
        """
        return Clip(
            id=data['id'],
            url=data.get('url', ''),
            embed_url=data.get('embed_url', ''),
            broadcaster_id=data['broadcaster_id'],
            creator_id=data['creator_id'],
            video_id=data.get('video_id'),
            game_id=data.get('game_id'),
            language=data.get('language', ''),
            title=data.get('title', ''),
            view_count=data.get('view_count', 0),
            created_at=datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')) if data.get('created_at') else None,
            thumbnail_url=data.get('thumbnail_url', ''),
            duration=data.get('duration', 0),
            vod_offset=data.get('vod_offset')
        ) 