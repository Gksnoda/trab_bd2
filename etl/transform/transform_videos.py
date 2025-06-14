"""
Transformador de Dados de Vídeos
Limpa e valida dados de vídeos da API Twitch
"""

import sys
import os
from typing import List, Dict, Any

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da classe base
from base_transformer import BaseTransformer

class VideoTransformer(BaseTransformer):
    """Transformador específico para dados de vídeos"""
    
    def __init__(self):
        """Inicializa o transformador de vídeos"""
        super().__init__()
        
        # Campos obrigatórios para vídeos
        self.required_fields = [
            'id',
            'title',
            'created_at',
            'type'
        ]
        
        # Limites de tamanho para strings
        self.string_limits = {
            'title': 200,         # Video title limit
            'description': 500,   # Video description limit
            'language': 10,       # Language code limit
            'type': 20,           # Video type (archive, highlight, upload)
            'duration': 20        # Duration string format
        }
    
    def transform_videos(self, videos_data: List[Dict]) -> List[Dict]:
        """
        Transforma dados brutos de vídeos
        
        Args:
            videos_data: Lista de dados brutos de vídeos
            
        Returns:
            Lista de vídeos transformados e validados
        """
        if not videos_data:
            info("⚠️ Nenhum dado de vídeo para transformar")
            return []
        
        info("🔄 Iniciando transformação de {} vídeos...", len(videos_data))
        self.stats['processed'] = len(videos_data)
        
        # 1. Limpar valores nulos em campos obrigatórios
        cleaned_videos = self.clean_null_values(videos_data, self.required_fields)
        
        # 2. Transformar cada vídeo individualmente
        transformed_videos = []
        for video in cleaned_videos:
            transformed_video = self._transform_single_video(video)
            if transformed_video:
                transformed_videos.append(transformed_video)
        
        # 3. Remover duplicatas baseado no ID
        unique_videos = self.remove_duplicates(transformed_videos, 'id')
        
        # 4. Log estatísticas finais
        self.log_final_stats('videos')
        
        info("✅ Transformação vídeos concluída: {} vídeos válidos", len(unique_videos))
        return unique_videos
    
    def _transform_single_video(self, video: Dict) -> Dict:
        """
        Transforma um único vídeo
        
        Args:
            video: Dados brutos do vídeo
            
        Returns:
            Vídeo transformado ou None se inválido
        """
        try:
            transformed = {}
            
            # ID do vídeo (obrigatório)
            video_id = self.validate_string(video.get('id'), 'id')
            if not video_id:
                return None
            transformed['id'] = video_id
            
            # Stream ID (opcional - pode não ter stream associado)
            # Na API não vem stream_id diretamente, precisa ser inferido ou vir de join
            stream_id = self.validate_string(video.get('stream_id'), 'stream_id')
            transformed['stream_id'] = stream_id
            
            # Título do vídeo (obrigatório)
            title = self.validate_string(
                video.get('title'), 
                'title', 
                self.string_limits['title']
            )
            if not title:
                return None
            transformed['title'] = title
            
            # Descrição (opcional)
            description = self.validate_string(
                video.get('description'), 
                'description', 
                self.string_limits['description']
            )
            transformed['description'] = description
            
            # Data de criação (obrigatório)
            created_at = self.standardize_datetime(video.get('created_at'))
            if not created_at:
                info("⚠️ Vídeo {} sem data de criação válida", video_id)
                return None
            transformed['created_at'] = created_at
            
            # Data de publicação (opcional, pode ser diferente da criação)
            published_at = self.standardize_datetime(video.get('published_at'))
            transformed['published_at'] = published_at
            
            # URL do vídeo (opcional mas útil)
            url = self.validate_string(video.get('url'), 'url')
            transformed['url'] = url
            
            # URL da thumbnail (opcional)
            thumbnail_url = self.validate_string(video.get('thumbnail_url'), 'thumbnail_url')
            transformed['thumbnail_url'] = thumbnail_url
            
            # Tipo do vídeo (obrigatório: archive, highlight, upload)
            video_type = self.validate_string(
                video.get('type'), 
                'type', 
                self.string_limits['type']
            )
            if not video_type:
                return None
            # Validar tipos aceitos
            valid_types = ['archive', 'highlight', 'upload']
            if video_type not in valid_types:
                info("⚠️ Tipo de vídeo inválido '{}' para vídeo {}, usando 'archive'", 
                       video_type, video_id)
                video_type = 'archive'
            transformed['type'] = video_type
            
            # Duração (opcional mas importante)
            duration = self.validate_string(
                video.get('duration'), 
                'duration', 
                self.string_limits['duration']
            )
            transformed['duration'] = duration
            
            # Idioma (opcional)
            language = self.validate_string(
                video.get('language'), 
                'language', 
                self.string_limits['language']
            )
            transformed['language'] = language if language else 'en'  # Default inglês
            
            # Viewcount (opcional, deve ser >= 0)
            view_count = self.validate_integer(
                video.get('view_count'), 
                'view_count', 
                min_value=0
            )
            transformed['view_count'] = view_count if view_count is not None else 0
            
            # is_public removido - não está no MER
            
            return transformed
            
        except Exception as e:
            error("💥 Erro ao transformar vídeo {}: {}", 
                  video.get('id', 'desconhecido'), e)
            self.stats['validation_errors'] += 1
            return None
    
    def validate_transformed_videos(self, videos: List[Dict]) -> bool:
        """
        Valida se todos os vídeos transformados estão corretos
        
        Args:
            videos: Lista de vídeos transformados
            
        Returns:
            True se todos válidos, False caso contrário
        """
        if not videos:
            return False
        
        info("🔍 Validando {} vídeos transformados...", len(videos))
        
        valid_count = 0
        for video in videos:
            if self._validate_single_video(video):
                valid_count += 1
        
        is_valid = valid_count == len(videos)
        
        if is_valid:
            info("✅ Todos os {} vídeos são válidos", len(videos))
        else:
            error("❌ Apenas {}/{} vídeos são válidos", valid_count, len(videos))
        
        return is_valid
    
    def _validate_single_video(self, video: Dict) -> bool:
        """Valida um único vídeo transformado"""
        required_keys = [
            'id', 
            'title', 'created_at', 'type'
        ]
        
        for key in required_keys:
            if key not in video or video[key] is None:
                info("⚠️ Vídeo inválido - campo '{}' ausente: {}", 
                       key, video.get('id', 'desconhecido'))
                return False
        
        # Validações específicas
        valid_types = ['archive', 'highlight', 'upload']
        if video['type'] not in valid_types:
            info("⚠️ Vídeo inválido - tipo '{}' não aceito: {}", 
                   video['type'], video.get('id'))
            return False
        
        if 'view_count' in video and video['view_count'] < 0:
            info("⚠️ Vídeo inválido - view_count negativo: {}", video.get('id'))
            return False
        
        return True 
