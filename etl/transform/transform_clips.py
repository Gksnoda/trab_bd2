"""
Transformador de Dados de Clips
Limpa e valida dados de clips da API Twitch
"""

import sys
import os
from typing import List, Dict, Any

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da classe base
from base_transformer import BaseTransformer

class ClipTransformer(BaseTransformer):
    """Transformador específico para dados de clips"""
    
    def __init__(self):
        """Inicializa o transformador de clips"""
        super().__init__()
        
        # Campos obrigatórios para clips
        self.required_fields = [
            'id',
            'broadcaster_id',  # Na API vem como broadcaster_id, será mapeado para user_id
            'title',
            'created_at'
        ]
        
        # Limites de tamanho para strings
        self.string_limits = {
            'title': 100,             # Clip title limit
            'game_id': 20,            # Game ID limit
            'language': 10,           # Language code limit
            'duration': 10            # Duration (geralmente poucos segundos)
        }
    
    def transform_clips(self, clips_data: List[Dict]) -> List[Dict]:
        """
        Transforma dados brutos de clips
        
        Args:
            clips_data: Lista de dados brutos de clips
            
        Returns:
            Lista de clips transformados e validados
        """
        if not clips_data:
            info("⚠️ Nenhum dado de clip para transformar")
            return []
        
        info("🔄 Iniciando transformação de {} clips...", len(clips_data))
        self.stats['processed'] = len(clips_data)
        
        # 1. Limpar valores nulos em campos obrigatórios
        cleaned_clips = self.clean_null_values(clips_data, self.required_fields)
        
        # 2. Transformar cada clip individualmente
        transformed_clips = []
        for clip in cleaned_clips:
            transformed_clip = self._transform_single_clip(clip)
            if transformed_clip:
                transformed_clips.append(transformed_clip)
        
        # 3. Remover duplicatas baseado no ID
        unique_clips = self.remove_duplicates(transformed_clips, 'id')
        
        # 4. Log estatísticas finais
        self.log_final_stats('clips')
        
        info("✅ Transformação clips concluída: {} clips válidos", len(unique_clips))
        return unique_clips
    
    def _transform_single_clip(self, clip: Dict) -> Dict:
        """
        Transforma um único clip
        
        Args:
            clip: Dados brutos do clip
            
        Returns:
            Clip transformado ou None se inválido
        """
        try:
            transformed = {}
            
            # ID do clip (obrigatório)
            clip_id = self.validate_string(clip.get('id'), 'id')
            if not clip_id:
                return None
            transformed['id'] = clip_id
            
            # User ID (obrigatório - usuário associado ao clip)
            user_id = self.validate_string(clip.get('broadcaster_id'), 'user_id')
            if not user_id:
                return None
            transformed['user_id'] = user_id
            
            # Título do clip (obrigatório)
            title = self.validate_string(
                clip.get('title'), 
                'title', 
                self.string_limits['title']
            )
            if not title:
                return None
            transformed['title'] = title
            
            # Game ID (opcional - pode ser de um jogo específico)
            game_id = self.validate_string(
                clip.get('game_id'), 
                'game_id', 
                self.string_limits['game_id']
            )
            transformed['game_id'] = game_id
            
            # Data de criação (obrigatório)
            created_at = self.standardize_datetime(clip.get('created_at'))
            if not created_at:
                info("⚠️ Clip {} sem data de criação válida", clip_id)
                return None
            transformed['created_at'] = created_at
            
            # URL do clip (opcional mas importante)
            url = self.validate_string(clip.get('url'), 'url')
            transformed['url'] = url
            
            # Embed URL (opcional)
            embed_url = self.validate_string(clip.get('embed_url'), 'embed_url')
            transformed['embed_url'] = embed_url
            
            # URL da thumbnail (opcional)
            thumbnail_url = self.validate_string(clip.get('thumbnail_url'), 'thumbnail_url')
            transformed['thumbnail_url'] = thumbnail_url
            
            # Duração em segundos (opcional mas importante)
            duration = clip.get('duration')
            if duration is not None:
                # Pode vir como float (segundos)
                try:
                    duration_float = float(duration)
                    if duration_float > 0:
                        transformed['duration'] = duration_float
                    else:
                        transformed['duration'] = None
                except (ValueError, TypeError):
                    info("⚠️ Duração inválida para clip {}: {}", clip_id, duration)
                    transformed['duration'] = None
            else:
                transformed['duration'] = None
            
            # View count (opcional, deve ser >= 0)
            view_count = self.validate_integer(
                clip.get('view_count'), 
                'view_count', 
                min_value=0
            )
            transformed['view_count'] = view_count if view_count is not None else 0
            
            # Idioma (opcional)
            language = self.validate_string(
                clip.get('language'), 
                'language', 
                self.string_limits['language']
            )
            transformed['language'] = language if language else 'en'  # Default inglês
            
            # is_featured removido - não está no MER
            
            return transformed
            
        except Exception as e:
            error("💥 Erro ao transformar clip {}: {}", 
                  clip.get('id', 'desconhecido'), e)
            self.stats['validation_errors'] += 1
            return None
    
    def validate_transformed_clips(self, clips: List[Dict]) -> bool:
        """
        Valida se todos os clips transformados estão corretos
        
        Args:
            clips: Lista de clips transformados
            
        Returns:
            True se todos válidos, False caso contrário
        """
        if not clips:
            return False
        
        info("🔍 Validando {} clips transformados...", len(clips))
        
        valid_count = 0
        for clip in clips:
            if self._validate_single_clip(clip):
                valid_count += 1
        
        is_valid = valid_count == len(clips)
        
        if is_valid:
            info("✅ Todos os {} clips são válidos", len(clips))
        else:
            error("❌ Apenas {}/{} clips são válidos", valid_count, len(clips))
        
        return is_valid
    
    def _validate_single_clip(self, clip: Dict) -> bool:
        """Valida um único clip transformado"""
        required_keys = [
            'id', 'user_id',  # Após transformação deve ter user_id
            'title', 'created_at'
        ]
        
        for key in required_keys:
            if key not in clip or clip[key] is None:
                info("⚠️ Clip inválido - campo '{}' ausente: {}", 
                       key, clip.get('id', 'desconhecido'))
                return False
        
        # Validações específicas
        if 'view_count' in clip and clip['view_count'] < 0:
            info("⚠️ Clip inválido - view_count negativo: {}", clip.get('id'))
            return False
        
        if 'duration' in clip and clip['duration'] is not None and clip['duration'] <= 0:
            info("⚠️ Clip inválido - duração inválida: {}", clip.get('id'))
            return False
        
        return True 
