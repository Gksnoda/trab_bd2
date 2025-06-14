"""
Transformador de Dados de Streams
Limpa e valida dados de streams da API Twitch
"""

import sys
import os
from typing import List, Dict, Any

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da classe base
from base_transformer import BaseTransformer

class StreamTransformer(BaseTransformer):
    """Transformador específico para dados de streams"""
    
    def __init__(self):
        """Inicializa o transformador de streams"""
        super().__init__()
        
        # Campos obrigatórios para streams
        self.required_fields = [
            'id',
            'user_id',
            'title',
            'started_at'
        ]
        
        # Limites de tamanho para strings
        self.string_limits = {
            'title': 140,         # Stream title limit (como Twitter)
            'language': 10       # Language code limit
        }
    
    def transform_streams(self, streams_data: List[Dict]) -> List[Dict]:
        """
        Transforma dados brutos de streams
        
        Args:
            streams_data: Lista de dados brutos de streams
            
        Returns:
            Lista de streams transformados e validados
        """
        if not streams_data:
            info("⚠️ Nenhum dado de stream para transformar")
            return []
        
        info("🔄 Iniciando transformação de {} streams...", len(streams_data))
        self.stats['processed'] = len(streams_data)
        
        # 1. Limpar valores nulos em campos obrigatórios
        cleaned_streams = self.clean_null_values(streams_data, self.required_fields)
        
        # 2. Transformar cada stream individualmente
        transformed_streams = []
        for stream in cleaned_streams:
            transformed_stream = self._transform_single_stream(stream)
            if transformed_stream:
                transformed_streams.append(transformed_stream)
        
        # 3. Remover duplicatas baseado no ID
        unique_streams = self.remove_duplicates(transformed_streams, 'id')
        
        # 4. Log estatísticas finais
        self.log_final_stats('streams')
        
        info("✅ Transformação streams concluída: {} streams válidos", len(unique_streams))
        return unique_streams
    
    def _transform_single_stream(self, stream: Dict) -> Dict:
        """
        Transforma uma única stream
        
        Args:
            stream: Dados brutos da stream
            
        Returns:
            Stream transformado ou None se inválido
        """
        try:
            transformed = {}
            
            # ID da stream (obrigatório)
            stream_id = self.validate_string(stream.get('id'), 'id')
            if not stream_id:
                return None
            transformed['id'] = stream_id
            
            # User ID (obrigatório)
            user_id = self.validate_string(stream.get('user_id'), 'user_id')
            if not user_id:
                return None
            transformed['user_id'] = user_id
            
            # user_login e user_name removidos - não estão no MER
            
            # game_id removido - não está no MER
            
            # Título da stream (obrigatório)
            title = self.validate_string(
                stream.get('title'), 
                'title', 
                self.string_limits['title']
            )
            if not title:
                return None
            transformed['title'] = title
            
            # Número de viewers (obrigatório, deve ser >= 0)
            viewer_count = self.validate_integer(
                stream.get('viewer_count'), 
                'viewer_count', 
                min_value=0
            )
            if viewer_count is None:
                viewer_count = 0  # Default para 0 se não informado
            transformed['viewer_count'] = viewer_count
            
            # Data/hora de início (obrigatório)
            started_at = self.standardize_datetime(stream.get('started_at'))
            if not started_at:
                info("⚠️ Stream {} sem data de início válida", stream_id)
                return None
            transformed['started_at'] = started_at
            
            # Idioma (opcional)
            language = self.validate_string(
                stream.get('language'), 
                'language', 
                self.string_limits['language']
            )
            transformed['language'] = language if language else 'en'  # Default inglês
            
            # URL da thumbnail (opcional)
            thumbnail_url = self.validate_string(stream.get('thumbnail_url'), 'thumbnail_url')
            transformed['thumbnail_url'] = thumbnail_url
            
            # tags e is_mature removidos - não estão no MER
            
            return transformed
            
        except Exception as e:
            error("💥 Erro ao transformar stream {}: {}", 
                  stream.get('id', 'desconhecido'), e)
            self.stats['validation_errors'] += 1
            return None
    
    def validate_transformed_streams(self, streams: List[Dict]) -> bool:
        """
        Valida se todos os streams transformados estão corretos
        
        Args:
            streams: Lista de streams transformados
            
        Returns:
            True se todos válidos, False caso contrário
        """
        if not streams:
            return False
        
        info("🔍 Validando {} streams transformados...", len(streams))
        
        valid_count = 0
        for stream in streams:
            if self._validate_single_stream(stream):
                valid_count += 1
        
        is_valid = valid_count == len(streams)
        
        if is_valid:
            info("✅ Todos os {} streams são válidos", len(streams))
        else:
            error("❌ Apenas {}/{} streams são válidos", valid_count, len(streams))
        
        return is_valid
    
    def _validate_single_stream(self, stream: Dict) -> bool:
        """Valida um único stream transformado"""
        required_keys = [
            'id', 'user_id', 'title', 'started_at'
        ]
        
        for key in required_keys:
            if key not in stream or stream[key] is None:
                info("⚠️ Stream inválido - campo '{}' ausente: {}", 
                       key, stream.get('id', 'desconhecido'))
                return False
        
        # Validações específicas
        if 'viewer_count' in stream and stream['viewer_count'] < 0:
            info("⚠️ Stream inválido - viewer_count negativo: {}", stream.get('id'))
            return False
        
        return True 
