"""
Módulo de Filtro de Dados - Remove registros com foreign keys inválidas
Garante integridade referencial antes da carga no banco
"""

import sys
import os
from typing import Dict, Any, List, Set

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

class DataFilter:
    """Filtro de dados para garantir integridade referencial"""
    
    def __init__(self):
        """Inicializa o filtro"""
        self.filter_stats = {}
    
    def filter_data_for_integrity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filtra dados removendo registros com foreign keys inválidas
        
        Args:
            data: Dados transformados completos
            
        Returns:
            Dados filtrados com integridade referencial
        """
        info("🔍 === INICIANDO FILTRO DE INTEGRIDADE ===")
        
        if 'data' not in data:
            error("⚠️ Chave 'data' não encontrada nos dados")
            return data
        
        data_section = data['data']
        filtered_data = data.copy()
        filtered_data['data'] = {}
        
        # Criar conjuntos de IDs válidos para referências
        valid_user_ids = self._get_valid_ids(data_section.get('users', []))
        valid_game_ids = self._get_valid_ids(data_section.get('games', []))
        valid_stream_ids = self._get_valid_ids(data_section.get('streams', []))
        
        info("📊 IDs válidos disponíveis:")
        info("  👥 Users: {}", len(valid_user_ids))
        info("  🎮 Games: {}", len(valid_game_ids))
        info("  📺 Streams: {}", len(valid_stream_ids))
        
        # Filtrar users (sem dependências)
        filtered_data['data']['users'] = data_section.get('users', [])
        
        # Filtrar games (sem dependências)
        filtered_data['data']['games'] = data_section.get('games', [])
        
        # Filtrar streams (depende de users)
        filtered_streams = self._filter_streams(
            data_section.get('streams', []), 
            valid_user_ids
        )
        filtered_data['data']['streams'] = filtered_streams
        
        # Atualizar IDs de streams válidos após filtro
        valid_stream_ids = self._get_valid_ids(filtered_streams)
        
        # Filtrar videos (depende de streams, mas stream_id pode ser null)
        filtered_videos = self._filter_videos(
            data_section.get('videos', []), 
            valid_stream_ids
        )
        filtered_data['data']['videos'] = filtered_videos
        
        # Atualizar IDs de videos válidos
        valid_video_ids = self._get_valid_ids(filtered_videos)
        
        # Filtrar clips (depende de users, games e videos)
        filtered_clips = self._filter_clips(
            data_section.get('clips', []), 
            valid_user_ids, 
            valid_game_ids, 
            valid_video_ids
        )
        filtered_data['data']['clips'] = filtered_clips
        
        # Atualizar summary
        self._update_summary(filtered_data)
        
        # Log estatísticas de filtro
        self._log_filter_stats()
        
        info("✅ === FILTRO DE INTEGRIDADE CONCLUÍDO ===")
        return filtered_data
    
    def _get_valid_ids(self, records: List[Dict]) -> Set[str]:
        """Extrai conjunto de IDs válidos de uma lista de registros"""
        return {str(record['id']) for record in records if 'id' in record}
    
    def _filter_streams(self, streams: List[Dict], valid_user_ids: Set[str]) -> List[Dict]:
        """Filtra streams removendo aqueles com user_id inválido"""
        original_count = len(streams)
        filtered_streams = []
        
        for stream in streams:
            user_id = str(stream.get('user_id', ''))
            
            if user_id in valid_user_ids:
                filtered_streams.append(stream)
            else:
                error("🚫 Stream {} removido: user_id {} inválido", 
                       stream.get('id', 'N/A'), user_id)
        
        filtered_count = len(filtered_streams)
        removed_count = original_count - filtered_count
        
        self.filter_stats['streams'] = {
            'original': original_count,
            'filtered': filtered_count,
            'removed': removed_count
        }
        
        info("🔍 Streams: {} → {} (removidos: {})", 
             original_count, filtered_count, removed_count)
        
        return filtered_streams
    
    def _filter_videos(self, videos: List[Dict], valid_stream_ids: Set[str]) -> List[Dict]:
        """Filtra videos removendo aqueles com stream_id inválido (null é permitido)"""
        original_count = len(videos)
        filtered_videos = []
        
        for video in videos:
            stream_id = video.get('stream_id')
            
            # Permitir stream_id null ou válido
            if stream_id is None or str(stream_id) in valid_stream_ids:
                filtered_videos.append(video)
            else:
                error("🚫 Video {} removido: stream_id {} inválido", 
                       video.get('id', 'N/A'), stream_id)
        
        filtered_count = len(filtered_videos)
        removed_count = original_count - filtered_count
        
        self.filter_stats['videos'] = {
            'original': original_count,
            'filtered': filtered_count,
            'removed': removed_count
        }
        
        info("🔍 Videos: {} → {} (removidos: {})", 
             original_count, filtered_count, removed_count)
        
        return filtered_videos
    
    def _filter_clips(self, clips: List[Dict], valid_user_ids: Set[str], 
                     valid_game_ids: Set[str], valid_video_ids: Set[str]) -> List[Dict]:
        """Filtra clips removendo aqueles com foreign keys inválidas"""
        original_count = len(clips)
        filtered_clips = []
        
        for clip in clips:
            user_id = str(clip.get('user_id', ''))
            game_id = clip.get('game_id')  # Pode ser null
            video_id = clip.get('video_id')  # Pode ser null
            
            # Verificar user_id (obrigatório)
            if user_id not in valid_user_ids:
                error("🚫 Clip {} removido: user_id {} inválido", 
                       clip.get('id', 'N/A'), user_id)
                continue
            
            # Verificar game_id (opcional)
            if game_id is not None and str(game_id) not in valid_game_ids:
                error("🚫 Clip {} removido: game_id {} inválido", 
                       clip.get('id', 'N/A'), game_id)
                continue
            
            # Verificar video_id (opcional)
            if video_id is not None and str(video_id) not in valid_video_ids:
                error("🚫 Clip {} removido: video_id {} inválido", 
                       clip.get('id', 'N/A'), video_id)
                continue
            
            filtered_clips.append(clip)
        
        filtered_count = len(filtered_clips)
        removed_count = original_count - filtered_count
        
        self.filter_stats['clips'] = {
            'original': original_count,
            'filtered': filtered_count,
            'removed': removed_count
        }
        
        info("🔍 Clips: {} → {} (removidos: {})", 
             original_count, filtered_count, removed_count)
        
        return filtered_clips
    
    def _update_summary(self, data: Dict[str, Any]):
        """Atualiza o summary com os dados filtrados"""
        if 'summary' in data and 'data' in data:
            data_section = data['data']
            
            data['summary'].update({
                'total_users': len(data_section.get('users', [])),
                'total_games': len(data_section.get('games', [])),
                'total_streams': len(data_section.get('streams', [])),
                'total_videos': len(data_section.get('videos', [])),
                'total_clips': len(data_section.get('clips', []))
            })
    
    def _log_filter_stats(self):
        """Loga estatísticas do filtro"""
        info("📊 === ESTATÍSTICAS DO FILTRO ===")
        
        total_original = 0
        total_filtered = 0
        total_removed = 0
        
        for table_name, stats in self.filter_stats.items():
            original = stats.get('original', 0)
            filtered = stats.get('filtered', 0)
            removed = stats.get('removed', 0)
            
            total_original += original
            total_filtered += filtered
            total_removed += removed
            
            if removed > 0:
                percentage_removed = (removed / original) * 100 if original > 0 else 0
                info("  🔍 {}: {} removidos ({:.1f}%)", 
                     table_name.capitalize(), removed, percentage_removed)
            else:
                info("  ✅ {}: nenhum removido", table_name.capitalize())
        
        if total_removed > 0:
            info("📊 TOTAL: {} registros removidos de {} originais", 
                 total_removed, total_original)
        else:
            info("✅ Nenhum registro removido - todos os dados são válidos")
        
        info("📊 =" * 50)

def main():
    """Função principal de teste"""
    info("🚀 === SCRIPT DE FILTRO DE DADOS ===")
    
    # Este script seria usado dentro do processo de carga
    info("ℹ️ Este módulo é usado pelo run_all_loads.py")

if __name__ == "__main__":
    main() 