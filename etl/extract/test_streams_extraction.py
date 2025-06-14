"""
Script de Teste - Extração baseada em Streams Ativas
Busca streams → user_ids → vídeos (04-05/2025) → clips
"""

import sys
import os
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Imports dos módulos de configuração
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from twitch_client import TwitchAPIClient
from settings import ETLConfig

class StreamBasedExtractor:
    """Extrator baseado em streams ativas"""
    
    def __init__(self):
        """Inicializa o extrator"""
        self.client = TwitchAPIClient()
        self.data_dir = self._ensure_data_dir()
        
        # Dados extraídos
        self.streams_data = []
        self.users_data = []
        self.videos_data = []
        self.clips_data = []
    
    def _ensure_data_dir(self) -> str:
        """Garante que o diretório de dados existe"""
        data_dir = os.path.join(ETLConfig.BASE_DIR, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            info("📁 Diretório de dados criado: {}", data_dir)
        return data_dir
    
    def _is_latin_only_title(self, title: str) -> bool:
        """
        Verifica se o título contém APENAS caracteres latinos
        
        Args:
            title: Título a ser verificado
            
        Returns:
            True se contém apenas letras latinas, False caso contrário
        """
        if not title:
            return False
        
        # Regex para aceitar apenas:
        # - Letras latinas (A-Z, a-z)
        # - Números (0-9)
        # - Espaços
        # - Pontuação básica (. , ! ? : ; - _ ' " ( ) [ ] { })
        latin_pattern = r'^[A-Za-z0-9\s\.\,\!\?\:\;\-\_\'\"\(\)\[\]\{\}]+$'
        
        return bool(re.match(latin_pattern, title))
    
    def _filter_latin_titles(self, items: List[Dict], title_field: str = 'title') -> List[Dict]:
        """
        Filtra itens mantendo apenas aqueles com títulos latinos
        
        Args:
            items: Lista de itens a filtrar
            title_field: Nome do campo que contém o título
            
        Returns:
            Lista filtrada com apenas títulos latinos
        """
        if not items:
            return items
        
        original_count = len(items)
        filtered_items = []
        
        for item in items:
            title = item.get(title_field, '')
            
            if self._is_latin_only_title(title):
                filtered_items.append(item)
            else:
                # Log título rejeitado (apenas primeiros 50 caracteres)
                short_title = title[:50] + '...' if len(title) > 50 else title
                info("❌ Título rejeitado (não-latino): {}", short_title)
        
        filtered_count = len(filtered_items)
        removed_count = original_count - filtered_count
        
        info("🔤 Filtro latino: {} itens → {} mantidos, {} removidos", 
             original_count, filtered_count, removed_count)
        
        return filtered_items
    
    def extract_streams_and_user_ids(self, limit: int = 500) -> List[str]:
        """
        Extrai streams ativas e retorna user_ids únicos
        
        Args:
            limit: Número máximo de streams/users
            
        Returns:
            Lista de user_ids únicos
        """
        info("📺 Buscando {} streams ativas para extrair user_ids...", limit)
        
        try:
            # Buscar streams ativas
            all_streams = []
            batch_size = 100  # API limita a 100 por request
            
            while len(all_streams) < limit:
                remaining = limit - len(all_streams)
                current_batch_size = min(batch_size, remaining)
                
                info("📦 Buscando lote de {} streams...", current_batch_size)
                streams = self.client.get_streams(limit=current_batch_size)
                
                if not streams:
                    info("ℹ️ Nenhuma stream retornada, finalizando")
                    break
                
                all_streams.extend(streams)
                info("✅ {} streams obtidas (total: {})", len(streams), len(all_streams))
                
                # Se retornou menos que solicitado, provavelmente acabaram
                if len(streams) < current_batch_size:
                    info("ℹ️ Menos streams retornadas que solicitado, finalizando")
                    break
            
            # Filtrar streams com títulos latinos apenas
            info("🔤 Aplicando filtro de títulos latinos nas streams...")
            filtered_streams = self._filter_latin_titles(all_streams, 'title')
            
            # Extrair user_ids únicos das streams filtradas
            user_ids = []
            seen_ids = set()
            
            for stream in filtered_streams:
                user_id = stream.get('user_id')
                if user_id and user_id not in seen_ids:
                    user_ids.append(user_id)
                    seen_ids.add(user_id)
            
            self.streams_data = filtered_streams
            
            info("🎯 {} streams processadas (antes do filtro), {} mantidas (após filtro latino)", 
                 len(all_streams), len(filtered_streams))
            info("👥 {} user_ids únicos extraídos das streams filtradas", len(user_ids))
            
            # Log das top 5 streams por viewer count (das filtradas)
            top_streams = sorted(filtered_streams, key=lambda x: x.get('viewer_count', 0), reverse=True)[:5]
            info("🔥 Top 5 streams:")
            for i, stream in enumerate(top_streams, 1):
                info("  {}º - {}: {} viewers", 
                     i, stream.get('user_name', 'Desconhecido'), 
                     stream.get('viewer_count', 0))
            
            return user_ids
            
        except Exception as e:
            error("💥 Erro ao extrair streams e user_ids: {}", e)
            return []
    
    def extract_users_data(self, user_ids: List[str]) -> List[Dict]:
        """
        Extrai dados dos usuários (SEM view_count)
        
        Args:
            user_ids: Lista de IDs de usuários
            
        Returns:
            Lista de dados de usuários
        """
        info("👤 Extraindo dados de {} usuários...", len(user_ids))
        
        try:
            all_users = []
            batch_size = 100  # API limita a 100 por request
            
            for i in range(0, len(user_ids), batch_size):
                batch_ids = user_ids[i:i + batch_size]
                info("📦 Processando lote {}/{} ({} usuários)", 
                     i//batch_size + 1, 
                     (len(user_ids) + batch_size - 1)//batch_size, 
                     len(batch_ids))
                
                users = self.client.get_users(ids=batch_ids)
                
                if users:
                    # Remover view_count de cada usuário
                    for user in users:
                        if 'view_count' in user:
                            del user['view_count']
                    
                    all_users.extend(users)
                    info("✅ {} usuários processados neste lote", len(users))
            
            # Filtrar usuários com display_name latino apenas
            info("🔤 Aplicando filtro de títulos latinos nos usuários (display_name)...")
            filtered_users = self._filter_latin_titles(all_users, 'display_name')
            
            self.users_data = filtered_users
            info("✅ {} usuários extraídos no total (após filtro latino)", len(filtered_users))
            
            # Log de alguns usuários
            for i, user in enumerate(filtered_users[:5], 1):
                info("  {}º - {} ({})", 
                     i, user.get('display_name', 'N/A'), 
                     user.get('broadcaster_type', 'normal'))
            
            return filtered_users
            
        except Exception as e:
            error("💥 Erro ao extrair dados de usuários: {}", e)
            return []
    
    def extract_videos_by_date_range(self, user_ids: List[str]) -> List[Dict]:
        """
        Extrai vídeos dos usuários entre 04/2025 e 05/2025
        
        Args:
            user_ids: Lista de IDs de usuários
            
        Returns:
            Lista de dados de vídeos
        """
        info("🎬 Extraindo vídeos entre 04/2025 e 05/2025...")
        
        try:
            all_videos = []
            
            # Datas de filtro (04/2025 a 05/2025)
            start_date = datetime(2025, 4, 1)
            end_date = datetime(2025, 5, 31, 23, 59, 59)
            
            info("📅 Período: {} até {}", 
                 start_date.strftime('%d/%m/%Y'), 
                 end_date.strftime('%d/%m/%Y'))
            
            for i, user_id in enumerate(user_ids, 1):
                info("🎬 Buscando vídeos do usuário {}/{}: {}", i, len(user_ids), user_id)
                
                # Buscar vídeos do usuário
                videos = self.client.get_videos(user_ids=[user_id], limit=50)
                
                if videos:
                    # Filtrar por data
                    filtered_videos = []
                    for video in videos:
                        created_at = video.get('created_at', '')
                        if created_at:
                            try:
                                # Parse da data ISO format
                                video_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                video_date = video_date.replace(tzinfo=None)  # Remove timezone
                                
                                if start_date <= video_date <= end_date:
                                    filtered_videos.append(video)
                            except:
                                pass  # Ignora vídeos com data inválida
                    
                    if filtered_videos:
                        all_videos.extend(filtered_videos)
                        info("✅ {} vídeos válidos encontrados para usuário {}", 
                             len(filtered_videos), user_id)
                    else:
                        info("ℹ️ Nenhum vídeo no período para usuário {}", user_id)
                else:
                    info("ℹ️ Nenhum vídeo encontrado para usuário {}", user_id)
                
                                 # Log para cada usuário no teste
                info("📊 Progresso: {}/{} usuários processados, {} vídeos encontrados", 
                     i, len(user_ids), len(all_videos))
            
            # Filtrar vídeos com títulos latinos apenas
            info("🔤 Aplicando filtro de títulos latinos nos vídeos...")
            filtered_videos = self._filter_latin_titles(all_videos, 'title')
            
            self.videos_data = filtered_videos
            info("✅ {} vídeos no período extraídos no total (após filtro latino)", len(filtered_videos))
            
            if filtered_videos:
                # Log dos vídeos com mais views
                top_videos = sorted(filtered_videos, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                info("🏆 Top 3 vídeos por views:")
                for i, video in enumerate(top_videos, 1):
                    info("  {}º - {} views: {}", 
                         i, video.get('view_count', 0), 
                         video.get('title', 'Sem título')[:50])
            
            return filtered_videos
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos por data: {}", e)
            return []
    
    def extract_clips_from_users(self, user_ids: List[str]) -> List[Dict]:
        """
        Extrai clips dos usuários (qualquer data)
        
        Args:
            user_ids: Lista de IDs de usuários
            
        Returns:
            Lista de dados de clips
        """
        info("🎭 Extraindo clips de qualquer data...")
        
        try:
            all_clips = []
            
            for i, user_id in enumerate(user_ids, 1):
                info("🎭 Buscando clips do usuário {}/{}: {}", i, len(user_ids), user_id)
                
                # Buscar clips do usuário (broadcaster_id)
                clips = self.client.get_clips(broadcaster_ids=[user_id], limit=30)
                
                if clips:
                    all_clips.extend(clips)
                    info("✅ {} clips encontrados para usuário {}", len(clips), user_id)
                    
                    # Log do clip mais popular
                    top_clip = max(clips, key=lambda x: x.get('view_count', 0))
                    info("  🏆 Clip mais popular: {} views - {}", 
                         top_clip.get('view_count', 0), 
                         top_clip.get('title', 'Sem título')[:40])
                else:
                    info("ℹ️ Nenhum clip encontrado para usuário {}", user_id)
                
                                 # Log para cada usuário no teste
                info("📊 Progresso: {}/{} usuários processados, {} clips encontrados", 
                     i, len(user_ids), len(all_clips))
            
            # Filtrar clips com títulos latinos apenas
            info("🔤 Aplicando filtro de títulos latinos nos clips...")
            filtered_clips = self._filter_latin_titles(all_clips, 'title')
            
            self.clips_data = filtered_clips
            info("✅ {} clips extraídos no total (após filtro latino)", len(filtered_clips))
            
            if filtered_clips:
                # Log dos clips com mais views
                top_clips = sorted(filtered_clips, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                info("🏆 Top 3 clips por views:")
                for i, clip in enumerate(top_clips, 1):
                    info("  {}º - {} views: {}", 
                         i, clip.get('view_count', 0), 
                         clip.get('title', 'Sem título')[:40])
            
            return filtered_clips
            
        except Exception as e:
            error("💥 Erro ao extrair clips: {}", e)
            return []
    
    def save_all_data(self) -> str:
        """
        Salva todos os dados extraídos em arquivo JSON
        
        Returns:
            Caminho do arquivo salvo
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"stream_based_extraction_{timestamp}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            final_data = {
                'extraction_info': {
                    'timestamp': datetime.now().isoformat(),
                    'method': 'stream_based_extraction',
                    'date_filter': '2025-04 to 2025-05 (videos only)',
                    'title_filter': 'Latin characters only (A-Z, a-z, 0-9, basic punctuation)'
                },
                'summary': {
                    'total_streams': len(self.streams_data),
                    'total_users': len(self.users_data),
                    'total_videos': len(self.videos_data),
                    'total_clips': len(self.clips_data)
                },
                'data': {
                    'streams': self.streams_data,
                    'users': self.users_data,
                    'videos': self.videos_data,
                    'clips': self.clips_data
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Todos os dados salvos em: {}", filepath)
            return filepath
            
        except Exception as e:
            error("💥 Erro ao salvar dados: {}", e)
            return ""
    
    def run_full_extraction(self, limit_users: int = 500) -> Dict:
        """
        Executa extração completa baseada em streams
        
        Args:
            limit_users: Número máximo de usuários para processar
            
        Returns:
            Resumo da extração
        """
        info("🚀 === INICIANDO EXTRAÇÃO BASEADA EM STREAMS ===")
        start_time = datetime.now()
        
        try:
            # 1. Extrair streams e user_ids
            info("📝 Etapa 1: Streams ativas → User IDs")
            user_ids = self.extract_streams_and_user_ids(limit_users)
            
            if not user_ids:
                error("❌ Nenhum user_id extraído das streams")
                return {'success': False, 'error': 'No user_ids found'}
            
            # Expandir para mais usuários agora que o teste funcionou
            test_user_ids = user_ids[:50]  # 50 usuários para extração real
            info("🚀 PRODUÇÃO: Usando {} usuários para extração completa", len(test_user_ids))
            
            # 2. Extrair dados dos usuários
            info("📝 Etapa 2: Dados dos usuários")
            users = self.extract_users_data(test_user_ids)
            
            # 3. Extrair vídeos (04-05/2025)
            info("📝 Etapa 3: Vídeos (04-05/2025)")
            videos = self.extract_videos_by_date_range(test_user_ids)
            
            # 4. Extrair clips (qualquer data)
            info("📝 Etapa 4: Clips (qualquer data)")
            clips = self.extract_clips_from_users(test_user_ids)
            
            # 5. Salvar dados
            info("📝 Etapa 5: Salvando dados")
            filepath = self.save_all_data()
            
            # Calcular tempo total
            end_time = datetime.now()
            total_time = end_time - start_time
            
            # Resumo final
            summary = {
                'success': True,
                'filepath': filepath,
                'total_time': str(total_time),
                'stats': {
                    'streams_processed': len(self.streams_data),
                    'unique_users': len(self.users_data),
                    'videos_found': len(self.videos_data),
                    'clips_found': len(self.clips_data)
                }
            }
            
            info("📊 === RESUMO FINAL ===")
            info("⏱️ Tempo total: {}", total_time)
            info("📺 Streams processadas: {}", summary['stats']['streams_processed'])
            info("👥 Usuários únicos: {}", summary['stats']['unique_users'])
            info("🎬 Vídeos (04-05/2025): {}", summary['stats']['videos_found'])
            info("🎭 Clips (qualquer data): {}", summary['stats']['clips_found'])
            info("💾 Arquivo salvo: {}", filepath)
            
            if filepath:
                info("✅ Extração baseada em streams concluída com sucesso!")
            else:
                error("❌ Falha ao salvar dados")
            
            return summary
            
        except Exception as e:
            error("💥 Erro na extração: {}", e)
            return {'success': False, 'error': str(e)}

def main():
    """Função principal de teste"""
    extractor = StreamBasedExtractor()
    result = extractor.run_full_extraction(limit_users=500)
    
    if result['success']:
        info("🎉 Teste concluído com sucesso!")
    else:
        error("💥 Teste falhou: {}", result.get('error', 'Erro desconhecido'))

if __name__ == "__main__":
    main() 