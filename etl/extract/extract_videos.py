"""
Script de Extração de Vídeos - Twitch API
Busca vídeos salvos de streamers específicos
"""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Imports dos módulos de configuração
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from twitch_client import TwitchAPIClient
from settings import ETLConfig

class VideoExtractor:
    """Extrator de dados de vídeos do Twitch"""
    
    def __init__(self):
        """Inicializa o extrator"""
        self.client = TwitchAPIClient()
        self.extracted_data = []
        self.data_dir = self._ensure_data_dir()
    
    def _ensure_data_dir(self) -> str:
        """Garante que o diretório de dados existe"""
        data_dir = os.path.join(ETLConfig.BASE_DIR, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            info("📁 Diretório de dados criado: {}", data_dir)
        return data_dir
    
    def extract_videos_from_users(self, user_ids: List[str], limit: int = 50) -> List[Dict]:
        """
        Extrai vídeos de usuários específicos
        
        Args:
            user_ids: Lista de IDs de usuários
            limit: Número máximo de vídeos por usuário
            
        Returns:
            Lista de dados de vídeos
        """
        info("📺 Extraindo vídeos de {} usuários...", len(user_ids))
        
        try:
            all_videos = []
            
            for user_id in user_ids:
                info("🎬 Buscando vídeos do usuário ID: {}", user_id)
                
                videos = self.client.get_videos(user_ids=[user_id], limit=limit)
                
                if videos:
                    all_videos.extend(videos)
                    info("✅ {} vídeos encontrados para usuário {}", len(videos), user_id)
                    
                    # Log dos vídeos mais populares deste usuário
                    top_videos = sorted(videos, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                    for i, video in enumerate(top_videos, 1):
                        info("  {}º mais visto: {} views - {}", 
                             i, video.get('view_count', 0), 
                             video.get('title', 'Sem título')[:50])
                else:
                    info("ℹ️ Nenhum vídeo encontrado para usuário {}", user_id)
            
            self.extracted_data.extend(all_videos)
            info("✅ {} vídeos extraídos no total", len(all_videos))
            return all_videos
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos de usuários: {}", e)
            return []
    
    def extract_videos_from_popular_streamers(self, limit_streamers: int = 20, limit_videos: int = 30) -> List[Dict]:
        """
        Extrai vídeos dos streamers mais populares
        
        Args:
            limit_streamers: Número de streamers para buscar
            limit_videos: Número de vídeos por streamer
            
        Returns:
            Lista de dados de vídeos
        """
        info("🌟 Extraindo vídeos dos {} streamers mais populares...", limit_streamers)
        
        try:
            # Primeiro buscar streams populares para obter user_ids
            streams = self.client.get_streams(limit=limit_streamers * 2)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            # Extrair user_ids únicos e ordenar por viewer_count
            user_streams = {}
            for stream in streams:
                user_id = stream['user_id']
                if user_id not in user_streams:
                    user_streams[user_id] = stream
            
            # Pegar os mais populares
            popular_users = sorted(
                user_streams.values(), 
                key=lambda x: x.get('viewer_count', 0), 
                reverse=True
            )[:limit_streamers]
            
            user_ids = [user['user_id'] for user in popular_users]
            
            info("👥 Encontrados {} streamers populares", len(user_ids))
            for user in popular_users[:5]:
                info("📺 {}: {} viewers", 
                     user.get('user_name', 'Desconhecido'), 
                     user.get('viewer_count', 0))
            
            # Buscar vídeos destes usuários
            return self.extract_videos_from_users(user_ids, limit_videos)
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos de streamers populares: {}", e)
            return []
    
    def extract_videos_by_usernames(self, usernames: List[str], limit: int = 50) -> List[Dict]:
        """
        Extrai vídeos por usernames específicos
        
        Args:
            usernames: Lista de usernames
            limit: Número máximo de vídeos por usuário
            
        Returns:
            Lista de dados de vídeos
        """
        info("🎯 Extraindo vídeos de usuários específicos: {}", usernames)
        
        try:
            # Primeiro obter user_ids a partir dos usernames
            users = self.client.get_users(logins=usernames)
            
            if not users:
                error("⚠️ Nenhum usuário encontrado")
                return []
            
            user_ids = [user['id'] for user in users]
            info("👤 {} usuários encontrados: {}", 
                 len(user_ids), 
                 [user['display_name'] for user in users])
            
            # Buscar vídeos destes usuários
            return self.extract_videos_from_users(user_ids, limit)
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos por username: {}", e)
            return []

    # === NOVOS MÉTODOS COM PAGINAÇÃO ===
    
    def extract_videos_from_users_with_pagination(self, user_ids: List[str], limit: int = 100, 
                                                 max_pages: int = 5) -> List[Dict]:
        """
        Extrai vídeos de usuários específicos COM PAGINAÇÃO
        
        Args:
            user_ids: Lista de IDs de usuários
            limit: Número máximo de vídeos por usuário
            max_pages: Máximo de páginas por usuário
            
        Returns:
            Lista de dados de vídeos
        """
        info("🎬 Extraindo vídeos de {} usuários COM PAGINAÇÃO...", len(user_ids))
        
        try:
            videos = self.client.get_videos(
                user_ids, 
                limit=limit, 
                use_pagination=True, 
                max_pages_per_user=max_pages
            )
            
            if videos:
                self.extracted_data.extend(videos)
                info("✅ {} vídeos extraídos com paginação", len(videos))
                
                # Estatísticas por usuário
                user_stats = {}
                for video in videos:
                    user_id = video.get('user_id', 'unknown')
                    user_stats[user_id] = user_stats.get(user_id, 0) + 1
                
                info("📊 Vídeos por usuário (top 5):")
                for user_id, count in sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
                    info("👤 {}: {} vídeos", user_id, count)
            
            return videos
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos com paginação: {}", e)
            return []
    
    def extract_videos_from_popular_streamers_with_pagination(self, limit_streamers: int = 50, 
                                                            limit_videos: int = 100, max_pages: int = 5) -> List[Dict]:
        """
        Extrai vídeos de streamers populares COM PAGINAÇÃO
        
        Args:
            limit_streamers: Número de streamers populares
            limit_videos: Número máximo de vídeos por streamer
            max_pages: Máximo de páginas por streamer
            
        Returns:
            Lista de dados de vídeos
        """
        info("📺 Extraindo vídeos de {} streamers populares COM PAGINAÇÃO...", limit_streamers)
        
        try:
            # Buscar streams atuais para encontrar streamers populares
            streams = self.client.get_streams(limit=limit_streamers * 3, use_pagination=True, max_pages=10)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            # Agrupar por usuário e pegar o de maior viewer count
            user_streams = {}
            for stream in streams:
                user_id = stream.get('user_id')
                if user_id:
                    current = user_streams.get(user_id, {'viewer_count': 0})
                    if stream.get('viewer_count', 0) > current.get('viewer_count', 0):
                        user_streams[user_id] = stream
            
            # Pegar os mais populares
            popular_users = sorted(
                user_streams.values(), 
                key=lambda x: x.get('viewer_count', 0), 
                reverse=True
            )[:limit_streamers]
            
            user_ids = [user['user_id'] for user in popular_users]
            
            info("👥 Encontrados {} streamers populares para extração de vídeos", len(user_ids))
            for user in popular_users[:10]:
                info("📺 {}: {} viewers", 
                     user.get('user_name', 'Desconhecido'), 
                     user.get('viewer_count', 0))
            
            # Buscar vídeos destes usuários COM PAGINAÇÃO
            return self.extract_videos_from_users_with_pagination(user_ids, limit_videos, max_pages)
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos de streamers populares com paginação: {}", e)
            return []
    
    def extract_videos_by_usernames_with_pagination(self, usernames: List[str], limit: int = 80, 
                                                   max_pages: int = 5) -> List[Dict]:
        """
        Extrai vídeos por usernames específicos COM PAGINAÇÃO
        
        Args:
            usernames: Lista de usernames
            limit: Número máximo de vídeos por usuário
            max_pages: Máximo de páginas por usuário
            
        Returns:
            Lista de dados de vídeos
        """
        info("🎯 Extraindo vídeos de {} usuários específicos COM PAGINAÇÃO...", len(usernames))
        
        try:
            # Primeiro obter user_ids a partir dos usernames
            users = self.client.get_users(logins=usernames)
            
            if not users:
                error("⚠️ Nenhum usuário encontrado")
                return []
            
            user_ids = [user['id'] for user in users]
            info("👤 {} usuários encontrados: {}", 
                 len(user_ids), 
                 [user['display_name'] for user in users])
            
            # Buscar vídeos destes usuários COM PAGINAÇÃO
            return self.extract_videos_from_users_with_pagination(user_ids, limit, max_pages)
            
        except Exception as e:
            error("💥 Erro ao extrair vídeos por username com paginação: {}", e)
            return []
    
    def save_to_file(self, filename: Optional[str] = None) -> str:
        """
        Salva dados extraídos em arquivo JSON
        
        Args:
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho do arquivo salvo
        """
        if not self.extracted_data:
            error("⚠️ Nenhum dado para salvar")
            return ""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"videos_extracted_{timestamp}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # Remover duplicatas por ID
            unique_videos = {}
            for video in self.extracted_data:
                unique_videos[video['id']] = video
            
            final_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'total_videos': len(unique_videos),
                'videos': list(unique_videos.values())
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Dados salvos em: {}", filepath)
            info("📊 Total de vídeos únicos: {}", len(unique_videos))
            return filepath
            
        except Exception as e:
            error("💥 Erro ao salvar arquivo: {}", e)
            return ""
    
    def get_extraction_summary(self) -> Dict:
        """
        Retorna resumo da extração
        
        Returns:
            Dicionário com estatísticas
        """
        if not self.extracted_data:
            return {'total': 0, 'unique': 0}
        
        unique_ids = set(video['id'] for video in self.extracted_data)
        total_views = sum(video.get('view_count', 0) for video in self.extracted_data)
        
        # Estatísticas por tipo
        video_types = {}
        for video in self.extracted_data:
            vtype = video.get('type', 'unknown')
            video_types[vtype] = video_types.get(vtype, 0) + 1
        
        # Estatísticas por linguagem
        languages = {}
        for video in self.extracted_data:
            lang = video.get('language', 'unknown')
            languages[lang] = languages.get(lang, 0) + 1
        
        # Top vídeos por view count
        top_videos = sorted(
            self.extracted_data, 
            key=lambda x: x.get('view_count', 0), 
            reverse=True
        )[:5]
        
        # Estatísticas por usuário
        users = {}
        for video in self.extracted_data:
            user_id = video.get('user_id', 'unknown')
            users[user_id] = users.get(user_id, 0) + 1
        
        return {
            'total_extracted': len(self.extracted_data),
            'unique_videos': len(unique_ids),
            'total_views': total_views,
            'avg_views': total_views // len(self.extracted_data) if self.extracted_data else 0,
            'video_types': video_types,
            'languages': languages,
            'unique_users': len(users),
            'videos_per_user': total_views // len(users) if users else 0,
            'top_videos': [
                f"{v.get('title', 'N/A')[:30]}... ({v.get('view_count', 0):,} views)"
                for v in top_videos
            ]
        }

def main():
    """Função principal de extração"""
    info("🚀 === INICIANDO EXTRAÇÃO DE VÍDEOS ===")
    
    extractor = VideoExtractor()
    
    try:
        # 1. Extrair vídeos de streamers populares COM PAGINAÇÃO
        info("📝 Etapa 1: Vídeos de streamers populares (COM PAGINAÇÃO)")
        popular_videos = extractor.extract_videos_from_popular_streamers_with_pagination(
            limit_streamers=50,  # Aumentado de 15 para 50
            limit_videos=100,    # Aumentado de 40 para 100
            max_pages=5          # Páginas por usuário
        )
        
        # 2. Extrair vídeos de usuários específicos conhecidos COM PAGINAÇÃO
        info("📝 Etapa 2: Vídeos de usuários específicos (COM PAGINAÇÃO)")
        known_users = [
            'ninja', 'pokimane', 'shroud', 'xqc', 'summit1g',
            'sodapoppin', 'lirik', 'timthetatman', 'asmongold', 'hasanabi',
            'tfue', 'rubius', 'ibai', 'gaules', 'loud_coringa',
            'cellbit', 'casimito', 'elspreen', 'thegrefg', 'auronplay'
        ]
        specific_videos = extractor.extract_videos_by_usernames_with_pagination(
            known_users, 
            limit=80,      # Aumentado de 30 para 80
            max_pages=5    # Páginas por usuário
        )
        
        # 3. Salvar dados
        info("📝 Etapa 3: Salvando dados")
        filepath = extractor.save_to_file()
        
        # 4. Resumo final
        summary = extractor.get_extraction_summary()
        info("📊 === RESUMO DA EXTRAÇÃO ===")
        info("Total extraído: {}", summary['total_extracted'])
        info("Vídeos únicos: {}", summary['unique_videos'])
        info("Total de views: {:,}", summary['total_views'])
        info("Média de views: {:,}", summary['avg_views'])
        info("Tipos de vídeo: {}", summary['video_types'])
        info("Usuários únicos: {}", summary['unique_users'])
        info("Top vídeos: {}", summary['top_videos'][:2])
        
        if filepath:
            info("✅ Extração de vídeos concluída com sucesso!")
            info("📁 Arquivo salvo: {}", filepath)
        else:
            error("❌ Falha ao salvar dados")
            
    except Exception as e:
        error("💥 Erro na extração: {}", e)

if __name__ == "__main__":
    main() 