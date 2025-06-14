"""
Script de Extração de Clips - Twitch API
Busca clips populares por jogos e streamers
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

class ClipExtractor:
    """Extrator de dados de clips do Twitch"""
    
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
    
    def extract_clips_by_game(self, game_ids: List[str], limit: int = 100) -> List[Dict]:
        """
        Extrai clips por jogos específicos
        
        Args:
            game_ids: Lista de IDs de jogos
            limit: Número máximo de clips por jogo
            
        Returns:
            Lista de dados de clips
        """
        info("🎮 Extraindo clips de {} jogos...", len(game_ids))
        
        try:
            all_clips = []
            
            for game_id in game_ids:
                info("🎬 Buscando clips do jogo ID: {}", game_id)
                
                clips = self.client.get_clips(game_ids=[game_id], limit=limit)
                
                if clips:
                    all_clips.extend(clips)
                    info("✅ {} clips encontrados para jogo {}", len(clips), game_id)
                    
                    # Log dos clips mais populares deste jogo
                    top_clips = sorted(clips, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                    for i, clip in enumerate(top_clips, 1):
                        info("  {}º mais visto: {} views - {}", 
                             i, clip.get('view_count', 0), 
                             clip.get('title', 'Sem título')[:40])
                else:
                    info("ℹ️ Nenhum clip encontrado para jogo {}", game_id)
            
            self.extracted_data.extend(all_clips)
            info("✅ {} clips por jogo extraídos no total", len(all_clips))
            return all_clips
            
        except Exception as e:
            error("💥 Erro ao extrair clips por jogo: {}", e)
            return []
    
    def extract_clips_by_broadcaster(self, broadcaster_ids: List[str], limit: int = 50) -> List[Dict]:
        """
        Extrai clips de streamers específicos
        
        Args:
            broadcaster_ids: Lista de IDs de streamers
            limit: Número máximo de clips por streamer
            
        Returns:
            Lista de dados de clips
        """
        info("📺 Extraindo clips de {} streamers...", len(broadcaster_ids))
        
        try:
            all_clips = []
            
            for broadcaster_id in broadcaster_ids:
                info("🎭 Buscando clips do streamer ID: {}", broadcaster_id)
                
                clips = self.client.get_clips(broadcaster_ids=[broadcaster_id], limit=limit)
                
                if clips:
                    all_clips.extend(clips)
                    info("✅ {} clips encontrados para streamer {}", len(clips), broadcaster_id)
                    
                    # Log dos clips mais populares deste streamer
                    top_clips = sorted(clips, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
                    for i, clip in enumerate(top_clips, 1):
                        info("  {}º mais visto: {} views - {}", 
                             i, clip.get('view_count', 0), 
                             clip.get('title', 'Sem título')[:40])
                else:
                    info("ℹ️ Nenhum clip encontrado para streamer {}", broadcaster_id)
            
            self.extracted_data.extend(all_clips)
            info("✅ {} clips por streamer extraídos no total", len(all_clips))
            return all_clips
            
        except Exception as e:
            error("💥 Erro ao extrair clips por streamer: {}", e)
            return []
    
    def extract_clips_from_popular_games(self, limit_games: int = 10, limit_clips: int = 50) -> List[Dict]:
        """
        Extrai clips dos jogos mais populares
        
        Args:
            limit_games: Número de jogos populares para buscar
            limit_clips: Número de clips por jogo
            
        Returns:
            Lista de dados de clips
        """
        info("🏆 Extraindo clips dos {} jogos mais populares...", limit_games)
        
        try:
            # Buscar jogos populares
            games = self.client.get_top_games(limit=limit_games)
            
            if not games:
                error("⚠️ Nenhum jogo popular encontrado")
                return []
            
            game_ids = [game['id'] for game in games]
            
            info("🎮 Jogos populares encontrados:")
            for game in games[:5]:
                info("  - {} (ID: {})", game['name'], game['id'])
            
            # Buscar clips destes jogos
            return self.extract_clips_by_game(game_ids, limit_clips)
            
        except Exception as e:
            error("💥 Erro ao extrair clips de jogos populares: {}", e)
            return []
    
    def extract_clips_from_popular_streamers(self, limit_streamers: int = 15, limit_clips: int = 40) -> List[Dict]:
        """
        Extrai clips dos streamers mais populares
        
        Args:
            limit_streamers: Número de streamers para buscar
            limit_clips: Número de clips por streamer
            
        Returns:
            Lista de dados de clips
        """
        info("🌟 Extraindo clips dos {} streamers mais populares...", limit_streamers)
        
        try:
            # Buscar streams populares para obter broadcaster_ids
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
            
            broadcaster_ids = [user['user_id'] for user in popular_users]
            
            info("👥 Streamers populares encontrados:")
            for user in popular_users[:5]:
                info("  - {} ({} viewers)", 
                     user.get('user_name', 'Desconhecido'), 
                     user.get('viewer_count', 0))
            
            # Buscar clips destes streamers
            return self.extract_clips_by_broadcaster(broadcaster_ids, limit_clips)
            
        except Exception as e:
            error("💥 Erro ao extrair clips de streamers populares: {}", e)
            return []
    
    def extract_clips_by_usernames(self, usernames: List[str], limit: int = 50) -> List[Dict]:
        """
        Extrai clips por usernames específicos
        
        Args:
            usernames: Lista de usernames
            limit: Número máximo de clips por usuário
            
        Returns:
            Lista de dados de clips
        """
        info("🎯 Extraindo clips de usuários específicos: {}", usernames)
        
        try:
            # Primeiro obter user_ids a partir dos usernames
            users = self.client.get_users(logins=usernames)
            
            if not users:
                error("⚠️ Nenhum usuário encontrado")
                return []
            
            broadcaster_ids = [user['id'] for user in users]
            info("👤 {} usuários encontrados: {}", 
                 len(broadcaster_ids), 
                 [user['display_name'] for user in users])
            
            # Buscar clips destes usuários
            return self.extract_clips_by_broadcaster(broadcaster_ids, limit)
            
        except Exception as e:
            error("💥 Erro ao extrair clips por username: {}", e)
            return []

    # === NOVOS MÉTODOS COM PAGINAÇÃO ===
    
    def extract_clips_by_game_with_pagination(self, game_ids: List[str], limit: int = 200, 
                                            max_pages: int = 10) -> List[Dict]:
        """
        Extrai clips de jogos específicos COM PAGINAÇÃO
        
        Args:
            game_ids: Lista de IDs de jogos
            limit: Número máximo de clips por jogo
            max_pages: Número máximo de páginas por jogo
            
        Returns:
            Lista de dados de clips
        """
        info("🎮 Extraindo clips de {} jogos COM PAGINAÇÃO...", len(game_ids))
        
        try:
            clips = self.client.get_clips(
                game_ids=game_ids,
                limit=limit,
                use_pagination=True,
                max_pages_per_item=max_pages
            )
            
            if clips:
                self.extracted_data.extend(clips)
                info("✅ {} clips extraídos de jogos com paginação", len(clips))
                
                # Estatísticas por jogo
                game_stats = {}
                for clip in clips:
                    game_id = clip.get('game_id', 'unknown')
                    game_stats[game_id] = game_stats.get(game_id, 0) + 1
                
                info("📊 Clips por jogo (top 5):")
                for game_id, count in sorted(game_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
                    info("🎮 {}: {} clips", game_id, count)
            
            return clips
            
        except Exception as e:
            error("💥 Erro ao extrair clips de jogos com paginação: {}", e)
            return []
    
    def extract_clips_by_broadcaster_with_pagination(self, broadcaster_ids: List[str], limit: int = 150, 
                                                   max_pages: int = 8) -> List[Dict]:
        """
        Extrai clips de broadcasters específicos COM PAGINAÇÃO
        
        Args:
            broadcaster_ids: Lista de IDs de broadcasters
            limit: Número máximo de clips por broadcaster
            max_pages: Número máximo de páginas por broadcaster
            
        Returns:
            Lista de dados de clips
        """
        info("👤 Extraindo clips de {} broadcasters COM PAGINAÇÃO...", len(broadcaster_ids))
        
        try:
            clips = self.client.get_clips(
                broadcaster_ids=broadcaster_ids,
                limit=limit,
                use_pagination=True,
                max_pages_per_item=max_pages
            )
            
            if clips:
                self.extracted_data.extend(clips)
                info("✅ {} clips extraídos de broadcasters com paginação", len(clips))
                
                # Estatísticas por broadcaster
                broadcaster_stats = {}
                for clip in clips:
                    broadcaster_id = clip.get('broadcaster_id', 'unknown')
                    broadcaster_stats[broadcaster_id] = broadcaster_stats.get(broadcaster_id, 0) + 1
                
                info("📊 Clips por broadcaster (top 5):")
                for broadcaster_id, count in sorted(broadcaster_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
                    info("👤 {}: {} clips", broadcaster_id, count)
            
            return clips
            
        except Exception as e:
            error("💥 Erro ao extrair clips de broadcasters com paginação: {}", e)
            return []
    
    def extract_clips_from_popular_games_with_pagination(self, limit_games: int = 20, 
                                                       limit_clips: int = 200, max_pages: int = 10) -> List[Dict]:
        """
        Extrai clips dos jogos mais populares COM PAGINAÇÃO
        
        Args:
            limit_games: Número de jogos populares
            limit_clips: Número máximo de clips por jogo
            max_pages: Número máximo de páginas por jogo
            
        Returns:
            Lista de dados de clips
        """
        info("🏆 Extraindo clips de {} jogos populares COM PAGINAÇÃO...", limit_games)
        
        try:
            # Buscar jogos mais populares
            top_games = self.client.get_top_games(limit=limit_games)
            
            if not top_games:
                error("⚠️ Nenhum jogo popular encontrado")
                return []
            
            game_ids = [game['id'] for game in top_games]
            
            info("🎮 Jogos selecionados:")
            for game in top_games[:10]:
                info("📊 {}", game.get('name', 'Desconhecido'))
            
            # Buscar clips destes jogos COM PAGINAÇÃO
            return self.extract_clips_by_game_with_pagination(game_ids, limit_clips, max_pages)
            
        except Exception as e:
            error("💥 Erro ao extrair clips de jogos populares com paginação: {}", e)
            return []
    
    def extract_clips_from_popular_streamers_with_pagination(self, limit_streamers: int = 30, 
                                                           limit_clips: int = 150, max_pages: int = 8) -> List[Dict]:
        """
        Extrai clips de streamers populares COM PAGINAÇÃO
        
        Args:
            limit_streamers: Número de streamers populares
            limit_clips: Número máximo de clips por streamer
            max_pages: Número máximo de páginas por streamer
            
        Returns:
            Lista de dados de clips
        """
        info("📺 Extraindo clips de {} streamers populares COM PAGINAÇÃO...", limit_streamers)
        
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
            
            broadcaster_ids = [user['user_id'] for user in popular_users]
            
            info("👥 Encontrados {} streamers populares para extração de clips", len(broadcaster_ids))
            for user in popular_users[:10]:
                info("📺 {}: {} viewers", 
                     user.get('user_name', 'Desconhecido'), 
                     user.get('viewer_count', 0))
            
            # Buscar clips destes streamers COM PAGINAÇÃO
            return self.extract_clips_by_broadcaster_with_pagination(broadcaster_ids, limit_clips, max_pages)
            
        except Exception as e:
            error("💥 Erro ao extrair clips de streamers populares com paginação: {}", e)
            return []
    
    def extract_clips_by_usernames_with_pagination(self, usernames: List[str], limit: int = 100, 
                                                 max_pages: int = 6) -> List[Dict]:
        """
        Extrai clips por usernames específicos COM PAGINAÇÃO
        
        Args:
            usernames: Lista de usernames
            limit: Número máximo de clips por usuário
            max_pages: Número máximo de páginas por usuário
            
        Returns:
            Lista de dados de clips
        """
        info("🎯 Extraindo clips de {} usuários específicos COM PAGINAÇÃO...", len(usernames))
        
        try:
            # Primeiro obter user_ids a partir dos usernames
            users = self.client.get_users(logins=usernames)
            
            if not users:
                error("⚠️ Nenhum usuário encontrado")
                return []
            
            broadcaster_ids = [user['id'] for user in users]
            info("👤 {} usuários encontrados: {}", 
                 len(broadcaster_ids), 
                 [user['display_name'] for user in users])
            
            # Buscar clips destes usuários COM PAGINAÇÃO
            return self.extract_clips_by_broadcaster_with_pagination(broadcaster_ids, limit, max_pages)
            
        except Exception as e:
            error("💥 Erro ao extrair clips por username com paginação: {}", e)
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
            filename = f"clips_extracted_{timestamp}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # Remover duplicatas por ID
            unique_clips = {}
            for clip in self.extracted_data:
                unique_clips[clip['id']] = clip
            
            final_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'total_clips': len(unique_clips),
                'clips': list(unique_clips.values())
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Dados salvos em: {}", filepath)
            info("📊 Total de clips únicos: {}", len(unique_clips))
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
        
        unique_ids = set(clip['id'] for clip in self.extracted_data)
        total_views = sum(clip.get('view_count', 0) for clip in self.extracted_data)
        
        # Estatísticas por jogo
        games = {}
        for clip in self.extracted_data:
            game_id = clip.get('game_id', 'unknown')
            games[game_id] = games.get(game_id, 0) + 1
        
        # Estatísticas por broadcaster
        broadcasters = {}
        for clip in self.extracted_data:
            broadcaster_id = clip.get('broadcaster_id', 'unknown')
            broadcasters[broadcaster_id] = broadcasters.get(broadcaster_id, 0) + 1
        
        # Top clips por view count
        top_clips = sorted(
            self.extracted_data, 
            key=lambda x: x.get('view_count', 0), 
            reverse=True
        )[:5]
        
        # Clips por data de criação (últimos 7 dias)
        recent_clips = 0
        week_ago = datetime.now() - timedelta(days=7)
        for clip in self.extracted_data:
            created_at = clip.get('created_at', '')
            if created_at:
                try:
                    clip_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    if clip_date.replace(tzinfo=None) >= week_ago:
                        recent_clips += 1
                except:
                    pass
        
        return {
            'total_extracted': len(self.extracted_data),
            'unique_clips': len(unique_ids),
            'total_views': total_views,
            'avg_views': total_views // len(self.extracted_data) if self.extracted_data else 0,
            'unique_games': len(games),
            'unique_broadcasters': len(broadcasters),
            'recent_clips': recent_clips,
            'top_clips': [
                f"{c.get('title', 'N/A')[:25]}... ({c.get('view_count', 0):,} views)"
                for c in top_clips
            ]
        }

def main():
    """Função principal de extração"""
    info("🚀 === INICIANDO EXTRAÇÃO DE CLIPS ===")
    
    extractor = ClipExtractor()
    
    try:
        # 1. Extrair clips dos jogos mais populares COM PAGINAÇÃO
        info("📝 Etapa 1: Clips de jogos populares (COM PAGINAÇÃO)")
        game_clips = extractor.extract_clips_from_popular_games_with_pagination(
            limit_games=20,   # Aumentado de 8 para 20
            limit_clips=200,  # Aumentado de 40 para 200
            max_pages=10      # Páginas por jogo
        )
        
        # 2. Extrair clips de streamers populares COM PAGINAÇÃO
        info("📝 Etapa 2: Clips de streamers populares (COM PAGINAÇÃO)")
        streamer_clips = extractor.extract_clips_from_popular_streamers_with_pagination(
            limit_streamers=30,  # Aumentado de 12 para 30
            limit_clips=150,     # Aumentado de 30 para 150
            max_pages=8          # Páginas por streamer
        )
        
        # 3. Extrair clips de usuários específicos conhecidos COM PAGINAÇÃO
        info("📝 Etapa 3: Clips de usuários específicos (COM PAGINAÇÃO)")
        known_users = [
            'ninja', 'pokimane', 'shroud', 'xqc', 'summit1g',
            'sodapoppin', 'lirik', 'timthetatman', 'asmongold', 'hasanabi',
            'tfue', 'rubius', 'ibai', 'gaules', 'loud_coringa',
            'cellbit', 'casimito', 'elspreen', 'thegrefg', 'auronplay',
            'mizkif', 'ludwig', 'myth', 'disguisedtoast', 'sykkuno'
        ]
        specific_clips = extractor.extract_clips_by_usernames_with_pagination(
            known_users, 
            limit=100,     # Aumentado de 25 para 100
            max_pages=6    # Páginas por usuário
        )
        
        # 4. Salvar dados
        info("📝 Etapa 4: Salvando dados")
        filepath = extractor.save_to_file()
        
        # 5. Resumo final
        summary = extractor.get_extraction_summary()
        info("📊 === RESUMO DA EXTRAÇÃO ===")
        info("Total extraído: {}", summary['total_extracted'])
        info("Clips únicos: {}", summary['unique_clips'])
        info("Total de views: {:,}", summary['total_views'])
        info("Média de views: {:,}", summary['avg_views'])
        info("Jogos únicos: {}", summary['unique_games'])
        info("Broadcasters únicos: {}", summary['unique_broadcasters'])
        info("Clips recentes (7 dias): {}", summary['recent_clips'])
        info("Top clips: {}", summary['top_clips'][:2])
        
        if filepath:
            info("✅ Extração de clips concluída com sucesso!")
            info("📁 Arquivo salvo: {}", filepath)
        else:
            error("❌ Falha ao salvar dados")
            
    except Exception as e:
        error("💥 Erro na extração: {}", e)

if __name__ == "__main__":
    main() 