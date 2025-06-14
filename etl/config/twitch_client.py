"""
Cliente para API Twitch
Classe responsável por todas as interações com a API
"""

import sys
import os
import requests
import time
from typing import Dict, List, Optional, Any

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

from settings import TwitchAPIConfig, ETLConfig

class TwitchAPIClient:
    """Cliente para interagir com a API Twitch"""
    
    def __init__(self):
        """Inicializa o cliente da API Twitch"""
        self.config = TwitchAPIConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.get_headers())
        
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Faz uma requisição para a API com retry automático
        
        Args:
            url: URL do endpoint
            params: Parâmetros da query string
            
        Returns:
            Dados da resposta ou None em caso de erro
        """
        for attempt in range(ETLConfig.MAX_RETRIES):
            try:
                info("Fazendo requisição para: {}", url)
                if params:
                    info("Parâmetros: {}", params)
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=ETLConfig.API_TIMEOUT
                )
                
                if response.status_code == 200:
                    data = response.json()
                    info("✅ Requisição bem-sucedida - {} registros retornados", 
                         len(data.get('data', [])))
                    return data
                
                elif response.status_code == 429:  # Rate limit
                    error("⚠️ Rate limit atingido. Aguardando...")
                    time.sleep(ETLConfig.RETRY_DELAY * 2)
                    continue
                    
                else:
                    error("❌ Erro na API - Status: {} - Resposta: {}", 
                          response.status_code, response.text)
                    
            except requests.exceptions.Timeout:
                error("⏰ Timeout na requisição (tentativa {}/{})", 
                      attempt + 1, ETLConfig.MAX_RETRIES)
            except requests.exceptions.RequestException as e:
                error("💥 Erro de conexão (tentativa {}/{}): {}", 
                      attempt + 1, ETLConfig.MAX_RETRIES, e)
            
            if attempt < ETLConfig.MAX_RETRIES - 1:
                info("Aguardando {} segundos antes da próxima tentativa...", 
                     ETLConfig.RETRY_DELAY)
                time.sleep(ETLConfig.RETRY_DELAY)
        
        error("❌ Falha após {} tentativas", ETLConfig.MAX_RETRIES)
        return None
    
    def validate_token(self) -> bool:
        """
        Valida se o token de acesso está válido
        
        Returns:
            True se válido, False caso contrário
        """
        info("🔐 Validando token de acesso...")
        
        headers = {'Authorization': f'OAuth {self.config.ACCESS_TOKEN}'}
        
        try:
            response = requests.get(
                self.config.ENDPOINTS['validate'],
                headers=headers,
                timeout=ETLConfig.API_TIMEOUT
            )
            
            if response.status_code == 200:
                token_info = response.json()
                expires_in = token_info.get('expires_in', 0)
                info("✅ Token válido - Expira em: {} segundos", expires_in)
                return True
            else:
                error("❌ Token inválido - Status: {}", response.status_code)
                return False
                
        except Exception as e:
            error("💥 Erro ao validar token: {}", e)
            return False
    
    def get_users(self, logins: List[str] = None, ids: List[str] = None, 
                  limit: int = None) -> List[Dict]:
        """
        Busca informações de usuários
        
        Args:
            logins: Lista de logins de usuário
            ids: Lista de IDs de usuário  
            limit: Limite de resultados
            
        Returns:
            Lista de usuários
        """
        info("👤 Buscando dados de usuários...")
        
        params = {}
        if logins:
            params['login'] = logins[:100]  # API limita a 100
        if ids:
            params['id'] = ids[:100]
        
        if not logins and not ids:
            # Se não especificar, buscar alguns usuários populares
            params['login'] = ['twitchdev', 'ninja', 'pokimane']
        
        data = self._make_request(self.config.ENDPOINTS['users'], params)
        
        if data and 'data' in data:
            users = data['data']
            if limit:
                users = users[:limit]
            info("📊 {} usuários encontrados", len(users))
            return users
        
        return []
    
    def get_games(self, ids: List[str] = None, names: List[str] = None) -> List[Dict]:
        """
        Busca informações de jogos específicos
        
        Args:
            ids: Lista de IDs de jogos
            names: Lista de nomes de jogos
            
        Returns:
            Lista de jogos
        """
        info("🎮 Buscando dados de jogos...")
        
        params = {}
        if ids:
            params['id'] = ids[:100]
        if names:
            params['name'] = names[:100]
        
        data = self._make_request(self.config.ENDPOINTS['games'], params)
        
        if data and 'data' in data:
            games = data['data']
            info("📊 {} jogos encontrados", len(games))
            return games
        
        return []
    
    def get_top_games(self, limit: int = 50) -> List[Dict]:
        """
        Busca jogos mais populares
        
        Args:
            limit: Número de jogos para retornar
            
        Returns:
            Lista de jogos populares
        """
        info("🏆 Buscando jogos mais populares...")
        
        params = {'first': min(limit, self.config.MAX_RESULTS_PER_PAGE)}
        
        data = self._make_request(self.config.ENDPOINTS['games_top'], params)
        
        if data and 'data' in data:
            games = data['data']
            info("📊 {} jogos populares encontrados", len(games))
            return games
        
        return []
    
    def get_streams(self, user_ids: List[str] = None, game_ids: List[str] = None,
                   limit: int = 100, use_pagination: bool = True, max_pages: int = 10) -> List[Dict]:
        """
        Busca streams ao vivo com paginação
        
        Args:
            user_ids: Lista de IDs de usuários
            game_ids: Lista de IDs de jogos
            limit: Número total de streams para retornar
            use_pagination: Se deve usar paginação para obter mais dados
            max_pages: Número máximo de páginas para buscar
            
        Returns:
            Lista de streams
        """
        info("📺 Buscando streams ao vivo (paginação: {})...", "SIM" if use_pagination else "NÃO")
        
        all_streams = []
        cursor = None
        page = 0
        per_page = min(self.config.MAX_RESULTS_PER_PAGE, 100)
        
        while len(all_streams) < limit and page < max_pages:
            params = {'first': per_page}
            
            if cursor:
                params['after'] = cursor
            if user_ids:
                params['user_id'] = user_ids[:100]
            if game_ids:
                params['game_id'] = game_ids[:10]
            
            data = self._make_request(self.config.ENDPOINTS['streams'], params)
            
            if not data or 'data' not in data:
                break
                
            streams = data['data']
            if not streams:
                break
                
            all_streams.extend(streams)
            page += 1
            
            info("📄 Página {}: {} streams (+{} total)", page, len(streams), len(all_streams))
            
            # Verificar se há próxima página
            if not use_pagination or not data.get('pagination', {}).get('cursor'):
                break
                
            cursor = data['pagination']['cursor']
            
            # Se não precisamos de mais dados, parar
            if len(all_streams) >= limit:
                break
        
        # Limitar ao número solicitado
        final_streams = all_streams[:limit]
        info("📊 {} streams ao vivo encontradas ({} páginas)", len(final_streams), page)
        return final_streams
    
    def get_videos(self, user_ids: List[str], limit: int = 50, use_pagination: bool = True, 
                  max_pages_per_user: int = 5) -> List[Dict]:
        """
        Busca vídeos de usuários com paginação
        
        Args:
            user_ids: Lista de IDs de usuários
            limit: Número de vídeos por usuário
            use_pagination: Se deve usar paginação
            max_pages_per_user: Máximo de páginas por usuário
            
        Returns:
            Lista de vídeos
        """
        info("🎬 Buscando vídeos (paginação: {})...", "SIM" if use_pagination else "NÃO")
        
        all_videos = []
        
        for user_id in user_ids[:20]:  # Aumentado de 10 para 20 usuários
            info("👤 Buscando vídeos do usuário: {}", user_id)
            user_videos = []
            cursor = None
            page = 0
            per_page = min(limit, self.config.MAX_RESULTS_PER_PAGE)
            
            while len(user_videos) < limit and page < max_pages_per_user:
                params = {
                    'user_id': user_id,
                    'first': per_page
                }
                
                if cursor:
                    params['after'] = cursor
                
                data = self._make_request(self.config.ENDPOINTS['videos'], params)
                
                if not data or 'data' not in data:
                    break
                    
                videos = data['data']
                if not videos:
                    break
                    
                user_videos.extend(videos)
                page += 1
                
                info("📄 Usuário {} - Página {}: {} vídeos", user_id, page, len(videos))
                
                # Verificar se há próxima página
                if not use_pagination or not data.get('pagination', {}).get('cursor'):
                    break
                    
                cursor = data['pagination']['cursor']
                
                # Se já temos vídeos suficientes para este usuário
                if len(user_videos) >= limit:
                    break
            
            # Limitar vídeos por usuário
            final_user_videos = user_videos[:limit]
            all_videos.extend(final_user_videos)
            info("✅ {} vídeos encontrados para usuário {} ({} páginas)", 
                 len(final_user_videos), user_id, page)
        
        info("📊 Total: {} vídeos encontrados de {} usuários", len(all_videos), len(user_ids[:20]))
        return all_videos
    
    def get_clips(self, broadcaster_ids: List[str] = None, game_ids: List[str] = None,
                 limit: int = 50, use_pagination: bool = True, max_pages_per_item: int = 10) -> List[Dict]:
        """
        Busca clips com paginação
        
        Args:
            broadcaster_ids: Lista de IDs de streamers
            game_ids: Lista de IDs de jogos
            limit: Número de clips por item (jogo/streamer)
            use_pagination: Se deve usar paginação
            max_pages_per_item: Máximo de páginas por item
            
        Returns:
            Lista de clips
        """
        info("🎥 Buscando clips (paginação: {})...", "SIM" if use_pagination else "NÃO")
        
        all_clips = []
        
        # Se não especificar critérios, buscar clips de jogos populares
        if not broadcaster_ids and not game_ids:
            top_games = self.get_top_games(10)  # Aumentado de 5 para 10
            game_ids = [game['id'] for game in top_games]
        
        if game_ids:
            for game_id in game_ids[:15]:  # Aumentado de 5 para 15 jogos
                info("🎮 Buscando clips do jogo: {}", game_id)
                game_clips = []
                cursor = None
                page = 0
                per_page = min(limit, self.config.MAX_RESULTS_PER_PAGE)
                
                while len(game_clips) < limit and page < max_pages_per_item:
                    params = {
                        'game_id': game_id,
                        'first': per_page
                    }
                    
                    if cursor:
                        params['after'] = cursor
                    
                    data = self._make_request(self.config.ENDPOINTS['clips'], params)
                    
                    if not data or 'data' not in data:
                        break
                        
                    clips = data['data']
                    if not clips:
                        break
                        
                    game_clips.extend(clips)
                    page += 1
                    
                    info("📄 Jogo {} - Página {}: {} clips", game_id, page, len(clips))
                    
                    # Verificar se há próxima página
                    if not use_pagination or not data.get('pagination', {}).get('cursor'):
                        break
                        
                    cursor = data['pagination']['cursor']
                    
                    if len(game_clips) >= limit:
                        break
                
                final_game_clips = game_clips[:limit]
                all_clips.extend(final_game_clips)
                info("✅ {} clips encontrados para jogo {} ({} páginas)", 
                     len(final_game_clips), game_id, page)
        
        if broadcaster_ids:
            for broadcaster_id in broadcaster_ids[:20]:  # Aumentado de 10 para 20
                info("👤 Buscando clips do streamer: {}", broadcaster_id)
                streamer_clips = []
                cursor = None
                page = 0
                per_page = min(limit, self.config.MAX_RESULTS_PER_PAGE)
                
                while len(streamer_clips) < limit and page < max_pages_per_item:
                    params = {
                        'broadcaster_id': broadcaster_id,
                        'first': per_page
                    }
                    
                    if cursor:
                        params['after'] = cursor
                    
                    data = self._make_request(self.config.ENDPOINTS['clips'], params)
                    
                    if not data or 'data' not in data:
                        break
                        
                    clips = data['data']
                    if not clips:
                        break
                        
                    streamer_clips.extend(clips)
                    page += 1
                    
                    info("📄 Streamer {} - Página {}: {} clips", broadcaster_id, page, len(clips))
                    
                    # Verificar se há próxima página
                    if not use_pagination or not data.get('pagination', {}).get('cursor'):
                        break
                        
                    cursor = data['pagination']['cursor']
                    
                    if len(streamer_clips) >= limit:
                        break
                
                final_streamer_clips = streamer_clips[:limit]
                all_clips.extend(final_streamer_clips)
                info("✅ {} clips encontrados para streamer {} ({} páginas)", 
                     len(final_streamer_clips), broadcaster_id, page)
        
        info("📊 Total: {} clips encontrados", len(all_clips))
        return all_clips

if __name__ == "__main__":
    # Teste básico do cliente
    client = TwitchAPIClient()
    
    if client.validate_token():
        info("🎯 Testando busca de usuários...")
        users = client.get_users(['twitchdev'])
        info("Usuários encontrados: {}", len(users))
        
        info("🎯 Testando busca de jogos populares...")
        games = client.get_top_games(3)
        info("Jogos encontrados: {}", len(games))
    else:
        error("❌ Token inválido - não é possível testar o cliente") 