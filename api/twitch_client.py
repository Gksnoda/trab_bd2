import requests
import os
from typing import Dict, List, Optional
from dotenv import load_dotenv
from logger import info, error
import time

# Carregar variáveis de ambiente
load_dotenv()

class TwitchAPIClient:
    """
    Cliente para interagir com a API do Twitch
    """
    
    def __init__(self):
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.client_secret = os.getenv('TWITCH_CLIENT_SECRET')
        self.access_token = os.getenv('TWITCH_TOKEN')
        self.base_url = 'https://api.twitch.tv/helix'
        
        if not all([self.client_id, self.client_secret]):
            raise ValueError("TWITCH_CLIENT_ID e TWITCH_CLIENT_SECRET devem estar definidos no .env")
        
        # Headers padrão para todas as requisições
        self.headers = {
            'Client-Id': self.client_id,
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        info("Cliente Twitch API inicializado")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Fazer requisição para a API do Twitch com tratamento de erros
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            info(f"Fazendo requisição para: {url}")
            info(f"Parâmetros: {params}")
            
            response = requests.get(url, headers=self.headers, params=params)
            
            # Verificar rate limit
            if response.status_code == 429:
                info("Rate limit atingido, aguardando...")
                time.sleep(60)  # Aguardar 1 minuto
                response = requests.get(url, headers=self.headers, params=params)
            
            response.raise_for_status()
            data = response.json()
            
            info(f"Requisição bem-sucedida. Retornados {len(data.get('data', []))} itens")
            return data
            
        except requests.exceptions.RequestException as e:
            error(f"Erro na requisição para {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                error(f"Status code: {e.response.status_code}")
                error(f"Response: {e.response.text}")
            raise
    
    def get_top_games(self, first: int = 20) -> List[Dict]:
        """
        Buscar os jogos mais populares
        """
        params = {'first': first}
        response = self._make_request('games/top', params)
        return response.get('data', [])
    
    def get_games(self, game_ids: List[str] = None, names: List[str] = None) -> List[Dict]:
        params = {}
        if game_ids:
            # monta ?id=123&id=456&id=789
            params['id'] = game_ids
        if names:
            params['name'] = names
        
        response = self._make_request('games', params)
        return response.get('data', [])
    
    def get_streams(self, game_id: str = None, user_id: List[str] = None, 
                   user_login: List[str] = None, first: int = 20) -> List[Dict]:
        """
        Buscar streams ao vivo
        """
        params = {'first': first}
        
        if game_id:
            params['game_id'] = game_id
        if user_id:
            for uid in user_id:
                params[f'user_id'] = uid
        if user_login:
            for login in user_login:
                params[f'user_login'] = login
        
        response = self._make_request('streams', params)
        return response.get('data', [])
    
    def get_users(self, user_ids: List[str] = None, logins: List[str] = None) -> List[Dict]:
        params = {}
        if user_ids:
            # envia todos os IDs de uma vez: requests monta ?id=1&id=2&...
            params['id'] = user_ids
        if logins:
            params['login'] = logins
        response = self._make_request('users', params)
        return response.get('data', [])

    
    def get_videos(self, user_id: str = None, game_id: str = None, 
                  video_ids: List[str] = None, first: int = 20) -> List[Dict]:
        """
        Buscar vídeos (VODs, highlights, uploads)
        """
        params = {'first': first}
        
        if user_id:
            params['user_id'] = user_id
        elif game_id:
            params['game_id'] = game_id
        elif video_ids:
            for vid in video_ids:
                params[f'id'] = vid
        
        response = self._make_request('videos', params)
        return response.get('data', [])
    
    def get_clips(self, broadcaster_id: str = None, game_id: str = None,
                 clip_ids: List[str] = None, first: int = 20,
                 started_at: str = None, ended_at: str = None) -> List[Dict]:
        """
        Buscar clips
        """
        params = {'first': first}
        
        if broadcaster_id:
            params['broadcaster_id'] = broadcaster_id
        elif game_id:
            params['game_id'] = game_id
        elif clip_ids:
            for cid in clip_ids:
                params[f'id'] = cid
        
        if started_at:
            params['started_at'] = started_at
        if ended_at:
            params['ended_at'] = ended_at
        
        response = self._make_request('clips', params)
        return response.get('data', [])
    
    def get_all_paginated(self, endpoint_method, **kwargs) -> List[Dict]:
        """
        Buscar todos os dados paginados de um endpoint
        """
        all_data = []
        cursor = None
        
        while True:
            if cursor:
                kwargs['after'] = cursor
            
            response_data = endpoint_method(**kwargs)
            
            if not response_data:
                break
                
            all_data.extend(response_data)
            
            # Verificar se há mais páginas (isso precisa ser implementado baseado na resposta)
            # Por enquanto, vamos limitar para evitar loops infinitos
            if len(response_data) < kwargs.get('first', 20):
                break
        
        info(f"Total de itens coletados: {len(all_data)}")
        return all_data 