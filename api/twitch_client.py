import requests
import os
from typing import Dict, List, Optional
from dotenv import load_dotenv
from etl.utils.logger import info, error
import time
from datetime import datetime

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
        all_games_data = []

        def fetch_in_chunks(params_key: str, values: List[str]):
            nonlocal all_games_data
            chunk_size = 100  # Twitch API limit for ID-based lookups
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i + chunk_size]
                params = {params_key: chunk}
                response = self._make_request('games', params)
                all_games_data.extend(response.get('data', []))
                time.sleep(0.1) # Be respectful to the API

        if game_ids:
            fetch_in_chunks('id', game_ids)
        
        if names:
            # Similar to get_users, if both are provided, names are fetched after IDs.
            # Current ETL usage primarily uses game_ids.
            fetch_in_chunks('name', names)
            
        return all_games_data
    
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
        all_users_data = []
        
        def fetch_in_chunks(params_key: str, values: List[str]):
            nonlocal all_users_data
            # Process in chunks of 100, as per Twitch API limits for 'id' and 'login' params
            chunk_size = 100 
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i + chunk_size]
                params = {params_key: chunk}
                response = self._make_request('users', params)
                all_users_data.extend(response.get('data', []))
                # Small delay to be respectful to the API, though _make_request has rate limit handling
                time.sleep(0.1) 

        if user_ids:
            fetch_in_chunks('id', user_ids)
        
        if logins:
            # Note: If both user_ids and logins are provided, logins will be fetched *after* user_ids,
            # potentially leading to duplicate user entries if not handled by the caller.
            # For the current use case in data_extractor, only user_ids are used in the problematic call.
            fetch_in_chunks('login', logins)
            
        return all_users_data

    
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
    
    def get_all_paginated(self, endpoint: str, params: Dict, max_items: int = 1000) -> List[Dict]:
        """
        Buscar todos os dados paginados de um endpoint específico até max_items.
        """
        all_data = []
        current_params = params.copy()
        
        # Ensure 'first' is set for pagination, capped at 100
        current_params['first'] = min(current_params.get('first', 100), 100)

        info(f"Iniciando busca paginada para endpoint '{endpoint}' com params: {current_params}, target: {max_items} items.")

        while True:
            if len(all_data) >= max_items:
                info(f"Atingido o limite de {max_items} itens. Coletados: {len(all_data)}.")
                break

            response_json = self._make_request(endpoint, current_params)
            
            data_batch = response_json.get('data', [])
            if not data_batch:
                info(f"Nenhum item retornado na página atual para {endpoint}. Finalizando paginacao.")
                break
            
            all_data.extend(data_batch)
            info(f"Coletados {len(data_batch)} itens. Total até agora: {len(all_data)} para {endpoint}.")

            if len(all_data) >= max_items:
                info(f"Total de itens coletados ({len(all_data)}) atingiu ou excedeu {max_items}. Cortando excesso se houver.")
                all_data = all_data[:max_items] # Trim to max_items exactly
                break

            pagination = response_json.get('pagination', {})
            cursor = pagination.get('cursor')

            if cursor:
                current_params['after'] = cursor
                # 'first' remains as set initially for subsequent calls
            else:
                info(f"Nao ha mais cursor de paginacao para {endpoint}. Finalizando.")
                break
        
        info(f"Busca paginada para {endpoint} concluída. Total de itens coletados: {len(all_data)}.")
        return all_data 

    def get_videos_by_date_range(self, user_id: str, start_date: datetime, end_date: datetime, first: int = 20) -> List[Dict]:
        """
        Buscar vídeos de um usuário em um intervalo de datas específico
        """
        params = {
            'user_id': user_id,
            'first': first,
            'created_at': f"{start_date.isoformat()}Z..{end_date.isoformat()}Z"
        }
        
        try:
            response = self._make_request('videos', params)
            videos = response.get('data', [])
            
            # Filtrar vídeos que estão dentro do intervalo (dupla verificação)
            filtered_videos = []
            for video in videos:
                if video.get('created_at'):
                    video_date = datetime.fromisoformat(video['created_at'].replace('Z', '+00:00'))
                    if start_date <= video_date <= end_date:
                        filtered_videos.append(video)
            
            return filtered_videos
        except Exception as e:
            info(f"Erro ao buscar vídeos para usuário {user_id} no período {start_date} - {end_date}: {e}")
            return [] 