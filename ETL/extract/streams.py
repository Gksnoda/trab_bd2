import requests
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error
from twitch_api import TwitchAPI

# CONFIGURAÇÕES - Modifique aqui para ajustar o comportamento
MAX_PAGES = 3          # Número máximo de páginas para buscar
STREAMS_PER_PAGE = 100  # Número de streams por página (máximo 100)
ENDPOINT = "/streams"   # Endpoint específico para streams

def get_streams(api, first=100, cursor=None):
    """
    Busca streams da Twitch
    
    Args:
        api (TwitchAPI): Instância da API da Twitch
        first (int): Número de streams por página (máximo 100)
        cursor (str): Cursor para paginação
        
    Returns:
        dict: Resposta da API com streams e dados de paginação
    """
    try:
        url = api.base_url + ENDPOINT
        params = {'first': first}
        
        if cursor:
            params['after'] = cursor
        
        response = requests.get(url, headers=api.headers, params=params)
        
        if response.status_code != 200:
            error(f"Erro ao buscar streams: {response.status_code}")
            return None
            
        return response.json()
        
    except Exception as e:
        error(f"Erro ao buscar streams: {str(e)}")
        return None

def fetch_streams():
    """
    Busca streams da Twitch API e salva em JSON
    """
    try:
        # Inicializar API da Twitch
        info("Inicializando API da Twitch...")
        api = TwitchAPI()
        
        # Criar diretório se não existir
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        os.makedirs(output_dir, exist_ok=True)
        info(f"Diretório criado/verificado: {output_dir}")
        
        # Buscar páginas configuradas
        all_streams = []
        cursor = None
        total_streams = 0
        
        for page in range(1, MAX_PAGES + 1):
            info(f"Buscando página {page}/{MAX_PAGES}...")
            
            # Usar método local para buscar streams
            streams_data = get_streams(api, first=STREAMS_PER_PAGE, cursor=cursor)
            
            if not streams_data:
                error(f"Erro ao buscar streams na página {page}")
                break
            
            page_streams = streams_data['data']
            
            if not page_streams:
                info(f"Nenhuma stream encontrada na página {page}. Parando...")
                break
            
            # Adicionar streams da página atual
            all_streams.extend(page_streams)
            total_streams += len(page_streams)
            info(f"Página {page}: {len(page_streams)} streams encontradas")
            
            # Obter cursor para próxima página
            pagination = streams_data.get('pagination', {})
            cursor = pagination.get('cursor')
            
            if not cursor:
                info("Não há mais páginas disponíveis")
                break
        
        info(f"Total de streams coletadas: {total_streams}")
        
        # Preparar dados finais
        final_data = {
            'data': all_streams,
            'total_streams': total_streams,
            'pages_fetched': min(page, MAX_PAGES),
            'streams_per_page': STREAMS_PER_PAGE,
            'max_pages_configured': MAX_PAGES
        }
        
        # Salvar em JSON
        output_file = os.path.join(output_dir, 'streams.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        info(f"Streams salvas em {output_file}")
        
    except Exception as e:
        error(f"Erro ao buscar streams: {str(e)}")

if __name__ == "__main__":
    fetch_streams()
