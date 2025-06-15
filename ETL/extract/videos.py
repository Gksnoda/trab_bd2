import asyncio
import aiohttp
import json
import os
import sys
from tqdm.asyncio import tqdm


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error
from twitch_api import TwitchAPI

# CONFIGURAÇÕES - Modifique aqui para ajustar o comportamento
MAX_PAGES = 5           # Número máximo de páginas por usuário
VIDEOS_PER_PAGE = 30    # Número de vídeos por página (máximo 100)
VIDEO_TYPE = "archive"  # Tipo de vídeo (all, archive, highlight, upload)
ENDPOINT = "/videos"    # Endpoint específico para vídeos
CONCURRENT_USERS = 30    # Número de usuários processados simultaneamente

async def get_videos_async(session, api, user_id, first=30, video_type="archive", cursor=None):
    """
    Busca vídeos de um usuário específico (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        first (int): Número de vídeos por página (máximo 100)
        video_type (str): Tipo de vídeo (all, archive, highlight, upload)
        cursor (str): Cursor para paginação
        
    Returns:
        dict: Resposta da API com vídeos e dados de paginação
    """
    try:
        url = api.base_url + ENDPOINT
        params = {
            'user_id': user_id,
            'first': first,
            'type': video_type
        }
        
        if cursor:
            params['after'] = cursor
        
        async with session.get(url, headers=api.headers, params=params) as response:
            if response.status != 200:
                return None
            return await response.json()
        
    except Exception as e:
        return None

async def get_user_videos_async(session, api, user_id, max_pages=5):
    """
    Busca todas as páginas de vídeos de um usuário (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        max_pages (int): Número máximo de páginas para buscar
        
    Returns:
        list: Lista com todos os vídeos do usuário
    """
    try:
        all_videos = []
        cursor = None
        
        for page in range(1, max_pages + 1):
            videos_data = await get_videos_async(session, api, user_id, first=VIDEOS_PER_PAGE, video_type=VIDEO_TYPE, cursor=cursor)
            
            if not videos_data:
                break
            
            page_videos = videos_data['data']
            
            if not page_videos:
                break
            
            all_videos.extend(page_videos)
            
            # Obter cursor para próxima página
            pagination = videos_data.get('pagination', {})
            cursor = pagination.get('cursor')
            
            if not cursor:
                break
        
        return all_videos
        
    except Exception as e:
        return []

async def process_user_videos(session, api, user_id, semaphore):
    """
    Processa vídeos de um usuário com controle de concorrência
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        semaphore (asyncio.Semaphore): Semáforo para controlar concorrência
        
    Returns:
        tuple: (user_id, lista_de_videos)
    """
    async with semaphore:
        user_videos = await get_user_videos_async(session, api, user_id, MAX_PAGES)
        return user_id, user_videos

async def extract_videos():
    """
    Extrai user_ids do arquivo users.json, busca vídeos de cada usuário na API e salva em videos.json
    """
    try:
        # Caminhos dos arquivos
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        users_file = os.path.join(data_dir, 'users.json')
        videos_file = os.path.join(data_dir, 'videos.json')
        
        # Verificar se o arquivo users.json existe
        if not os.path.exists(users_file):
            error(f"Arquivo {users_file} não encontrado. Execute users.py primeiro.")
            return
        
        # Ler dados dos usuários
        info("Lendo dados dos usuários...")
        with open(users_file, 'r', encoding='utf-8') as f:
            users_data = json.load(f)
        
        # Extrair user_ids
        users = users_data.get('data', [])
        user_ids = [user.get('id') for user in users if user.get('id')]
        
        info(f"Encontrados {len(user_ids)} usuários para buscar vídeos")
        
        if not user_ids:
            error("Nenhum user_id encontrado nos usuários")
            return
        
        # Inicializar API da Twitch
        info("Inicializando API da Twitch...")
        api = TwitchAPI()
        
        # Configurar semáforo para controlar concorrência
        semaphore = asyncio.Semaphore(CONCURRENT_USERS)
        
        info(f"Iniciando busca assíncrona de vídeos ({VIDEO_TYPE})")
        info(f"Configuração: {CONCURRENT_USERS} usuários simultâneos, {MAX_PAGES} páginas de {VIDEOS_PER_PAGE} vídeos cada")
        
        # Criar sessão HTTP assíncrona
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Criar tasks para todos os usuários
            tasks = [
                process_user_videos(session, api, user_id, semaphore)
                for user_id in user_ids
            ]
            
            # Executar tasks com barra de progresso
            results = await tqdm.gather(*tasks, desc="Processando usuários")
        
        # Processar resultados
        all_videos = []
        users_with_videos = 0
        total_videos = 0
        
        for user_id, user_videos in results:
            if user_videos:
                all_videos.extend(user_videos)
                users_with_videos += 1
                total_videos += len(user_videos)
        
        info(f"Coleta finalizada: {total_videos} vídeos de {users_with_videos} usuários")
        
        # Preparar dados finais
        final_data = {
            'data': all_videos,
            'total_videos': total_videos,
            'total_users_processed': len(user_ids),
            'users_with_videos': users_with_videos,
            'success_rate': users_with_videos / len(user_ids) * 100 if user_ids else 0,
            'video_type': VIDEO_TYPE,
            'videos_per_page': VIDEOS_PER_PAGE,
            'max_pages_per_user': MAX_PAGES,
            'concurrent_users': CONCURRENT_USERS
        }
        
        # Salvar em JSON
        info(f"Salvando dados em {videos_file}...")
        with open(videos_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        info(f"Dados dos vídeos salvos em {videos_file}")
        info(f"Total: {total_videos} vídeos de {users_with_videos}/{len(user_ids)} usuários")
        info(f"Taxa de sucesso: {final_data['success_rate']:.1f}%")
        
    except Exception as e:
        error(f"Erro ao extrair vídeos: {str(e)}")

def main():
    """
    Função principal para executar o processo assíncrono
    """
    asyncio.run(extract_videos())

if __name__ == "__main__":
    main() 