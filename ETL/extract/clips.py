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
MAX_PAGES = 2           # Número máximo de páginas por usuário
CLIPS_PER_PAGE = 30     # Número de clips por página (máximo 100)
ENDPOINT = "/clips"     # Endpoint específico para clips
CONCURRENT_USERS = 30   # Número de usuários processados simultaneamente

async def get_clips_async(session, api, user_id, first=30, cursor=None):
    """
    Busca clips de um usuário específico (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        first (int): Número de clips por página (máximo 100)
        cursor (str): Cursor para paginação
        
    Returns:
        dict: Resposta da API com clips e dados de paginação
    """
    try:
        url = api.base_url + ENDPOINT
        params = {
            'broadcaster_id': user_id,
            'first': first
        }
        
        if cursor:
            params['after'] = cursor
        
        async with session.get(url, headers=api.headers, params=params) as response:
            if response.status != 200:
                return None
            return await response.json()
        
    except Exception as e:
        return None

async def get_user_clips_async(session, api, user_id, max_pages=2):
    """
    Busca todas as páginas de clips de um usuário (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        max_pages (int): Número máximo de páginas para buscar
        
    Returns:
        list: Lista com todos os clips do usuário
    """
    try:
        all_clips = []
        cursor = None
        
        for page in range(1, max_pages + 1):
            clips_data = await get_clips_async(session, api, user_id, first=CLIPS_PER_PAGE, cursor=cursor)
            
            if not clips_data:
                break
            
            page_clips = clips_data['data']
            
            if not page_clips:
                break
            
            all_clips.extend(page_clips)
            
            # Obter cursor para próxima página
            pagination = clips_data.get('pagination', {})
            cursor = pagination.get('cursor')
            
            if not cursor:
                break
        
        return all_clips
        
    except Exception as e:
        return []

async def process_user_clips(session, api, user_id, semaphore):
    """
    Processa clips de um usuário com controle de concorrência
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_id (str): ID do usuário
        semaphore (asyncio.Semaphore): Semáforo para controlar concorrência
        
    Returns:
        tuple: (user_id, lista_de_clips)
    """
    async with semaphore:
        user_clips = await get_user_clips_async(session, api, user_id, MAX_PAGES)
        return user_id, user_clips

async def extract_clips():
    """
    Extrai user_ids do arquivo users.json, busca clips de cada usuário na API e salva em clips.json
    """
    try:
        # Caminhos dos arquivos
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        users_file = os.path.join(data_dir, 'users.json')
        clips_file = os.path.join(data_dir, 'clips.json')
        
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
        
        info(f"Encontrados {len(user_ids)} usuários para buscar clips")
        
        if not user_ids:
            error("Nenhum user_id encontrado nos usuários")
            return
        
        # Inicializar API da Twitch
        info("Inicializando API da Twitch...")
        api = TwitchAPI()
        
        # Configurar semáforo para controlar concorrência
        semaphore = asyncio.Semaphore(CONCURRENT_USERS)
        
        info(f"Iniciando busca assíncrona de clips")
        info(f"Configuração: {CONCURRENT_USERS} usuários simultâneos, {MAX_PAGES} páginas de {CLIPS_PER_PAGE} clips cada")
        
        # Criar sessão HTTP assíncrona
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Criar tasks para todos os usuários
            tasks = [
                process_user_clips(session, api, user_id, semaphore)
                for user_id in user_ids
            ]
            
            # Executar tasks com barra de progresso
            results = await tqdm.gather(*tasks, desc="Processando usuários")
        
        # Processar resultados
        all_clips = []
        users_with_clips = 0
        total_clips = 0
        
        for user_id, user_clips in results:
            if user_clips:
                all_clips.extend(user_clips)
                users_with_clips += 1
                total_clips += len(user_clips)
        
        info(f"Coleta finalizada: {total_clips} clips de {users_with_clips} usuários")
        
        # Preparar dados finais
        final_data = {
            'data': all_clips,
            'total_clips': total_clips,
            'total_users_processed': len(user_ids),
            'users_with_clips': users_with_clips,
            'success_rate': users_with_clips / len(user_ids) * 100 if user_ids else 0,
            'clips_per_page': CLIPS_PER_PAGE,
            'max_pages_per_user': MAX_PAGES,
            'concurrent_users': CONCURRENT_USERS
        }
        
        # Salvar em JSON
        info(f"Salvando dados em {clips_file}...")
        with open(clips_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        info(f"Dados dos clips salvos em {clips_file}")
        info(f"Total: {total_clips} clips de {users_with_clips}/{len(user_ids)} usuários")
        info(f"Taxa de sucesso: {final_data['success_rate']:.1f}%")
        
    except Exception as e:
        error(f"Erro ao extrair clips: {str(e)}")

def main():
    """
    Função principal para executar o processo assíncrono
    """
    asyncio.run(extract_clips())

if __name__ == "__main__":
    main() 