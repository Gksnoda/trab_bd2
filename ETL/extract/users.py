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
USERS_PER_BATCH = 100   # Número de usuários por lote (máximo 100)
ENDPOINT = "/users"     # Endpoint específico para usuários
CONCURRENT_BATCHES = 30 # Número de lotes processados simultaneamente

async def get_users_async(session, api, user_ids):
    """
    Busca informações de usuários por IDs (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_ids (list): Lista de IDs de usuários (máximo 100 por requisição)
        
    Returns:
        dict: Resposta da API com dados dos usuários
    """
    try:
        url = api.base_url + ENDPOINT
        
        # A API aceita máximo 100 IDs por requisição
        if len(user_ids) > USERS_PER_BATCH:
            return None
        
        # Fazer requisição com múltiplos parâmetros id
        async with session.get(url, headers=api.headers, params={'id': user_ids}) as response:
            if response.status != 200:
                return None
            return await response.json()
        
    except Exception as e:
        return None

async def process_user_batch(session, api, user_batch, semaphore):
    """
    Processa um lote de usuários com controle de concorrência
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        user_batch (list): Lote de IDs de usuários
        semaphore (asyncio.Semaphore): Semáforo para controlar concorrência
        
    Returns:
        list: Lista de usuários encontrados no lote
    """
    async with semaphore:
        response = await get_users_async(session, api, user_batch)
        if response and 'data' in response:
            return response['data']
        return []

async def extract_users():
    """
    Extrai user_ids do arquivo streams.json, busca dados dos usuários na API e salva em users.json
    """
    try:
        # Caminhos dos arquivos
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        streams_file = os.path.join(data_dir, 'streams.json')
        users_file = os.path.join(data_dir, 'users.json')
        
        # Verificar se o arquivo streams.json existe
        if not os.path.exists(streams_file):
            error(f"Arquivo {streams_file} não encontrado. Execute streams.py primeiro.")
            return
        
        # Ler dados das streams
        info("Lendo dados das streams...")
        with open(streams_file, 'r', encoding='utf-8') as f:
            streams_data = json.load(f)
        
        # Extrair user_ids únicos
        user_ids = set()
        streams = streams_data.get('data', [])
        
        for stream in streams:
            user_id = stream.get('user_id')
            if user_id:
                user_ids.add(user_id)
        
        user_ids_list = list(user_ids)
        info(f"Encontrados {len(user_ids_list)} usuários únicos nas streams")
        
        if not user_ids_list:
            error("Nenhum user_id encontrado nas streams")
            return
        
        # Inicializar API da Twitch
        info("Inicializando API da Twitch...")
        api = TwitchAPI()
        
        # Dividir em lotes
        user_batches = []
        for i in range(0, len(user_ids_list), USERS_PER_BATCH):
            batch = user_ids_list[i:i + USERS_PER_BATCH]
            user_batches.append(batch)
        
        info(f"Criados {len(user_batches)} lotes de usuários para processamento")
        
        # Configurar semáforo para controlar concorrência
        semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)
        
        info(f"Iniciando busca assíncrona de usuários")
        info(f"Configuração: {CONCURRENT_BATCHES} lotes simultâneos de {USERS_PER_BATCH} usuários cada")
        
        # Criar sessão HTTP assíncrona
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Criar tasks para todos os lotes
            tasks = [
                process_user_batch(session, api, batch, semaphore)
                for batch in user_batches
            ]
            
            # Executar tasks com barra de progresso
            batch_results = await tqdm.gather(*tasks, desc="Processando lotes de usuários")
        
        # Processar resultados
        all_users = []
        for batch_users in batch_results:
            if batch_users:
                all_users.extend(batch_users)
        
        info(f"Dados coletados de {len(all_users)} usuários")
        
        # Preparar dados finais
        final_data = {
            'data': all_users,
            'total_users': len(all_users),
            'requested_users': len(user_ids_list),
            'success_rate': len(all_users) / len(user_ids_list) * 100 if user_ids_list else 0,
            'users_per_batch': USERS_PER_BATCH,
            'concurrent_batches': CONCURRENT_BATCHES,
            'total_batches': len(user_batches)
        }
        
        # Salvar em JSON
        info(f"Salvando dados em {users_file}...")
        with open(users_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        info(f"Dados dos usuários salvos em {users_file}")
        info(f"Taxa de sucesso: {final_data['success_rate']:.1f}%")
        
    except Exception as e:
        error(f"Erro ao extrair usuários: {str(e)}")

def main():
    """
    Função principal para executar o processo assíncrono
    """
    asyncio.run(extract_users())

if __name__ == "__main__":
    main() 