import asyncio
import aiohttp
import aiofiles
import json
import os
import sys
from tqdm.asyncio import tqdm
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error
from twitch_api import TwitchAPI

# CONFIGURAÇÕES - Modifique aqui para ajustar o comportamento
GAMES_PER_BATCH = 100   # Número de games por lote (máximo 100)
ENDPOINT = "/games"     # Endpoint específico para games
CONCURRENT_BATCHES = 30 # Número de lotes processados simultaneamente

async def read_json_async(file_path):
    """
    Lê um arquivo JSON de forma assíncrona
    
    Args:
        file_path (str): Caminho do arquivo JSON
        
    Returns:
        dict: Dados do JSON ou None se erro
    """
    try:
        if not os.path.exists(file_path):
            return None
            
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        return None

async def get_games_async(session, api, game_ids):
    """
    Busca informações de games por IDs (versão assíncrona)
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        game_ids (list): Lista de IDs de games (máximo 100 por requisição)
        
    Returns:
        dict: Resposta da API com dados dos games
    """
    try:
        url = api.base_url + ENDPOINT
        
        # A API aceita máximo 100 IDs por requisição
        if len(game_ids) > GAMES_PER_BATCH:
            return None
        
        # Fazer requisição com múltiplos parâmetros id
        async with session.get(url, headers=api.headers, params={'id': game_ids}) as response:
            if response.status != 200:
                return None
            return await response.json()
        
    except Exception as e:
        return None

async def process_game_batch(session, api, game_batch, semaphore):
    """
    Processa um lote de games com controle de concorrência
    
    Args:
        session (aiohttp.ClientSession): Sessão HTTP assíncrona
        api (TwitchAPI): Instância da API da Twitch
        game_batch (list): Lote de IDs de games
        semaphore (asyncio.Semaphore): Semáforo para controlar concorrência
        
    Returns:
        list: Lista de games encontrados no lote
    """
    async with semaphore:
        response = await get_games_async(session, api, game_batch)
        if response and 'data' in response:
            return response['data']
        return []

async def extract_games():
    """
    Extrai game_ids dos arquivos streams.json e clips.json, busca dados dos games na API e salva em games.json
    """
    try:
        # Caminhos dos arquivos
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
        streams_file = os.path.join(data_dir, 'streams.json')
        clips_file = os.path.join(data_dir, 'clips.json')
        games_file = os.path.join(data_dir, 'games.json')
        
        # Ler todos os arquivos JSON simultaneamente
        info("Lendo arquivos JSON simultaneamente...")
        streams_data, clips_data = await asyncio.gather(
            read_json_async(streams_file),
            read_json_async(clips_file),
            return_exceptions=True
        )
        
        # Verificar quais arquivos foram lidos com sucesso
        files_read = []
        if streams_data and not isinstance(streams_data, Exception):
            files_read.append("streams")
        if clips_data and not isinstance(clips_data, Exception):
            files_read.append("clips")
        
        if not files_read:
            error("Nenhum arquivo JSON foi encontrado. Execute os scripts de extração primeiro.")
            return
        
        info(f"Arquivos lidos com sucesso: {', '.join(files_read)}")
        
        # Extrair game_ids únicos de todos os arquivos
        game_ids = set()
        
        # Extrair de streams
        if streams_data and not isinstance(streams_data, Exception):
            streams = streams_data.get('data', [])
            for stream in streams:
                game_id = stream.get('game_id')
                if game_id and game_id != '0':  # Ignorar game_id '0' (sem categoria)
                    game_ids.add(game_id)
            info(f"Game IDs extraídos de streams: {len([s.get('game_id') for s in streams if s.get('game_id') and s.get('game_id') != '0'])}")
        
        # Extrair de clips
        if clips_data and not isinstance(clips_data, Exception):
            clips = clips_data.get('data', [])
            for clip in clips:
                game_id = clip.get('game_id')
                if game_id and game_id != '0':
                    game_ids.add(game_id)
            info(f"Game IDs extraídos de clips: {len([c.get('game_id') for c in clips if c.get('game_id') and c.get('game_id') != '0'])}")
        

        
        game_ids_list = list(game_ids)
        info(f"Total de games únicos encontrados: {len(game_ids_list)}")
        
        if not game_ids_list:
            error("Nenhum game_id encontrado nos arquivos JSON")
            return
        
        # Inicializar API da Twitch
        info("Inicializando API da Twitch...")
        api = TwitchAPI()
        
        # Dividir em lotes
        game_batches = []
        for i in range(0, len(game_ids_list), GAMES_PER_BATCH):
            batch = game_ids_list[i:i + GAMES_PER_BATCH]
            game_batches.append(batch)
        
        info(f"Criados {len(game_batches)} lotes de games para processamento")
        
        # Configurar semáforo para controlar concorrência
        semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)
        
        info(f"Iniciando busca assíncrona de games")
        info(f"Configuração: {CONCURRENT_BATCHES} lotes simultâneos de {GAMES_PER_BATCH} games cada")
        
        # Criar sessão HTTP assíncrona
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Criar tasks para todos os lotes
            tasks = [
                process_game_batch(session, api, batch, semaphore)
                for batch in game_batches
            ]
            
            # Executar tasks com barra de progresso
            batch_results = await tqdm.gather(*tasks, desc="Processando lotes de games")
        
        # Processar resultados
        all_games = []
        for batch_games in batch_results:
            if batch_games:
                all_games.extend(batch_games)
        
        info(f"Dados coletados de {len(all_games)} games")
        
        # Preparar dados finais
        final_data = {
            'data': all_games,
            'total_games': len(all_games),
            'requested_games': len(game_ids_list),
            'success_rate': len(all_games) / len(game_ids_list) * 100 if game_ids_list else 0,
            'games_per_batch': GAMES_PER_BATCH,
            'concurrent_batches': CONCURRENT_BATCHES,
            'total_batches': len(game_batches),
            'source_files': files_read
        }
        
        # Salvar em JSON
        info(f"Salvando dados em {games_file}...")
        async with aiofiles.open(games_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(final_data, ensure_ascii=False, indent=2))
        
        info(f"Dados dos games salvos em {games_file}")
        info(f"Taxa de sucesso: {final_data['success_rate']:.1f}%")
        
    except Exception as e:
        error(f"Erro ao extrair games: {str(e)}")

def main():
    """
    Função principal para executar o processo assíncrono
    """
    asyncio.run(extract_games())

if __name__ == "__main__":
    main() 