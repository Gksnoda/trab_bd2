"""
Teste de conexão com a API da Twitch
Este script testa se as credenciais estão funcionando corretamente
"""

import os
import sys
import requests
from dotenv import load_dotenv

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Carregar variáveis de ambiente
load_dotenv()

def testar_api_twitch():
    """
    Testa a conexão com a API da Twitch
    """
    info("=== INICIANDO TESTE DA API TWITCH ===")
    
    # Obter credenciais do .env
    client_id = os.getenv('TWITCH_CLIENT_ID')
    client_secret = os.getenv('TWITCH_CLIENT_SECRET')
    token = os.getenv('TWITCH_TOKEN')
    
    if not client_id or not client_secret or not token:
        error("❌ Credenciais não encontradas no .env")
        return False
    
    info("✅ Credenciais carregadas - Client ID: {}", client_id[:8] + "...")
    
    # Headers para autenticação
    headers = {
        'Authorization': f'Bearer {token}',
        'Client-Id': client_id
    }
    
    try:
        # Teste 1: Validar token
        info("🔐 Testando validação do token...")
        validate_url = "https://id.twitch.tv/oauth2/validate"
        response = requests.get(validate_url, headers={'Authorization': f'OAuth {token}'})
        
        if response.status_code == 200:
            token_info = response.json()
            info(f"✅ Token válido - Expira em: {token_info.get('expires_in', 'N/A')} segundos")
        else:
            error(f"❌ Token inválido - Status: {response.status_code}")
            return False
        
        # Teste 2: Buscar informações de um usuário (TwitchDev)
        info("👤 Testando endpoint de usuários...")
        users_url = "https://api.twitch.tv/helix/users"
        params = {'login': 'twitchdev'}
        
        response = requests.get(users_url, headers=headers, params=params)
        
        if response.status_code == 200:
            user_data = response.json()
            if user_data['data']:
                user = user_data['data'][0]
                info(f"✅ Usuário encontrado: {user['display_name']} (ID: {user['id']})")
            else:
                error("⚠️ Nenhum usuário encontrado")
        else:
            error(f"❌ Erro ao buscar usuário - Status: {response.status_code}")
            return False
        
        # Teste 3: Buscar jogos populares
        info("🎮 Testando endpoint de jogos...")
        games_url = "https://api.twitch.tv/helix/games/top"
        params = {'first': 5}
        
        response = requests.get(games_url, headers=headers, params=params)
        
        if response.status_code == 200:
            games_data = response.json()
            info(f"✅ {len(games_data['data'])} jogos populares encontrados:")
            for game in games_data['data'][:3]:
                info(f"   - {game['name']} (ID: {game['id']})")
        else:
            error(f"❌ Erro ao buscar jogos - Status: {response.status_code}")
            return False
        
        # Teste 4: Buscar streams ao vivo
        info("📺 Testando endpoint de streams...")
        streams_url = "https://api.twitch.tv/helix/streams"
        params = {'first': 3}
        
        response = requests.get(streams_url, headers=headers, params=params)
        
        if response.status_code == 200:
            streams_data = response.json()
            info(f"✅ {len(streams_data['data'])} streams ao vivo encontradas:")
            for stream in streams_data['data']:
                info(f"   - {stream['user_name']}: {stream['game_name']} ({stream['viewer_count']} viewers)")
        else:
            error(f"❌ Erro ao buscar streams - Status: {response.status_code}")
            return False
        
        info("🎉 TODOS OS TESTES DA API TWITCH PASSARAM!")
        return True
        
    except requests.exceptions.RequestException as e:
        error(f"❌ Erro de conexão: {e}")
        return False
    except Exception as e:
        error(f"❌ Erro inesperado: {e}")
        return False

if __name__ == "__main__":
    sucesso = testar_api_twitch()
    if sucesso:
        info("✅ Teste concluído com sucesso!")
        sys.exit(0)
    else:
        error("❌ Teste falhou!")
        sys.exit(1) 