import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from logger import info, error

# Carregar variáveis de ambiente
load_dotenv()

class TwitchAPI:
    """
    Classe base para gerenciar inicialização da API da Twitch
    """
    
    def __init__(self):
        self.base_url = "https://api.twitch.tv/helix"
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.client_secret = os.getenv('TWITCH_CLIENT_SECRET')
        self.access_token = os.getenv('TWITCH_TOKEN')
        
        if not self.client_id or not self.client_secret or not self.access_token:
            error("Credenciais da Twitch não encontradas no .env")
            raise ValueError("Credenciais da Twitch não encontradas")
        
        self.headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self.access_token}'
        }
        
        info("TwitchAPI inicializada com sucesso") 