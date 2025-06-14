"""
Configurações centralizadas do ETL Twitch
Baseado no .env e no modelo MER.png
"""

import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Carregar variáveis de ambiente
load_dotenv()

class TwitchAPIConfig:
    """Configurações da API Twitch"""
    
    CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
    CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
    ACCESS_TOKEN = os.getenv('TWITCH_TOKEN')
    REDIRECT_URI = os.getenv('TWITCH_REDIRECT_URI')
    
    # URLs base da API
    API_BASE_URL = "https://api.twitch.tv/helix"
    AUTH_BASE_URL = "https://id.twitch.tv/oauth2"
    
    # Endpoints específicos
    ENDPOINTS = {
        'users': f"{API_BASE_URL}/users",
        'games': f"{API_BASE_URL}/games",
        'games_top': f"{API_BASE_URL}/games/top",
        'streams': f"{API_BASE_URL}/streams",
        'videos': f"{API_BASE_URL}/videos",
        'clips': f"{API_BASE_URL}/clips",
        'validate': f"{AUTH_BASE_URL}/validate"
    }
    
    # Headers padrão
    @classmethod
    def get_headers(cls):
        return {
            'Authorization': f'Bearer {cls.ACCESS_TOKEN}',
            'Client-Id': cls.CLIENT_ID
        }
    
    # Limites de paginação da API
    MAX_RESULTS_PER_PAGE = 100
    DEFAULT_RESULTS_PER_PAGE = 100

class DatabaseConfig:
    """Configurações do banco PostgreSQL"""
    
    # URL completa do banco
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    # Parse da URL para componentes individuais
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        HOST = parsed.hostname
        PORT = parsed.port
        DATABASE = parsed.path.lstrip('/')
        USERNAME = parsed.username
        PASSWORD = parsed.password
    else:
        HOST = 'localhost'
        PORT = 5432
        DATABASE = 'twitch_analytics'
        USERNAME = 'postgres'
        PASSWORD = 'admin'
    
    # String de conexão para psycopg2
    @classmethod
    def get_connection_params(cls):
        return {
            'host': cls.HOST,
            'port': cls.PORT,
            'database': cls.DATABASE,
            'user': cls.USERNAME,
            'password': cls.PASSWORD
        }

class ETLConfig:
    """Configurações gerais do ETL"""
    
    # Diretórios de trabalho
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # etl/
    DATA_DIR = os.path.join(BASE_DIR, 'data')  # Para arquivos temporários
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')  # Para logs específicos
    
    # Configurações de batch/lotes
    BATCH_SIZE = 1000  # Registros por lote
    MAX_RETRIES = 3    # Tentativas em caso de erro
    RETRY_DELAY = 5    # Segundos entre tentativas
    
    # Configurações de timeout
    API_TIMEOUT = 30   # Segundos para timeout da API
    DB_TIMEOUT = 60    # Segundos para timeout do banco

# Configurações das tabelas baseadas no MER.png
class TableSchemas:
    """Esquemas das tabelas baseados no MER"""
    
    # Tabela: users (baseada na entidade User do MER)
    USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(50) PRIMARY KEY,
        login VARCHAR(100) NOT NULL,
        display_name VARCHAR(100),
        broadcaster_type VARCHAR(20),
        type VARCHAR(20),
        description TEXT,
        profile_image_url TEXT,
        view_count BIGINT DEFAULT 0,
        created_at TIMESTAMP
    );
    """
    
    # Tabela: games (baseada na entidade Game do MER)
    GAMES_TABLE = """
    CREATE TABLE IF NOT EXISTS games (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        box_art_url TEXT
    );
    """
    
    # Tabela: streams (baseada na entidade Stream do MER)
    STREAMS_TABLE = """
    CREATE TABLE IF NOT EXISTS streams (
        id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        game_id VARCHAR(50),
        title TEXT,
        viewer_count INTEGER DEFAULT 0,
        started_at TIMESTAMP,
        language VARCHAR(10),
        thumbnail_url TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (game_id) REFERENCES games(id)
    );
    """
    
    # Tabela: videos (baseada na entidade Video do MER)  
    VIDEOS_TABLE = """
    CREATE TABLE IF NOT EXISTS videos (
        id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        title TEXT,
        description TEXT,
        created_at TIMESTAMP,
        published_at TIMESTAMP,
        url TEXT,
        thumbnail_url TEXT,
        view_count BIGINT DEFAULT 0,
        duration VARCHAR(20),
        language VARCHAR(10),
        type VARCHAR(20),
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    
    # Tabela: clips (baseada na entidade Clip do MER)
    CLIPS_TABLE = """
    CREATE TABLE IF NOT EXISTS clips (
        id VARCHAR(50) PRIMARY KEY,
        broadcaster_id VARCHAR(50) NOT NULL,
        creator_id VARCHAR(50),
        video_id VARCHAR(50),
        game_id VARCHAR(50),
        title TEXT,
        view_count BIGINT DEFAULT 0,
        created_at TIMESTAMP,
        thumbnail_url TEXT,
        url TEXT,
        embed_url TEXT,
        duration REAL,
        language VARCHAR(10),
        FOREIGN KEY (broadcaster_id) REFERENCES users(id),
        FOREIGN KEY (creator_id) REFERENCES users(id),
        FOREIGN KEY (video_id) REFERENCES videos(id),
        FOREIGN KEY (game_id) REFERENCES games(id)
    );
    """
    
    # Ordem de criação das tabelas (respeitando dependências)
    CREATION_ORDER = [
        ('users', USERS_TABLE),
        ('games', GAMES_TABLE),
        ('streams', STREAMS_TABLE),
        ('videos', VIDEOS_TABLE),
        ('clips', CLIPS_TABLE)
    ]

# Validação das configurações
def validate_config():
    """Valida se todas as configurações necessárias estão presentes"""
    errors = []
    
    # Validar configurações da API Twitch
    if not TwitchAPIConfig.CLIENT_ID:
        errors.append("TWITCH_CLIENT_ID não encontrado no .env")
    if not TwitchAPIConfig.CLIENT_SECRET:
        errors.append("TWITCH_CLIENT_SECRET não encontrado no .env")
    if not TwitchAPIConfig.ACCESS_TOKEN:
        errors.append("TWITCH_TOKEN não encontrado no .env")
    
    # Validar configurações do banco
    if not DatabaseConfig.DATABASE_URL:
        errors.append("DATABASE_URL não encontrada no .env")
    
    return errors

if __name__ == "__main__":
    # Teste de configuração
    errors = validate_config()
    if errors:
        print("❌ Erros de configuração:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Todas as configurações estão válidas!")
        print(f"✅ API Twitch: {TwitchAPIConfig.CLIENT_ID[:8]}...")
        print(f"✅ Banco: {DatabaseConfig.HOST}:{DatabaseConfig.PORT}/{DatabaseConfig.DATABASE}") 