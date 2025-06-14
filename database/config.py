import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from etl.utils.logger import info, error

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do banco PostgreSQL com encoding UTF-8
DATABASE_URL = os.getenv(
    'DATABASE_URL', 
    'postgresql://postgres:admin@localhost:5432/twitch_analytics?client_encoding=utf8'
)

# Criar engine do SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Para reduzir logs durante debug
    pool_size=10,
    max_overflow=20,
    # Configurações específicas para resolver problemas de encoding
    connect_args={
        "client_encoding": "utf8",
        "options": "-c timezone=UTC"
    }
)

# Criar session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para os modelos
Base = declarative_base()

def get_db():
    """
    Dependency para obter sessão do banco de dados
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """
    Criar todas as tabelas no banco de dados
    """
    info("Criando tabelas no banco de dados...")
    Base.metadata.create_all(bind=engine)
    info("Tabelas criadas com sucesso!")

def drop_tables():
    """
    Dropar todas as tabelas do banco de dados
    """
    info("Dropando todas as tabelas...")
    Base.metadata.drop_all(bind=engine)
    info("Tabelas removidas!") 