"""
Script de Validação do Setup ETL
Verifica se todas as dependências e configurações estão corretas
"""

import sys
import os
from typing import List, Tuple

# Adicionar paths para imports
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl.utils.logger import info, error

class ETLValidator:
    """Validador de setup do ETL"""
    
    def __init__(self):
        """Inicializa o validador"""
        self.errors = []
        self.warnings = []
    
    def validate_all(self) -> bool:
        """
        Executa todas as validações
        
        Returns:
            True se tudo OK, False se há erros críticos
        """
        info("🔍 === VALIDANDO SETUP DO ETL ===")
        
        # Validações
        self._validate_environment()
        self._validate_dependencies()
        self._validate_config_files()
        self._validate_database_connection()
        self._validate_twitch_api()
        self._validate_directories()
        
        # Relatório final
        self._generate_validation_report()
        
        return len(self.errors) == 0
    
    def _validate_environment(self):
        """Valida variáveis de ambiente"""
        info("🌍 Validando variáveis de ambiente...")
        
        required_vars = [
            'TWITCH_CLIENT_ID',
            'TWITCH_CLIENT_SECRET', 
            'TWITCH_REDIRECT_URI',
            'TWITCH_TOKEN',
            'DATABASE_URL'
        ]
        
        try:
            # Tentar carregar .env
            env_file = os.path.join(os.path.dirname(__file__), '..', '.env')
            if os.path.exists(env_file):
                info("✅ Arquivo .env encontrado")
                
                # Verificar se as variáveis estão definidas
                with open(env_file, 'r') as f:
                    env_content = f.read()
                
                for var in required_vars:
                    if f"{var}=" in env_content:
                        info("✅ Variável {} definida", var)
                    else:
                        self.errors.append(f"Variável de ambiente {var} não encontrada no .env")
            else:
                self.errors.append("Arquivo .env não encontrado")
                
        except Exception as e:
            self.errors.append(f"Erro ao validar ambiente: {e}")
    
    def _validate_dependencies(self):
        """Valida dependências Python"""
        info("📦 Validando dependências Python...")
        
        required_packages = [
            'requests',
            'psycopg2'
        ]
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                info("✅ Pacote {} disponível", package)
            except ImportError:
                self.errors.append(f"Pacote {package} não instalado")
    
    def _validate_config_files(self):
        """Valida arquivos de configuração"""
        info("⚙️ Validando arquivos de configuração...")
        
        config_files = [
            ('config/twitch_client.py', 'Cliente Twitch API'),
            ('config/database_client.py', 'Cliente PostgreSQL'),
            ('utils/logger.py', 'Sistema de Logs')
        ]
        
        base_path = os.path.dirname(__file__)
        
        for file_path, description in config_files:
            full_path = os.path.join(base_path, file_path)
            if os.path.exists(full_path):
                info("✅ {} encontrado", description)
            else:
                self.errors.append(f"{description} não encontrado: {file_path}")
    
    def _validate_database_connection(self):
        """Valida conexão com banco de dados"""
        info("🗄️ Validando conexão com PostgreSQL...")
        
        try:
            sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
            from database_client import DatabaseClient
            
            db_client = DatabaseClient()
            # Testar conexão usando o context manager
            with db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1;")
                result = cursor.fetchone()
                if result:
                    info("✅ Conexão com PostgreSQL OK")
                else:
                    self.errors.append("Falha ao conectar com PostgreSQL")
                
        except Exception as e:
            self.errors.append(f"Erro ao testar PostgreSQL: {e}")
    
    def _validate_twitch_api(self):
        """Valida conexão com API Twitch"""
        info("🎮 Validando conexão com API Twitch...")
        
        try:
            sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
            from twitch_client import TwitchAPIClient
            
            twitch_client = TwitchAPIClient()
            if twitch_client.validate_token():
                info("✅ Conexão com API Twitch OK")
            else:
                self.errors.append("Falha ao conectar com API Twitch")
                
        except Exception as e:
            self.errors.append(f"Erro ao testar API Twitch: {e}")
    
    def _validate_directories(self):
        """Valida estrutura de diretórios"""
        info("📁 Validando estrutura de diretórios...")
        
        required_dirs = [
            'extract',
            'transform', 
            'load',
            'config',
            'utils',
            'data'
        ]
        
        base_path = os.path.dirname(__file__)
        
        for dir_name in required_dirs:
            dir_path = os.path.join(base_path, dir_name)
            if os.path.exists(dir_path):
                info("✅ Diretório {} encontrado", dir_name)
            else:
                self.warnings.append(f"Diretório {dir_name} não encontrado")
    
    def _generate_validation_report(self):
        """Gera relatório de validação"""
        info("📋 === RELATÓRIO DE VALIDAÇÃO ===")
        
        if not self.errors and not self.warnings:
            info("🎉 TODAS AS VALIDAÇÕES PASSARAM!")
            info("✅ Sistema pronto para executar ETL")
        else:
            if self.errors:
                info("❌ ERROS CRÍTICOS ENCONTRADOS:")
                for i, error_msg in enumerate(self.errors, 1):
                    info("   {}. {}", i, error_msg)
            
            if self.warnings:
                info("⚠️ AVISOS:")
                for i, warning_msg in enumerate(self.warnings, 1):
                    info("   {}. {}", i, warning_msg)
            
            if self.errors:
                info("💥 CORRIJA OS ERROS ANTES DE EXECUTAR O ETL")
            else:
                info("✅ Sistema pode executar ETL (com avisos)")

def main():
    """Função principal"""
    try:
        validator = ETLValidator()
        is_valid = validator.validate_all()
        
        if is_valid:
            info("🚀 Sistema validado! Pronto para executar ETL")
            return True
        else:
            error("💥 Validação falhou! Corrija os erros antes de continuar")
            return False
            
    except Exception as e:
        error("💥 Erro durante validação: {}", e)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 