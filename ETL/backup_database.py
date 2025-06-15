"""
Script para fazer backup do banco PostgreSQL
Cria um dump completo e copia para o sistema de arquivos do Windows
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Adicionar o path para usar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), 'etl', 'utils'))
from logger import info, error

def create_backup():
    """Cria backup completo do banco PostgreSQL"""
    try:
        info("🗄️ === INICIANDO BACKUP DO BANCO DE DADOS ===")
        
        # Configurações do banco (usando as mesmas do projeto)
        DB_CONFIG = {
            'host': 'localhost',
            'port': '5432',
            'database': 'twitch_analytics',
            'username': 'postgres'
        }
        
        # Criar nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        custom_backup_filename = f"twitch_analytics_backup_{timestamp}.backup"
        
        # Diretório no WSL para salvar temporariamente
        wsl_backup_dir = "/tmp/backups"
        os.makedirs(wsl_backup_dir, exist_ok=True)
        wsl_backup_path = os.path.join(wsl_backup_dir, custom_backup_filename)
        
        # Diretório no Windows (acessível via /mnt/c/)
        windows_backup_dir = "/mnt/c/Database_Backups"
        os.makedirs(windows_backup_dir, exist_ok=True)
        windows_backup_path = os.path.join(windows_backup_dir, custom_backup_filename)
        
        info("📂 Diretórios de backup:")
        info("   • WSL (temporário): {}", wsl_backup_path)
        info("   • Windows: C:\\Database_Backups\\{}", custom_backup_filename)
        
        # Comando pg_dump
        pg_dump_cmd = [
            'pg_dump',
            f'--host={DB_CONFIG["host"]}',
            f'--port={DB_CONFIG["port"]}',
            f'--username={DB_CONFIG["username"]}',
            '--format=custom',  # Formato personalizado (recomendado)
            '--verbose',
            '--clean',  # Inclui comandos DROP
            '--create', # Inclui comando CREATE DATABASE
            '--if-exists',  # Usar IF EXISTS nos comandos DROP
            f'--file={wsl_backup_path}',
            DB_CONFIG['database']
        ]
        
        info("🔄 Executando pg_dump...")
        info("   Comando: {}", ' '.join(pg_dump_cmd))
        
        # Definir senha via variável de ambiente
        env = os.environ.copy()
        env['PGPASSWORD'] = 'admin'  # A senha do postgres
        
        # Executar pg_dump
        result = subprocess.run(
            pg_dump_cmd,
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode != 0:
            error("💥 Erro ao executar pg_dump:")
            error("   STDERR: {}", result.stderr)
            return False
        
        # Verificar se o arquivo foi criado
        if not os.path.exists(wsl_backup_path):
            error("💥 Arquivo de backup não foi criado: {}", wsl_backup_path)
            return False
        
        # Obter tamanho do arquivo
        file_size = os.path.getsize(wsl_backup_path)
        file_size_mb = file_size / (1024 * 1024)
        
        info("✅ Backup criado com sucesso!")
        info("   • Arquivo: {}", wsl_backup_path)
        info("   • Tamanho: {} MB", f"{file_size_mb:.2f}")
        
        # Copiar para o Windows
        info("📋 Copiando para o Windows...")
        try:
            subprocess.run(['cp', wsl_backup_path, windows_backup_path], check=True)
            info("✅ Backup copiado para: C:\\Database_Backups\\{}", custom_backup_filename)
            
            # Remover arquivo temporário do WSL
            os.remove(wsl_backup_path)
            info("🧹 Arquivo temporário removido do WSL")
            
        except subprocess.CalledProcessError as e:
            error("💥 Erro ao copiar para Windows: {}", e)
            return False
        
        # Criar também um backup em formato SQL texto para facilitar visualização
        info("📝 Criando backup adicional em formato SQL texto...")
        
        sql_backup_filename = f"twitch_analytics_backup_{timestamp}.sql"
        windows_sql_path = os.path.join(windows_backup_dir, sql_backup_filename)
        wsl_sql_path = os.path.join(wsl_backup_dir, sql_backup_filename)
        
        pg_dump_sql_cmd = [
            'pg_dump',
            f'--host={DB_CONFIG["host"]}',
            f'--port={DB_CONFIG["port"]}',
            f'--username={DB_CONFIG["username"]}',
            '--format=plain',  # Formato SQL texto
            '--clean',
            '--create',
            '--if-exists',
            f'--file={wsl_sql_path}',
            DB_CONFIG['database']
        ]
        
        result = subprocess.run(
            pg_dump_sql_cmd,
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode == 0:
            subprocess.run(['cp', wsl_sql_path, windows_sql_path], check=True)
            os.remove(wsl_sql_path)
            
            sql_size = os.path.getsize(windows_sql_path)
            sql_size_mb = sql_size / (1024 * 1024)
            info("✅ Backup SQL texto criado: {} MB", f"{sql_size_mb:.2f}")
        
        info("🎉 === BACKUP CONCLUÍDO COM SUCESSO ===")
        info("📂 Arquivos disponíveis em C:\\Database_Backups\\:")
        info("   • {} (formato binário - para restauração)", custom_backup_filename)
        info("   • {} (formato texto - para visualização)", sql_backup_filename)
        
        return True
        
    except Exception as e:
        error("💥 Erro crítico durante backup: {}", e)
        return False

def restore_backup(backup_file: str):
    """
    Restaura um backup do banco
    
    Args:
        backup_file: Caminho para o arquivo de backup (.custom)
    """
    try:
        info("🔄 === INICIANDO RESTAURAÇÃO DO BANCO ===")
        
        if not os.path.exists(backup_file):
            error("💥 Arquivo de backup não encontrado: {}", backup_file)
            return False
        
        DB_CONFIG = {
            'host': 'localhost',
            'port': '5432',
            'database': 'twitch_analytics',
            'username': 'postgres'
        }
        
        # Comando pg_restore
        pg_restore_cmd = [
            'pg_restore',
            f'--host={DB_CONFIG["host"]}',
            f'--port={DB_CONFIG["port"]}',
            f'--username={DB_CONFIG["username"]}',
            '--verbose',
            '--clean',
            '--create',
            '--if-exists',
            f'--dbname=postgres',  # Conectar ao banco postgres para criar o banco
            backup_file
        ]
        
        info("🔄 Executando pg_restore...")
        
        env = os.environ.copy()
        env['PGPASSWORD'] = 'admin'
        
        result = subprocess.run(
            pg_restore_cmd,
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode != 0:
            error("💥 Erro ao restaurar backup:")
            error("   STDERR: {}", result.stderr)
            return False
        
        info("✅ Backup restaurado com sucesso!")
        return True
        
    except Exception as e:
        error("💥 Erro durante restauração: {}", e)
        return False

def main():
    """Função principal"""
    if len(sys.argv) > 1 and sys.argv[1] == 'restore':
        if len(sys.argv) < 3:
            error("💥 Uso: python backup_database.py restore <caminho_do_backup>")
            return
        
        backup_file = sys.argv[2]
        restore_backup(backup_file)
    else:
        create_backup()

if __name__ == "__main__":
    main() 