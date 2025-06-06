#!/usr/bin/env python3
"""
Script simples para testar conexão com PostgreSQL
"""
import os
from dotenv import load_dotenv
import psycopg2

# Carregar variáveis de ambiente
load_dotenv()

def test_connection():
    """Testar conexão simples com PostgreSQL"""
    
    # Pegar DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    print(f"DATABASE_URL found: {database_url}")
    
    if not database_url:
        print("❌ DATABASE_URL não encontrada no .env")
        return
    
    try:
        # Tentar conectar usando psycopg2 diretamente
        print("🔍 Testando conexão com psycopg2...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Conexão bem-sucedida!")
        print(f"📊 PostgreSQL version: {version[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        print(f"❌ Tipo do erro: {type(e)}")
        
        # Tentar diagnosticar o problema
        print("\n🔍 Diagnóstico:")
        try:
            # Tentar decodificar a URL
            decoded = database_url.encode('utf-8').decode('utf-8')
            print(f"✅ URL decodifica corretamente: {decoded}")
        except Exception as decode_error:
            print(f"❌ Erro na decodificação da URL: {decode_error}")

if __name__ == "__main__":
    test_connection() 