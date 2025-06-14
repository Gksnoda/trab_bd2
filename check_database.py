import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    
    # Verificar se as tabelas existem
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    
    tables = cursor.fetchall()
    print('🗄️ Tabelas encontradas:')
    for table in tables:
        print(f'  • {table[0]}')
    
    print('\n📊 Contagem de registros por tabela:')
    table_names = ['users', 'games', 'streams', 'videos', 'clips']
    
    for table_name in table_names:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
            count = cursor.fetchone()[0]
            print(f'  • {table_name}: {count} registros')
        except:
            print(f'  • {table_name}: tabela não existe')
    
    conn.close()
    print('\n✅ Verificação concluída!')
    
except Exception as e:
    print(f'❌ Erro ao conectar ao banco: {e}') 