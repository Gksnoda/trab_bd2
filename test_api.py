#!/usr/bin/env python3
"""
Script de teste para verificar se a API do Twitch está funcionando
"""

from api.twitch_client import TwitchAPIClient
from logger import info, error
import json

def test_twitch_api():
    """
    Testar conexão e endpoints da API Twitch
    """
    info("🧪 Testando API do Twitch...")
    
    try:
        client = TwitchAPIClient()
        
        # Teste 1: Top Games
        info("📊 Testando endpoint de top games...")
        games = client.get_top_games(first=5)
        info(f"✅ Retornados {len(games)} jogos")
        
        if games:
            info("🎮 Top 3 jogos:")
            for i, game in enumerate(games[:3], 1):
                info(f"   {i}. {game['name']} (ID: {game['id']})")
        
        # Teste 2: Streams ao vivo
        info("📺 Testando endpoint de streams...")
        streams = client.get_streams(first=5)
        info(f"✅ Retornadas {len(streams)} streams")
        
        if streams:
            info("🔴 Streams ao vivo:")
            for stream in streams[:3]:
                info(f"   • {stream['user_name']}: {stream['title'][:50]}... ({stream['viewer_count']} viewers)")
        
        # Teste 3: Buscar usuários específicos
        if streams:
            info("👥 Testando endpoint de usuários...")
            user_ids = [stream['user_id'] for stream in streams[:2]]
            users = client.get_users(user_ids=user_ids)
            info(f"✅ Retornados {len(users)} usuários")
            
            for user in users:
                info(f"   • {user['display_name']} (@{user['login']}) - {user['view_count']:,} views")
        
        # Teste 4: Vídeos de um jogo
        if games:
            game_id = games[0]['id']
            info(f"📹 Testando vídeos do jogo {games[0]['name']}...")
            videos = client.get_videos(game_id=game_id, first=3)
            info(f"✅ Retornados {len(videos)} vídeos")
            
            for video in videos[:2]:
                info(f"   • {video['title'][:50]}... ({video['view_count']:,} views)")
        
        # Teste 5: Clips de um jogo
        if games:
            game_id = games[0]['id']
            info(f"🎬 Testando clips do jogo {games[0]['name']}...")
            clips = client.get_clips(game_id=game_id, first=3)
            info(f"✅ Retornados {len(clips)} clips")
            
            for clip in clips[:2]:
                info(f"   • {clip['title'][:50]}... ({clip['view_count']:,} views)")
        
        info("🎉 Todos os testes da API passaram!")
        return True
        
    except Exception as e:
        error(f"❌ Erro nos testes da API: {e}")
        return False

def test_database_connection():
    """
    Testar conexão com o banco de dados
    """
    info("🗄️ Testando conexão com banco de dados...")
    
    try:
        from database.config import engine
        
        # Testar conexão
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            info("✅ Conexão com PostgreSQL estabelecida!")
            return True
            
    except Exception as e:
        error(f"❌ Erro na conexão com banco: {e}")
        info("💡 Certifique-se de que:")
        info("   • PostgreSQL está rodando")
        info("   • DATABASE_URL está configurada no .env")
        info("   • Banco 'twitch_analytics' existe")
        return False

def main():
    """
    Executar todos os testes
    """
    info("🚀 INICIANDO TESTES DO SISTEMA")
    info("=" * 50)
    
    # Teste 1: API Twitch
    api_ok = test_twitch_api()
    
    print()  # Linha em branco
    
    # Teste 2: Banco de dados
    db_ok = test_database_connection()
    
    print()  # Linha em branco
    
    # Resultado final
    if api_ok and db_ok:
        info("🎉 TODOS OS TESTES PASSARAM!")
        info("✅ Sistema pronto para executar o ETL")
    else:
        error("❌ ALGUNS TESTES FALHARAM")
        if not api_ok:
            error("   • Verificar configuração da API Twitch")
        if not db_ok:
            error("   • Verificar configuração do PostgreSQL")

if __name__ == "__main__":
    main() 