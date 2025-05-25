#!/usr/bin/env python3
"""
Script principal para executar o processo ETL do Twitch Analytics
"""

import sys
import os
from datetime import datetime
from database.config import create_tables, drop_tables
from etl.data_extractor import TwitchDataExtractor
from logger import info, error

def setup_database():
    """
    Configurar o banco de dados (criar tabelas)
    """
    info("🔧 Configurando banco de dados...")
    try:
        create_tables()
        info("✅ Banco de dados configurado com sucesso!")
        return True
    except Exception as e:
        error(f"❌ Erro ao configurar banco: {e}")
        return False

def run_full_etl():
    """
    Executar o processo ETL completo
    """
    info("🚀 Iniciando processo ETL completo...")
    start_time = datetime.now()
    
    try:
        extractor = TwitchDataExtractor()
        
        # 1. Extrair jogos populares
        info("📊 Fase 1: Extraindo jogos populares...")
        games_count = extractor.extract_and_load_games(limit=50)
        
        # 2. Extrair streams ao vivo
        info("🎮 Fase 2: Extraindo streams ao vivo...")
        streams_count = extractor.extract_and_load_streams(limit=100)
        
        # 3. Extrair vídeos dos top 10 jogos
        info("📹 Fase 3: Extraindo vídeos dos jogos populares...")
        videos_count = 0
        
        # Buscar IDs dos top 10 jogos do banco
        from database.config import SessionLocal
        from database.models import Game
        
        db = SessionLocal()
        top_games = db.query(Game).limit(10).all()
        db.close()
        
        for game in top_games:
            try:
                count = extractor.extract_and_load_videos_by_game(game.id, limit=20)
                videos_count += count
                info(f"   📹 {count} vídeos extraídos para {game.name}")
            except Exception as e:
                info(f"   ⚠️ Erro ao extrair vídeos para {game.name}: {e}")
        
        # 4. Extrair clips dos top 5 jogos
        info("🎬 Fase 4: Extraindo clips dos jogos populares...")
        clips_count = 0
        
        for game in top_games[:5]:  # Top 5 jogos apenas
            try:
                count = extractor.extract_and_load_clips_by_game(game.id, limit=30)
                clips_count += count
                info(f"   🎬 {count} clips extraídos para {game.name}")
            except Exception as e:
                info(f"   ⚠️ Erro ao extrair clips para {game.name}: {e}")
        
        # Resumo final
        end_time = datetime.now()
        duration = end_time - start_time
        
        info("🎉 ETL CONCLUÍDO COM SUCESSO!")
        info("=" * 50)
        info(f"📊 Jogos carregados: {games_count}")
        info(f"🎮 Streams carregadas: {streams_count}")
        info(f"📹 Vídeos carregados: {videos_count}")
        info(f"🎬 Clips carregados: {clips_count}")
        info(f"⏱️ Tempo total: {duration}")
        info("=" * 50)
        
        return True
        
    except Exception as e:
        error(f"❌ Erro durante o ETL: {e}")
        return False

def show_database_stats():
    """
    Mostrar estatísticas do banco de dados
    """
    info("📈 Estatísticas do banco de dados:")
    
    try:
        from database.config import SessionLocal
        from database.models import User, Game, Stream, Video, Clip
        
        db = SessionLocal()
        
        users_count = db.query(User).count()
        games_count = db.query(Game).count()
        streams_count = db.query(Stream).count()
        videos_count = db.query(Video).count()
        clips_count = db.query(Clip).count()
        
        db.close()
        
        info("=" * 40)
        info(f"👥 Usuários: {users_count:,}")
        info(f"🎮 Jogos: {games_count:,}")
        info(f"📺 Streams: {streams_count:,}")
        info(f"📹 Vídeos: {videos_count:,}")
        info(f"🎬 Clips: {clips_count:,}")
        info("=" * 40)
        
    except Exception as e:
        error(f"Erro ao obter estatísticas: {e}")

def main():
    """
    Função principal
    """
    info("🎯 TWITCH ANALYTICS ETL")
    info("=" * 50)
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "setup":
            setup_database()
        elif command == "reset":
            info("⚠️ RESETANDO BANCO DE DADOS...")
            drop_tables()
            setup_database()
        elif command == "stats":
            show_database_stats()
        elif command == "etl":
            if not run_full_etl():
                sys.exit(1)
        else:
            print("Comandos disponíveis:")
            print("  setup  - Configurar banco de dados")
            print("  reset  - Resetar banco de dados")
            print("  etl    - Executar ETL completo")
            print("  stats  - Mostrar estatísticas")
    else:
        # Executar ETL completo por padrão
        if not setup_database():
            sys.exit(1)
        
        if not run_full_etl():
            sys.exit(1)
        
        show_database_stats()

if __name__ == "__main__":
    main() 