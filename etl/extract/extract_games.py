"""
Script de Extração de Jogos/Categorias - Twitch API
Busca jogos populares e categorias específicas
"""

import sys
import os
import json
from datetime import datetime
from typing import List, Dict, Optional

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Imports dos módulos de configuração
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from twitch_client import TwitchAPIClient
from settings import ETLConfig

class GameExtractor:
    """Extrator de dados de jogos/categorias do Twitch"""
    
    def __init__(self):
        """Inicializa o extrator"""
        self.client = TwitchAPIClient()
        self.extracted_data = []
        self.data_dir = self._ensure_data_dir()
    
    def _ensure_data_dir(self) -> str:
        """Garante que o diretório de dados existe"""
        data_dir = os.path.join(ETLConfig.BASE_DIR, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            info("📁 Diretório de dados criado: {}", data_dir)
        return data_dir
    
    def extract_top_games(self, limit: int = 100) -> List[Dict]:
        """
        Extrai jogos mais populares do Twitch
        
        Args:
            limit: Número máximo de jogos para extrair
            
        Returns:
            Lista de dados de jogos
        """
        info("🏆 Extraindo top {} jogos mais populares...", limit)
        
        try:
            games = []
            extracted = 0
            pagination_cursor = None
            
            while extracted < limit:
                # Calcular quantos buscar nesta iteração
                batch_size = min(100, limit - extracted)  # API limita a 100
                
                info("📦 Buscando lote: {} jogos (total: {}/{})", 
                     batch_size, extracted, limit)
                
                # Buscar jogos populares
                batch_games = self.client.get_top_games(limit=batch_size)
                
                if not batch_games:
                    info("ℹ️ Nenhum jogo retornado, finalizando")
                    break
                
                games.extend(batch_games)
                extracted += len(batch_games)
                
                # Se retornou menos que o solicitado, provavelmente acabaram
                if len(batch_games) < batch_size:
                    info("ℹ️ Menos jogos retornados que solicitado, finalizando")
                    break
            
            self.extracted_data.extend(games)
            info("✅ {} jogos populares extraídos", len(games))
            return games
            
        except Exception as e:
            error("💥 Erro ao extrair jogos populares: {}", e)
            return []
    
    def extract_specific_games(self, game_names: List[str]) -> List[Dict]:
        """
        Extrai jogos específicos por nome
        
        Args:
            game_names: Lista de nomes de jogos para buscar
            
        Returns:
            Lista de dados de jogos
        """
        info("🎯 Extraindo jogos específicos: {}", game_names)
        
        try:
            games = []
            
            # API limita a 100 nomes por request
            batch_size = 100
            for i in range(0, len(game_names), batch_size):
                batch_names = game_names[i:i + batch_size]
                info("📦 Processando lote {}/{} ({} jogos)", 
                     i//batch_size + 1, 
                     (len(game_names) + batch_size - 1)//batch_size, 
                     len(batch_names))
                
                batch_games = self.client.get_games(names=batch_names)
                games.extend(batch_games)
            
            self.extracted_data.extend(games)
            info("✅ {} jogos específicos extraídos", len(games))
            return games
            
        except Exception as e:
            error("💥 Erro ao extrair jogos específicos: {}", e)
            return []
    
    def extract_games_from_streams(self, limit: int = 200) -> List[Dict]:
        """
        Extrai jogos baseado nas streams ativas
        
        Args:
            limit: Número de streams para analisar
            
        Returns:
            Lista de dados de jogos únicos
        """
        info("📺 Extraindo jogos das streams ativas...")
        
        try:
            # Buscar streams ativas
            streams = self.client.get_streams(limit=limit)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            # Extrair game_ids únicos das streams
            game_ids = list(set([
                stream['game_id'] 
                for stream in streams 
                if stream.get('game_id')
            ]))
            
            info("🎮 {} jogos únicos encontrados nas streams", len(game_ids))
            
            if not game_ids:
                error("⚠️ Nenhum game_id encontrado nas streams")
                return []
            
            # Buscar dados completos dos jogos
            games = []
            batch_size = 100  # API Twitch limita a 100 por request
            
            for i in range(0, len(game_ids), batch_size):
                batch_ids = game_ids[i:i + batch_size]
                info("📦 Processando lote {}/{} ({} jogos)", 
                     i//batch_size + 1, 
                     (len(game_ids) + batch_size - 1)//batch_size, 
                     len(batch_ids))
                
                batch_games = self.client.get_games(ids=batch_ids)
                games.extend(batch_games)
            
            self.extracted_data.extend(games)
            info("✅ {} jogos das streams extraídos", len(games))
            return games
            
        except Exception as e:
            error("💥 Erro ao extrair jogos das streams: {}", e)
            return []

    # === NOVO MÉTODO COM PAGINAÇÃO ===
    
    def extract_games_from_streams_with_pagination(self, limit: int = 1000) -> List[Dict]:
        """
        Extrai jogos baseado nas streams ativas COM PAGINAÇÃO
        
        Args:
            limit: Número de streams para analisar
            
        Returns:
            Lista de dados de jogos únicos
        """
        info("📺 Extraindo jogos das streams ativas COM PAGINAÇÃO (até {} streams)...", limit)
        
        try:
            # Buscar streams ativas COM PAGINAÇÃO
            streams = self.client.get_streams(
                limit=limit, 
                use_pagination=True, 
                max_pages=20  # Máximo 20 páginas
            )
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            info("📊 {} streams analisadas para extrair jogos", len(streams))
            
            # Extrair game_ids únicos das streams
            game_ids = list(set([
                stream['game_id'] 
                for stream in streams 
                if stream.get('game_id')
            ]))
            
            info("🎮 {} jogos únicos encontrados nas streams", len(game_ids))
            
            if not game_ids:
                error("⚠️ Nenhum game_id encontrado nas streams")
                return []
            
            # Buscar dados completos dos jogos
            games = []
            batch_size = 100  # API Twitch limita a 100 por request
            
            for i in range(0, len(game_ids), batch_size):
                batch_ids = game_ids[i:i + batch_size]
                info("📦 Processando lote {}/{} ({} jogos)", 
                     i//batch_size + 1, 
                     (len(game_ids) + batch_size - 1)//batch_size, 
                     len(batch_ids))
                
                batch_games = self.client.get_games(ids=batch_ids)
                games.extend(batch_games)
                
                # Log dos jogos encontrados neste lote
                for game in batch_games[:3]:  # Primeiros 3 de cada lote
                    info("🎮 {}", game.get('name', 'Desconhecido'))
            
            self.extracted_data.extend(games)
            info("✅ {} jogos das streams extraídos com paginação", len(games))
            
            # Estatísticas dos jogos mais populares (por aparições em streams)
            game_popularity = {}
            for stream in streams:
                game_id = stream.get('game_id')
                if game_id:
                    game_popularity[game_id] = game_popularity.get(game_id, 0) + 1
            
            # Top 10 jogos mais populares
            top_popular = sorted(game_popularity.items(), key=lambda x: x[1], reverse=True)[:10]
            info("📊 Top 10 jogos mais populares (por streams):")
            for game_id, count in top_popular:
                game_name = next((g['name'] for g in games if g['id'] == game_id), 'Desconhecido')
                info("🏆 {}: {} streams", game_name, count)
            
            return games
            
        except Exception as e:
            error("💥 Erro ao extrair jogos das streams com paginação: {}", e)
            return []
    
    def save_to_file(self, filename: Optional[str] = None) -> str:
        """
        Salva dados extraídos em arquivo JSON
        
        Args:
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho do arquivo salvo
        """
        if not self.extracted_data:
            error("⚠️ Nenhum dado para salvar")
            return ""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"games_extracted_{timestamp}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # Remover duplicatas por ID
            unique_games = {}
            for game in self.extracted_data:
                unique_games[game['id']] = game
            
            final_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'total_games': len(unique_games),
                'games': list(unique_games.values())
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Dados salvos em: {}", filepath)
            info("📊 Total de jogos únicos: {}", len(unique_games))
            return filepath
            
        except Exception as e:
            error("💥 Erro ao salvar arquivo: {}", e)
            return ""
    
    def get_extraction_summary(self) -> Dict:
        """
        Retorna resumo da extração
        
        Returns:
            Dicionário com estatísticas
        """
        if not self.extracted_data:
            return {'total': 0, 'unique': 0}
        
        unique_ids = set(game['id'] for game in self.extracted_data)
        
        # Top jogos por nome
        game_names = [game['name'] for game in self.extracted_data[:10]]
        
        return {
            'total_extracted': len(self.extracted_data),
            'unique_games': len(unique_ids),
            'top_games': game_names,
            'has_box_art': sum(1 for game in self.extracted_data if game.get('box_art_url'))
        }

def main():
    """Função principal de extração"""
    info("🚀 === INICIANDO EXTRAÇÃO DE JOGOS ===")
    
    extractor = GameExtractor()
    
    try:
        # 1. Extrair jogos populares (AUMENTADO)
        info("📝 Etapa 1: Top jogos populares (AUMENTADO)")
        top_games = extractor.extract_top_games(limit=300)  # Aumentado de 100 para 300
        
        # 2. Extrair jogos específicos conhecidos (EXPANDIDO)
        info("📝 Etapa 2: Jogos específicos (EXPANDIDO)")
        known_games = [
            'League of Legends', 'Fortnite', 'Valorant', 'Counter-Strike 2',
            'Grand Theft Auto V', 'Minecraft', 'Apex Legends', 'Call of Duty: Warzone',
            'World of Warcraft', 'Dota 2', 'Overwatch 2', 'Rocket League',
            'Among Us', 'Fall Guys', 'Dead by Daylight', 'PUBG: BATTLEGROUNDS',
            'Genshin Impact', 'Lost Ark', 'Escape from Tarkov', 'FIFA 24',
            'Rainbow Six Siege', 'Destiny 2', 'New World', 'Cyberpunk 2077',
            'The Witcher 3', 'Baldur\'s Gate 3', 'Starfield', 'Diablo IV',
            'Street Fighter 6', 'Mortal Kombat 1', 'Spider-Man 2', 'Hogwarts Legacy',
            'Palworld', 'Helldivers 2', 'The Finals', 'Lethal Company'
        ]
        specific_games = extractor.extract_specific_games(known_games)
        
        # 3. Extrair jogos das streams ativas (COM PAGINAÇÃO)
        info("📝 Etapa 3: Jogos das streams ativas (COM PAGINAÇÃO)")
        stream_games = extractor.extract_games_from_streams_with_pagination(limit=1000)  # Aumentado para 1000
        
        # 4. Salvar dados
        info("📝 Etapa 4: Salvando dados")
        filepath = extractor.save_to_file()
        
        # 5. Resumo final
        summary = extractor.get_extraction_summary()
        info("📊 === RESUMO DA EXTRAÇÃO ===")
        info("Total extraído: {}", summary['total_extracted'])
        info("Jogos únicos: {}", summary['unique_games'])
        info("Com box art: {}", summary['has_box_art'])
        info("Top jogos: {}", ', '.join(summary['top_games'][:5]))
        
        if filepath:
            info("✅ Extração de jogos concluída com sucesso!")
            info("📁 Arquivo salvo: {}", filepath)
        else:
            error("❌ Falha ao salvar dados")
            
    except Exception as e:
        error("💥 Erro na extração: {}", e)

if __name__ == "__main__":
    main() 