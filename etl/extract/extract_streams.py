"""
Script de Extração de Streams - Twitch API
Busca streams ao vivo por categoria, linguagem e popularidade
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

class StreamExtractor:
    """Extrator de dados de streams do Twitch"""
    
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
    
    def extract_top_streams(self, limit: int = 200) -> List[Dict]:
        """
        Extrai streams mais populares (por viewer count)
        
        Args:
            limit: Número máximo de streams para extrair
            
        Returns:
            Lista de dados de streams
        """
        info("🔥 Extraindo top {} streams mais populares...", limit)
        
        try:
            streams = self.client.get_streams(limit=limit)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            # Ordenar por viewer_count (descendente)
            streams.sort(key=lambda x: x.get('viewer_count', 0), reverse=True)
            
            self.extracted_data.extend(streams)
            info("✅ {} streams populares extraídas", len(streams))
            
            # Log das top 5 streams
            top_5 = streams[:5]
            for i, stream in enumerate(top_5, 1):
                info("{}º - {} viewers: {} ({})", 
                     i, stream.get('viewer_count', 0), 
                     stream.get('title', 'Sem título')[:50],
                     stream.get('user_name', 'Desconhecido'))
            
            return streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams populares: {}", e)
            return []
    
    def extract_streams_by_game(self, game_ids: List[str], limit: int = 100) -> List[Dict]:
        """
        Extrai streams de jogos específicos
        
        Args:
            game_ids: Lista de IDs de jogos
            limit: Número máximo de streams por jogo
            
        Returns:
            Lista de dados de streams
        """
        info("🎮 Extraindo streams de {} jogos específicos...", len(game_ids))
        
        try:
            all_streams = []
            
            for game_id in game_ids:
                info("📺 Buscando streams do jogo ID: {}", game_id)
                
                streams = self.client.get_streams(
                    game_ids=[game_id], 
                    limit=limit
                )
                
                if streams:
                    all_streams.extend(streams)
                    info("✅ {} streams encontradas para jogo {}", len(streams), game_id)
                else:
                    info("ℹ️ Nenhuma stream encontrada para jogo {}", game_id)
            
            self.extracted_data.extend(all_streams)
            info("✅ {} streams por jogo extraídas no total", len(all_streams))
            return all_streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams por jogo: {}", e)
            return []
    
    def extract_streams_by_language(self, languages: List[str], limit: int = 50) -> List[Dict]:
        """
        Extrai streams por linguagem
        
        Args:
            languages: Lista de códigos de linguagem (ex: ['en', 'pt', 'es'])
            limit: Número máximo de streams por linguagem
            
        Returns:
            Lista de dados de streams
        """
        info("🌍 Extraindo streams em {} linguagens...", len(languages))
        
        try:
            all_streams = []
            
            for lang in languages:
                info("🗣️ Buscando streams em linguagem: {}", lang)
                
                # Buscar streams gerais e filtrar por linguagem
                streams = self.client.get_streams(limit=limit * 2)  # Buscar mais para filtrar
                
                if streams:
                    # Filtrar por linguagem
                    lang_streams = [
                        stream for stream in streams 
                        if stream.get('language') == lang
                    ][:limit]
                    
                    all_streams.extend(lang_streams)
                    info("✅ {} streams encontradas em {}", len(lang_streams), lang)
                else:
                    info("ℹ️ Nenhuma stream encontrada em {}", lang)
            
            self.extracted_data.extend(all_streams)
            info("✅ {} streams por linguagem extraídas no total", len(all_streams))
            return all_streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams por linguagem: {}", e)
            return []
    
    def extract_streams_by_users(self, user_ids: List[str]) -> List[Dict]:
        """
        Extrai streams de usuários específicos
        
        Args:
            user_ids: Lista de IDs de usuários
            
        Returns:
            Lista de dados de streams
        """
        info("👤 Extraindo streams de {} usuários específicos...", len(user_ids))
        
        try:
            streams = self.client.get_streams(user_ids=user_ids)
            
            if streams:
                self.extracted_data.extend(streams)
                info("✅ {} streams de usuários específicos extraídas", len(streams))
                
                # Log dos streamers ativos
                for stream in streams:
                    info("📺 Stream ativa: {} - {} viewers", 
                         stream.get('user_name', 'Desconhecido'),
                         stream.get('viewer_count', 0))
            else:
                info("ℹ️ Nenhuma stream ativa dos usuários especificados")
            
            return streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams de usuários: {}", e)
            return []

    # === NOVOS MÉTODOS COM PAGINAÇÃO ===
    
    def extract_top_streams_with_pagination(self, limit: int = 2000, max_pages: int = 20) -> List[Dict]:
        """
        Extrai streams mais populares usando paginação
        
        Args:
            limit: Número máximo de streams para extrair
            max_pages: Número máximo de páginas
            
        Returns:
            Lista de dados de streams
        """
        info("🔥 Extraindo top {} streams com PAGINAÇÃO (máx {} páginas)...", limit, max_pages)
        
        try:
            streams = self.client.get_streams(
                limit=limit, 
                use_pagination=True, 
                max_pages=max_pages
            )
            
            if streams:
                # Ordenar por viewer_count (descendente)
                streams.sort(key=lambda x: x.get('viewer_count', 0), reverse=True)
                
                self.extracted_data.extend(streams)
                info("✅ {} streams populares extraídas com paginação", len(streams))
                
                # Log das top 10 streams
                top_10 = streams[:10]
                for i, stream in enumerate(top_10, 1):
                    info("{}º - {} viewers: {} ({})", 
                         i, stream.get('viewer_count', 0), 
                         stream.get('title', 'Sem título')[:50],
                         stream.get('user_name', 'Desconhecido'))
            
            return streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams com paginação: {}", e)
            return []
    
    def extract_streams_by_game_with_pagination(self, game_ids: List[str], limit: int = 300, 
                                              max_pages: int = 15) -> List[Dict]:
        """
        Extrai streams de jogos específicos usando paginação
        
        Args:
            game_ids: Lista de IDs de jogos
            limit: Número máximo de streams por jogo
            max_pages: Número máximo de páginas por jogo
            
        Returns:
            Lista de dados de streams
        """
        info("🎮 Extraindo streams de {} jogos com PAGINAÇÃO...", len(game_ids))
        
        try:
            all_streams = []
            
            for game_id in game_ids:
                info("📺 Buscando streams do jogo ID: {} (com paginação)", game_id)
                
                streams = self.client.get_streams(
                    game_ids=[game_id], 
                    limit=limit,
                    use_pagination=True,
                    max_pages=max_pages
                )
                
                if streams:
                    all_streams.extend(streams)
                    info("✅ {} streams encontradas para jogo {} (paginação)", len(streams), game_id)
                else:
                    info("ℹ️ Nenhuma stream encontrada para jogo {}", game_id)
            
            self.extracted_data.extend(all_streams)
            info("✅ {} streams por jogo extraídas no total (com paginação)", len(all_streams))
            return all_streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams por jogo com paginação: {}", e)
            return []
    
    def extract_streams_by_language_with_pagination(self, languages: List[str], limit: int = 200, 
                                                   max_pages: int = 10) -> List[Dict]:
        """
        Extrai streams por linguagem usando paginação
        
        Args:
            languages: Lista de códigos de linguagem
            limit: Número máximo de streams por linguagem
            max_pages: Número máximo de páginas por busca
            
        Returns:
            Lista de dados de streams
        """
        info("🌍 Extraindo streams em {} linguagens com PAGINAÇÃO...", len(languages))
        
        try:
            all_streams = []
            
            for lang in languages:
                info("🗣️ Buscando streams em linguagem: {} (com paginação)", lang)
                
                # Buscar streams gerais com paginação e filtrar por linguagem
                streams = self.client.get_streams(
                    limit=limit * 3,  # Buscar mais para compensar filtro
                    use_pagination=True,
                    max_pages=max_pages
                )
                
                if streams:
                    # Filtrar por linguagem
                    lang_streams = [
                        stream for stream in streams 
                        if stream.get('language') == lang
                    ][:limit]
                    
                    all_streams.extend(lang_streams)
                    info("✅ {} streams encontradas em {} (após filtro)", len(lang_streams), lang)
                else:
                    info("ℹ️ Nenhuma stream encontrada em {}", lang)
            
            self.extracted_data.extend(all_streams)
            info("✅ {} streams por linguagem extraídas no total (com paginação)", len(all_streams))
            return all_streams
            
        except Exception as e:
            error("💥 Erro ao extrair streams por linguagem com paginação: {}", e)
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
            filename = f"streams_extracted_{timestamp}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # Remover duplicatas por ID
            unique_streams = {}
            for stream in self.extracted_data:
                unique_streams[stream['id']] = stream
            
            final_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'total_streams': len(unique_streams),
                'streams': list(unique_streams.values())
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Dados salvos em: {}", filepath)
            info("📊 Total de streams únicas: {}", len(unique_streams))
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
        
        unique_ids = set(stream['id'] for stream in self.extracted_data)
        total_viewers = sum(stream.get('viewer_count', 0) for stream in self.extracted_data)
        
        # Estatísticas por linguagem
        languages = {}
        for stream in self.extracted_data:
            lang = stream.get('language', 'unknown')
            languages[lang] = languages.get(lang, 0) + 1
        
        # Estatísticas por jogo
        games = {}
        for stream in self.extracted_data:
            game_name = stream.get('game_name', 'unknown')
            games[game_name] = games.get(game_name, 0) + 1
        
        # Top streamers por viewer count
        top_streamers = sorted(
            self.extracted_data, 
            key=lambda x: x.get('viewer_count', 0), 
            reverse=True
        )[:5]
        
        return {
            'total_extracted': len(self.extracted_data),
            'unique_streams': len(unique_ids),
            'total_viewers': total_viewers,
            'avg_viewers': total_viewers // len(self.extracted_data) if self.extracted_data else 0,
            'languages': languages,
            'top_games': dict(list(games.items())[:5]),
            'top_streamers': [
                f"{s.get('user_name', 'N/A')} ({s.get('viewer_count', 0)} viewers)"
                for s in top_streamers
            ]
        }

def main():
    """Função principal de extração"""
    info("🚀 === INICIANDO EXTRAÇÃO DE STREAMS ===")
    
    extractor = StreamExtractor()
    
    try:
        # 1. Extrair streams populares gerais COM PAGINAÇÃO
        info("📝 Etapa 1: Streams mais populares (COM PAGINAÇÃO)")
        top_streams = extractor.extract_top_streams_with_pagination(limit=2000, max_pages=20)
        
        # 2. Extrair streams por linguagem COM PAGINAÇÃO
        info("📝 Etapa 2: Streams por linguagem (COM PAGINAÇÃO)")
        languages = ['en', 'pt', 'es', 'fr', 'de', 'ru', 'ja', 'ko', 'it', 'zh', 'tr', 'ar']
        lang_streams = extractor.extract_streams_by_language_with_pagination(languages, limit=200, max_pages=10)
        
        # 3. Extrair streams de jogos populares COM PAGINAÇÃO
        info("📝 Etapa 3: Streams de jogos específicos (COM PAGINAÇÃO)")
        # IDs de jogos populares expandida
        popular_game_ids = [
            '21779',    # League of Legends
            '33214',    # Fortnite
            '516575',   # Valorant
            '32982',    # Grand Theft Auto V
            '27471',    # Minecraft
            '511224',   # Apex Legends
            '18122',    # World of Warcraft
            '29595',    # Dota 2
            '138585',   # Hearthstone
            '493057',   # PUBG
        ]
        game_streams = extractor.extract_streams_by_game_with_pagination(popular_game_ids, limit=300, max_pages=15)
        
        # 4. Salvar dados
        info("📝 Etapa 4: Salvando dados")
        filepath = extractor.save_to_file()
        
        # 5. Resumo final
        summary = extractor.get_extraction_summary()
        info("📊 === RESUMO DA EXTRAÇÃO ===")
        info("Total extraído: {}", summary['total_extracted'])
        info("Streams únicas: {}", summary['unique_streams'])
        info("Total de viewers: {:,}", summary['total_viewers'])
        info("Média de viewers: {}", summary['avg_viewers'])
        info("Linguagens: {}", summary['languages'])
        info("Top streamers: {}", ', '.join(summary['top_streamers'][:3]))
        
        if filepath:
            info("✅ Extração de streams concluída com sucesso!")
            info("📁 Arquivo salvo: {}", filepath)
        else:
            error("❌ Falha ao salvar dados")
            
    except Exception as e:
        error("💥 Erro na extração: {}", e)

if __name__ == "__main__":
    main() 