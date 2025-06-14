"""
Transformador de Dados de Jogos
Limpa e valida dados de jogos/categorias da API Twitch
"""

import sys
import os
from typing import List, Dict, Any

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da classe base
from base_transformer import BaseTransformer

class GameTransformer(BaseTransformer):
    """Transformador específico para dados de jogos"""
    
    def __init__(self):
        """Inicializa o transformador de jogos"""
        super().__init__()
        
        # Campos obrigatórios para jogos
        self.required_fields = [
            'id',
            'name'
        ]
        
        # Limites de tamanho para strings
        self.string_limits = {
            'name': 100,          # Game name limit
            'box_art_url': 500   # URL limit (longer for images)
        }
    
    def transform_games(self, games_data: List[Dict]) -> List[Dict]:
        """
        Transforma dados brutos de jogos
        
        Args:
            games_data: Lista de dados brutos de jogos
            
        Returns:
            Lista de jogos transformados e validados
        """
        if not games_data:
            info("⚠️ Nenhum dado de jogo para transformar")
            return []
        
        info("🔄 Iniciando transformação de {} jogos...", len(games_data))
        self.stats['processed'] = len(games_data)
        
        # 1. Limpar valores nulos em campos obrigatórios
        cleaned_games = self.clean_null_values(games_data, self.required_fields)
        
        # 2. Transformar cada jogo individualmente
        transformed_games = []
        for game in cleaned_games:
            transformed_game = self._transform_single_game(game)
            if transformed_game:
                transformed_games.append(transformed_game)
        
        # 3. Remover duplicatas baseado no ID
        unique_games = self.remove_duplicates(transformed_games, 'id')
        
        # 4. Log estatísticas finais
        self.log_final_stats('games')
        
        info("✅ Transformação jogos concluída: {} jogos válidos", len(unique_games))
        return unique_games
    
    def _transform_single_game(self, game: Dict) -> Dict:
        """
        Transforma um único jogo
        
        Args:
            game: Dados brutos do jogo
            
        Returns:
            Jogo transformado ou None se inválido
        """
        try:
            transformed = {}
            
            # ID do jogo (obrigatório)
            game_id = self.validate_string(game.get('id'), 'id')
            if not game_id:
                return None
            transformed['id'] = game_id
            
            # Nome do jogo (obrigatório)
            name = self.validate_string(
                game.get('name'), 
                'name', 
                self.string_limits['name']
            )
            if not name:
                return None
            transformed['name'] = name
            
            # URL da box art (opcional mas importante)
            box_art_url = self.validate_string(
                game.get('box_art_url'), 
                'box_art_url', 
                self.string_limits['box_art_url']
            )
            transformed['box_art_url'] = box_art_url
            
            # IGDB ID removido - não está no MER
            
            return transformed
            
        except Exception as e:
            error("💥 Erro ao transformar jogo {}: {}", 
                  game.get('name', 'desconhecido'), e)
            self.stats['validation_errors'] += 1
            return None
    
    def validate_transformed_games(self, games: List[Dict]) -> bool:
        """
        Valida se todos os jogos transformados estão corretos
        
        Args:
            games: Lista de jogos transformados
            
        Returns:
            True se todos válidos, False caso contrário
        """
        if not games:
            return False
        
        info("🔍 Validando {} jogos transformados...", len(games))
        
        valid_count = 0
        for game in games:
            if self._validate_single_game(game):
                valid_count += 1
        
        is_valid = valid_count == len(games)
        
        if is_valid:
            info("✅ Todos os {} jogos são válidos", len(games))
        else:
            error("❌ Apenas {}/{} jogos são válidos", valid_count, len(games))
        
        return is_valid
    
    def _validate_single_game(self, game: Dict) -> bool:
        """Valida um único jogo transformado"""
        required_keys = ['id', 'name']
        
        for key in required_keys:
            if key not in game or not game[key]:
                info("⚠️ Jogo inválido - campo '{}' ausente: {}", 
                       key, game.get('name', 'desconhecido'))
                return False
        
        return True 
