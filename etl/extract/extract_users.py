"""
Script de Extração de Usuários - Twitch API
Busca streamers populares e usuários específicos
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

class UserExtractor:
    """Extrator de dados de usuários do Twitch"""
    
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
    
    def extract_popular_streamers(self, limit: int = 100) -> List[Dict]:
        """
        Extrai streamers populares baseado nas streams ativas
        
        Args:
            limit: Número máximo de streamers para extrair
            
        Returns:
            Lista de dados de usuários
        """
        info("🔥 Iniciando extração de streamers populares...")
        
        try:
            # Primeiro buscar streams populares para obter user_ids
            streams = self.client.get_streams(limit=limit)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada")
                return []
            
            # Extrair user_ids únicos das streams
            user_ids = list(set([stream['user_id'] for stream in streams]))
            info("👥 {} streamers únicos encontrados nas streams", len(user_ids))
            
            # Buscar dados completos dos usuários
            users = []
            batch_size = 100  # API Twitch limita a 100 por request
            
            for i in range(0, len(user_ids), batch_size):
                batch_ids = user_ids[i:i + batch_size]
                info("📦 Processando lote {}/{} ({} usuários)", 
                     i//batch_size + 1, 
                     (len(user_ids) + batch_size - 1)//batch_size, 
                     len(batch_ids))
                
                batch_users = self.client.get_users(ids=batch_ids)
                users.extend(batch_users)
            
            self.extracted_data.extend(users)
            info("✅ {} streamers populares extraídos", len(users))
            return users
            
        except Exception as e:
            error("💥 Erro ao extrair streamers populares: {}", e)
            return []
    
    def extract_specific_users(self, usernames: List[str]) -> List[Dict]:
        """
        Extrai usuários específicos por username
        
        Args:
            usernames: Lista de usernames para buscar
            
        Returns:
            Lista de dados de usuários
        """
        info("🎯 Extraindo usuários específicos: {}", usernames)
        
        try:
            users = self.client.get_users(logins=usernames)
            self.extracted_data.extend(users)
            info("✅ {} usuários específicos extraídos", len(users))
            return users
            
        except Exception as e:
            error("💥 Erro ao extrair usuários específicos: {}", e)
            return []
    
    def extract_top_streamers_by_category(self, game_id: str, limit: int = 50) -> List[Dict]:
        """
        Extrai top streamers de uma categoria específica
        
        Args:
            game_id: ID do jogo/categoria
            limit: Número de streamers para buscar
            
        Returns:
            Lista de dados de usuários
        """
        info("🎮 Extraindo top streamers da categoria: {}", game_id)
        
        try:
            # Buscar streams da categoria
            streams = self.client.get_streams(game_ids=[game_id], limit=limit)
            
            if not streams:
                error("⚠️ Nenhuma stream encontrada para categoria {}", game_id)
                return []
            
            # Obter user_ids únicos
            user_ids = list(set([stream['user_id'] for stream in streams]))
            
            # Buscar dados dos usuários
            users = self.client.get_users(ids=user_ids)
            self.extracted_data.extend(users)
            
            info("✅ {} streamers da categoria {} extraídos", len(users), game_id)
            return users
            
        except Exception as e:
            error("💥 Erro ao extrair streamers por categoria: {}", e)
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
            filename = f"users_extracted_{timestamp}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            # Remover duplicatas por ID
            unique_users = {}
            for user in self.extracted_data:
                unique_users[user['id']] = user
            
            final_data = {
                'extraction_timestamp': datetime.now().isoformat(),
                'total_users': len(unique_users),
                'users': list(unique_users.values())
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            
            info("💾 Dados salvos em: {}", filepath)
            info("📊 Total de usuários únicos: {}", len(unique_users))
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
        
        unique_ids = set(user['id'] for user in self.extracted_data)
        
        return {
            'total_extracted': len(self.extracted_data),
            'unique_users': len(unique_ids),
            'broadcaster_types': self._count_by_field('broadcaster_type'),
            'user_types': self._count_by_field('type'),
            'sample_users': [user['display_name'] for user in self.extracted_data[:5]]
        }
    
    def _count_by_field(self, field: str) -> Dict:
        """Conta valores únicos de um campo"""
        counts = {}
        for user in self.extracted_data:
            value = user.get(field, 'unknown')
            counts[value] = counts.get(value, 0) + 1
        return counts

def main():
    """Função principal de extração"""
    info("🚀 === INICIANDO EXTRAÇÃO DE USUÁRIOS ===")
    
    extractor = UserExtractor()
    
    try:
        # 1. Extrair streamers populares
        info("📝 Etapa 1: Streamers populares")
        popular = extractor.extract_popular_streamers(limit=100)
        
        # 2. Extrair usuários específicos conhecidos
        info("📝 Etapa 2: Usuários específicos")
        known_users = [
            'twitchdev', 'ninja', 'pokimane', 'shroud', 'xqc',
            'summit1g', 'sodapoppin', 'lirik', 'timthetatman', 'drdisrespect'
        ]
        specific = extractor.extract_specific_users(known_users)
        
        # 3. Salvar dados
        info("📝 Etapa 3: Salvando dados")
        filepath = extractor.save_to_file()
        
        # 4. Resumo final
        summary = extractor.get_extraction_summary()
        info("📊 === RESUMO DA EXTRAÇÃO ===")
        info("Total extraído: {}", summary['total_extracted'])
        info("Usuários únicos: {}", summary['unique_users'])
        info("Tipos de broadcaster: {}", summary['broadcaster_types'])
        info("Exemplos: {}", ', '.join(summary['sample_users']))
        
        if filepath:
            info("✅ Extração de usuários concluída com sucesso!")
            info("📁 Arquivo salvo: {}", filepath)
        else:
            error("❌ Falha ao salvar dados")
            
    except Exception as e:
        error("💥 Erro na extração: {}", e)

if __name__ == "__main__":
    main() 