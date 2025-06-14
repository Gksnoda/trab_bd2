"""
Script Principal de Transformação - Orquestra todos os transformadores
Processa dados brutos extraídos e prepara para inserção no banco
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, List

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Imports dos transformadores
from transform_users import UserTransformer
from transform_streams import StreamTransformer
from transform_videos import VideoTransformer
from transform_clips import ClipTransformer
from transform_games import GameTransformer

class DataTransformationOrchestrator:
    """Orquestrador principal de transformação de dados"""
    
    def __init__(self):
        """Inicializa o orquestrador"""
        self.transformers = {
            'users': UserTransformer(),
            'streams': StreamTransformer(),
            'videos': VideoTransformer(),
            'clips': ClipTransformer(),
            'games': GameTransformer()
        }
        
        self.data_dir = self._ensure_data_dir()
        self.transformed_data = {}
        self.transformation_stats = {}
    
    def _ensure_data_dir(self) -> str:
        """Garante que o diretório de dados existe"""
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            info("📁 Diretório de dados criado: {}", data_dir)
        return data_dir
    
    def load_extracted_data(self, file_path: str) -> Dict[str, Any]:
        """
        Carrega dados extraídos de arquivo JSON
        
        Args:
            file_path: Caminho para o arquivo de dados extraídos
            
        Returns:
            Dicionário com dados extraídos
        """
        try:
            full_path = os.path.join(self.data_dir, file_path)
            
            if not os.path.exists(full_path):
                error("❌ Arquivo não encontrado: {}", full_path)
                return {}
            
            with open(full_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            info("📂 Dados carregados de: {}", file_path)
            
            # Log resumo dos dados carregados
            if 'summary' in data:
                summary = data['summary']
                info("📊 Resumo dos dados carregados:")
                for key, value in summary.items():
                    info("  • {}: {}", key.replace('total_', '').title(), value)
            
            return data
            
        except Exception as e:
            error("💥 Erro ao carregar dados de {}: {}", file_path, e)
            return {}
    
    def extract_data_by_type(self, extracted_data: Dict[str, Any], data_type: str) -> List[Dict]:
        """
        Extrai dados específicos do arquivo de dados extraídos
        
        Args:
            extracted_data: Dados extraídos completos
            data_type: Tipo de dados a extrair (users, streams, videos, clips)
            
        Returns:
            Lista de dados do tipo especificado
        """
        # Tentar acessar estrutura unificada primeiro
        if 'data' in extracted_data:
            data_section = extracted_data['data']
            if data_type in data_section:
                type_data = data_section[data_type]
                if isinstance(type_data, list):
                    info("📦 Extraídos {} itens de tipo '{}' (estrutura unificada)", len(type_data), data_type)
                    return type_data
        
        # Se não encontrou estrutura unificada, tentar acessar diretamente
        # Mapear nomes de tipos para chaves nos arquivos individuais
        key_mapping = {
            'streams': 'streams',
            'users': 'users', 
            'videos': 'videos',
            'clips': 'clips',
            'games': 'games'
        }
        
        if data_type in key_mapping:
            key = key_mapping[data_type]
            if key in extracted_data:
                type_data = extracted_data[key]
                if isinstance(type_data, list):
                    info("📦 Extraídos {} itens de tipo '{}' (estrutura individual)", len(type_data), data_type)
                    return type_data
        
        # Se não encontrou de forma alguma, retornar lista vazia
        info("⚠️ Nenhum dado encontrado para tipo '{}'", data_type)
        return []
    
    def extract_unique_games(self, extracted_data: Dict[str, Any]) -> List[Dict]:
        """
        Extrai jogos únicos de streams, vídeos e clips
        
        Args:
            extracted_data: Dados extraídos completos
            
        Returns:
            Lista de jogos únicos
        """
        games_dict = {}
        
        # Extrair jogos de streams
        streams = self.extract_data_by_type(extracted_data, 'streams')
        for stream in streams:
            game_id = stream.get('game_id')
            game_name = stream.get('game_name')
            if game_id and game_name:
                games_dict[game_id] = {
                    'id': game_id,
                    'name': game_name
                }
        
        # Extrair jogos de clips (se tiverem game_id)
        clips = self.extract_data_by_type(extracted_data, 'clips')
        for clip in clips:
            game_id = clip.get('game_id')
            if game_id and game_id not in games_dict:
                # Para clips, pode não ter game_name, usar ID como nome temporário
                games_dict[game_id] = {
                    'id': game_id,
                    'name': f"Game_{game_id}"  # Nome temporário
                }
        
        games_list = list(games_dict.values())
        info("🎮 Extraídos {} jogos únicos", len(games_list))
        return games_list
    
    def transform_all_data(self, extracted_data: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Transforma todos os tipos de dados
        
        Args:
            extracted_data: Dados extraídos completos
            
        Returns:
            Dicionário com dados transformados por tipo
        """
        info("🔄 === INICIANDO TRANSFORMAÇÃO COMPLETA ===")
        
        transformed = {}
        
        # 1. Transformar usuários (sem dependências)
        info("👤 Transformando usuários...")
        users_data = self.extract_data_by_type(extracted_data, 'users')
        transformed['users'] = self.transformers['users'].transform_users(users_data)
        self.transformation_stats['users'] = self.transformers['users'].get_transformation_stats()
        
        # 2. Transformar jogos (sem dependências)
        info("🎮 Transformando jogos...")
        games_data = self.extract_unique_games(extracted_data)
        transformed['games'] = self.transformers['games'].transform_games(games_data)
        self.transformation_stats['games'] = self.transformers['games'].get_transformation_stats()
        
        # 3. Transformar streams (depende de users e games)
        info("📺 Transformando streams...")
        streams_data = self.extract_data_by_type(extracted_data, 'streams')
        transformed['streams'] = self.transformers['streams'].transform_streams(streams_data)
        self.transformation_stats['streams'] = self.transformers['streams'].get_transformation_stats()
        
        # 4. Transformar vídeos (depende de users)
        info("🎬 Transformando vídeos...")
        videos_data = self.extract_data_by_type(extracted_data, 'videos')
        transformed['videos'] = self.transformers['videos'].transform_videos(videos_data)
        self.transformation_stats['videos'] = self.transformers['videos'].get_transformation_stats()
        
        # 5. Transformar clips (depende de users e games)
        info("🎭 Transformando clips...")
        clips_data = self.extract_data_by_type(extracted_data, 'clips')
        transformed['clips'] = self.transformers['clips'].transform_clips(clips_data)
        self.transformation_stats['clips'] = self.transformers['clips'].get_transformation_stats()
        
        self.transformed_data = transformed
        
        info("✅ === TRANSFORMAÇÃO COMPLETA FINALIZADA ===")
        return transformed
    
    def validate_all_transformed_data(self) -> bool:
        """
        Valida todos os dados transformados
        
        Returns:
            True se todos válidos, False caso contrário
        """
        info("🔍 === VALIDANDO DADOS TRANSFORMADOS ===")
        
        all_valid = True
        
        for data_type, transformer in self.transformers.items():
            if data_type in self.transformed_data:
                data = self.transformed_data[data_type]
                
                if data_type == 'users':
                    is_valid = transformer.validate_transformed_users(data)
                elif data_type == 'streams':
                    is_valid = transformer.validate_transformed_streams(data)
                elif data_type == 'videos':
                    is_valid = transformer.validate_transformed_videos(data)
                elif data_type == 'clips':
                    is_valid = transformer.validate_transformed_clips(data)
                elif data_type == 'games':
                    is_valid = transformer.validate_transformed_games(data)
                
                if not is_valid:
                    all_valid = False
        
        if all_valid:
            info("✅ Todos os dados transformados são válidos")
        else:
            error("❌ Alguns dados transformados contêm erros")
        
        return all_valid
    
    def save_transformed_data(self, output_filename: str = None) -> str:
        """
        Salva dados transformados em arquivo JSON
        
        Args:
            output_filename: Nome do arquivo de saída (opcional)
            
        Returns:
            Caminho do arquivo salvo
        """
        if not self.transformed_data:
            error("⚠️ Nenhum dado transformado para salvar")
            return ""
        
        # Gerar nome do arquivo se não fornecido
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"transformed_data_{timestamp}.json"
        
        output_path = os.path.join(self.data_dir, output_filename)
        
        try:
            # Preparar dados para salvamento
            output_data = {
                'transformation_info': {
                    'timestamp': datetime.now().isoformat(),
                    'method': 'complete_transformation',
                    'total_processed': sum(stats['processed'] for stats in self.transformation_stats.values())
                },
                'summary': {
                    f'total_{data_type}': len(data) 
                    for data_type, data in self.transformed_data.items()
                },
                'statistics': self.transformation_stats,
                'data': self.transformed_data
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(output_path)
            info("💾 Dados transformados salvos: {} ({:.2f} MB)", 
                 output_filename, file_size / (1024 * 1024))
            
            return output_path
            
        except Exception as e:
            error("💥 Erro ao salvar dados transformados: {}", e)
            return ""
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """Retorna resumo completo da transformação"""
        return {
            'total_types': len(self.transformed_data),
            'data_counts': {
                data_type: len(data) 
                for data_type, data in self.transformed_data.items()
            },
            'statistics': self.transformation_stats
        }

def find_all_extracted_files(data_dir: str) -> Dict[str, str]:
    """
    Encontra arquivos de extração mais recentes por tipo
    
    Args:
        data_dir: Diretório onde estão os arquivos
        
    Returns:
        Dicionário com arquivos encontrados por tipo
    """
    import glob
    
    extracted_files = {}
    
    # Padrões para cada tipo de arquivo
    patterns = {
        'users': "users_extracted_*.json",
        'games': "games_extracted_*.json", 
        'streams': "streams_extracted_*.json",
        'videos': "videos_extracted_*.json",
        'clips': "clips_extracted_*.json"
    }
    
    for data_type, pattern in patterns.items():
        full_pattern = os.path.join(data_dir, pattern)
        files = glob.glob(full_pattern)
        
        if files:
            # Pegar o arquivo mais recente
            latest_file = max(files, key=os.path.getmtime)
            filename = os.path.basename(latest_file)
            extracted_files[data_type] = filename
            info("📂 Arquivo {} encontrado: {}", data_type, filename)
        else:
            info("⚠️ Nenhum arquivo {} encontrado", data_type)
    
    return extracted_files

def load_all_extracted_data(data_dir: str, extracted_files: Dict[str, str]) -> Dict[str, Any]:
    """
    Carrega todos os arquivos de extração e unifica os dados
    
    Args:
        data_dir: Diretório dos arquivos
        extracted_files: Dicionário com arquivos por tipo
        
    Returns:
        Estrutura unificada com todos os dados
    """
    if not extracted_files:
        error("❌ Nenhum arquivo de extração encontrado")
        return {}
    
    unified_data = {
        'extraction_info': {
            'timestamp': datetime.now().isoformat(),
            'method': 'unified_from_individual_files',
            'total_files': len(extracted_files)
        },
        'data': {}
    }
    
    total_records = 0
    
    for data_type, filename in extracted_files.items():
        file_path = os.path.join(data_dir, filename)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
            
            info("📂 Carregando arquivo {}: {}", data_type, filename)
            
            # Extrair dados baseado na estrutura do arquivo
            items = []
            
            if data_type in file_data:
                # Estrutura: {"users": [...], "total_users": X}
                items = file_data[data_type]
            elif 'data' in file_data and data_type in file_data['data']:
                # Estrutura: {"data": {"users": [...]}}
                items = file_data['data'][data_type]
            elif data_type.rstrip('s') in file_data:
                # Tentar singular (ex: "user" ao invés de "users")
                items = file_data[data_type.rstrip('s')]
            elif 'data' in file_data:
                # Tentar pegar 'data' diretamente se for uma lista
                items = file_data['data'] if isinstance(file_data['data'], list) else []
            
            if isinstance(items, list):
                unified_data['data'][data_type] = items
                total_records += len(items)
                info("✅ {} itens de {} carregados", len(items), data_type)
            else:
                error("⚠️ Dados de {} não são uma lista válida", data_type)
                unified_data['data'][data_type] = []
            
        except Exception as e:
            error("💥 Erro ao carregar arquivo {}: {}", filename, e)
            unified_data['data'][data_type] = []
    
    info("📦 Total de {} registros carregados de {} arquivos", total_records, len(extracted_files))
    return unified_data

def main():
    """Função principal de teste"""
    info("🚀 === SCRIPT PRINCIPAL DE TRANSFORMAÇÃO ===")
    
    # Criar orquestrador
    orchestrator = DataTransformationOrchestrator()
    
    # Buscar todos os arquivos de extração
    extracted_files = find_all_extracted_files(orchestrator.data_dir)
    
    if not extracted_files:
        error("❌ Nenhum arquivo de extração encontrado")
        return
    
    # Carregar e unificar todos os dados extraídos
    extracted_data = load_all_extracted_data(orchestrator.data_dir, extracted_files)
    
    if not extracted_data or not extracted_data.get('data'):
        error("❌ Falha ao carregar dados extraídos")
        return
    
    # Transformar todos os dados
    transformed_data = orchestrator.transform_all_data(extracted_data)
    
    # Validar dados transformados
    is_valid = orchestrator.validate_all_transformed_data()
    
    # Salvar dados transformados
    if is_valid:
        output_file = orchestrator.save_transformed_data()
        if output_file:
            summary = orchestrator.get_transformation_summary()
            info("📊 Resumo final: {}", summary)
    
    info("🏁 Transformação concluída!")

if __name__ == "__main__":
    main() 
