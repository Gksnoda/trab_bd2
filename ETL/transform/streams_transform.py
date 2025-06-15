import json
import os
from pathlib import Path
import sys
from datetime import datetime

# Adicionar o diretório ETL ao path para importar o logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def transform_streams():
    """
    Transforma os dados de streams removendo campos desnecessários.
    """
    try:
        # Determinar o diretório base do projeto
        current_dir = Path(__file__).parent
        project_root = current_dir.parent  # ETL/transform -> ETL
        
        # Caminhos dos arquivos
        raw_file_path = project_root / "data" / "raw" / "streams.json"
        transformed_dir = project_root / "data" / "transformed"
        transformed_file_path = transformed_dir / "streams_transformed.json"
        
        # Criar diretório transformed se não existir
        transformed_dir.mkdir(parents=True, exist_ok=True)
        
        info("Iniciando transformação dos dados de streams...")
        info("Arquivo de origem: {}", raw_file_path)
        info("Arquivo de destino: {}", transformed_file_path)
        
        # Verificar se o arquivo raw existe
        if not raw_file_path.exists():
            error("Arquivo de dados brutos não encontrado: {}", raw_file_path)
            return False
        
        # Ler dados brutos
        info("Lendo dados brutos...")
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Verificar estrutura dos dados
        if 'data' not in raw_data:
            error("Estrutura de dados inválida. Campo 'data' não encontrado.")
            return False
        
        streams = raw_data['data']
        info("Total de streams encontradas: {}", len(streams))
        
        # Campos a serem removidos
        fields_to_remove = [
            'user_login',
            'type',
            'tag_ids',
            'is_mature',
            'game_name',
            'user_name',
        ]
        
        info("Campos que serão removidos: {}", fields_to_remove)
        
        # Transformar dados
        transformed_streams = []
        streams_removed_empty_game_id = 0
        
        for i, stream in enumerate(streams):
            # Filtrar streams com game_id vazio
            game_id = stream.get('game_id', '')
            if not game_id or game_id.strip() == '':
                streams_removed_empty_game_id += 1
                continue
            
            # Criar uma cópia do stream sem os campos indesejados
            transformed_stream = {}
            for key, value in stream.items():
                if key not in fields_to_remove:
                    transformed_stream[key] = value
            
            transformed_streams.append(transformed_stream)
            
            # Log de progresso a cada 1000 streams
            if (i + 1) % 1000 == 0:
                info("Processadas {} streams...", i + 1)
        
        info("Streams removidas por game_id vazio: {}", streams_removed_empty_game_id)
        info("Streams mantidas após filtros: {}", len(transformed_streams))
        
        # Criar estrutura final dos dados transformados
        transformed_data = {
            'data': transformed_streams,
            'metadata': {
                'total_streams_original': len(streams),
                'total_streams_transformed': len(transformed_streams),
                'streams_removed_empty_game_id': streams_removed_empty_game_id,
                'fields_removed': fields_to_remove,
                'filters_applied': [
                    'Removidas streams com game_id vazio'
                ],
                'transformation_timestamp': datetime.now().isoformat()
            }
        }
        
        # Salvar dados transformados
        info("Salvando dados transformados...")
        with open(transformed_file_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        info("Transformação concluída com sucesso!")
        info("Total de streams transformadas: {}", len(transformed_streams))
        info("Arquivo salvo em: {}", transformed_file_path)
        
        # Mostrar exemplo de um stream transformado
        if transformed_streams:
            info("Exemplo de stream transformada:")
            info("{}", transformed_streams[0])
        
        return True
        
    except FileNotFoundError as e:
        error("Arquivo não encontrado: {}", str(e))
        return False
    except json.JSONDecodeError as e:
        error("Erro ao decodificar JSON: {}", str(e))
        return False
    except Exception as e:
        error("Erro inesperado durante a transformação: {}", str(e))
        return False

def main():
    """Função principal para executar a transformação"""
    info("=== INICIANDO TRANSFORMAÇÃO DE STREAMS ===")
    success = transform_streams()
    
    if success:
        info("=== TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO ===")
    else:
        error("=== TRANSFORMAÇÃO FALHOU ===")
    
    return success

if __name__ == "__main__":
    main() 