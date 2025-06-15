import json
import os
from pathlib import Path
import sys
from datetime import datetime

# Adicionar o diretório ETL ao path para importar o logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def transform_games():
    """
    Transforma os dados de games removendo campos desnecessários.
    
    Campos removidos:
    - igdb_id
    """
    try:
        # Determinar o diretório base do projeto
        current_dir = Path(__file__).parent
        project_root = current_dir.parent  # ETL/transform -> ETL
        
        # Caminhos dos arquivos
        raw_file_path = project_root / "data" / "raw" / "games.json"
        transformed_dir = project_root / "data" / "transformed"
        transformed_file_path = transformed_dir / "games_transformed.json"
        
        # Criar diretório transformed se não existir
        transformed_dir.mkdir(parents=True, exist_ok=True)
        
        info("Iniciando transformação dos dados de games...")
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
        
        games = raw_data['data']
        info("Total de games encontrados: {}", len(games))
        
        # Campos a serem removidos
        fields_to_remove = [
            'igdb_id'
        ]
        
        info("Campos que serão removidos: {}", fields_to_remove)
        
        # Transformar dados
        transformed_games = []
        for i, game in enumerate(games):
            # Criar uma cópia do game sem os campos indesejados
            transformed_game = {}
            for key, value in game.items():
                if key not in fields_to_remove:
                    transformed_game[key] = value
            
            transformed_games.append(transformed_game)
            
            # Log de progresso a cada 500 games (número menor porque games são menos)
            if (i + 1) % 500 == 0:
                info("Processados {} games...", i + 1)
        
        # Criar estrutura final dos dados transformados
        transformed_data = {
            'data': transformed_games,
            'metadata': {
                'total_games': len(transformed_games),
                'fields_removed': fields_to_remove,
                'transformation_timestamp': datetime.now().isoformat()
            }
        }
        
        # Salvar dados transformados
        info("Salvando dados transformados...")
        with open(transformed_file_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        info("Transformação concluída com sucesso!")
        info("Total de games transformados: {}", len(transformed_games))
        info("Arquivo salvo em: {}", transformed_file_path)
        
        # Mostrar exemplo de um game transformado
        if transformed_games:
            info("Exemplo de game transformado:")
            info("{}", transformed_games[0])
        
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
    info("=== INICIANDO TRANSFORMAÇÃO DE GAMES ===")
    success = transform_games()
    
    if success:
        info("=== TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO ===")
    else:
        error("=== TRANSFORMAÇÃO FALHOU ===")
    
    return success

if __name__ == "__main__":
    main() 