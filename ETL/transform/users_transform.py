import json
import os
from pathlib import Path
import sys
from datetime import datetime

# Adicionar o diretório ETL ao path para importar o logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def transform_users():
    """
    Transforma os dados de users removendo campos desnecessários.
    
    Campos removidos:
    - login
    - type
    - offline_image_url
    - view_count
    
    Transformações:
    - created_at: mantém apenas a data, remove o horário
    """
    try:
        # Determinar o diretório base do projeto
        current_dir = Path(__file__).parent
        project_root = current_dir.parent  # ETL/transform -> ETL
        
        # Caminhos dos arquivos
        raw_file_path = project_root / "data" / "raw" / "users.json"
        transformed_dir = project_root / "data" / "transformed"
        transformed_file_path = transformed_dir / "users_transformed.json"
        
        # Criar diretório transformed se não existir
        transformed_dir.mkdir(parents=True, exist_ok=True)
        
        info("Iniciando transformação dos dados de users...")
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
        
        users = raw_data['data']
        info("Total de users encontrados: {}", len(users))
        
        # Campos a serem removidos
        fields_to_remove = [
            'login',
            'type',
            'offline_image_url',
            'view_count'
        ]
        
        info("Campos que serão removidos: {}", fields_to_remove)
        
        # Transformar dados
        transformed_users = []
        for i, user in enumerate(users):
            # Criar uma cópia do user sem os campos indesejados
            transformed_user = {}
            for key, value in user.items():
                if key not in fields_to_remove:
                    # Transformar created_at para conter apenas a data
                    if key == 'created_at' and value:
                        try:
                            # Parse do timestamp ISO 8601 e extração apenas da data
                            date_only = datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat()
                            transformed_user[key] = date_only
                        except (ValueError, AttributeError) as e:
                            error("Erro ao processar created_at para user {}: {}", user.get('id', 'unknown'), str(e))
                            # Manter o valor original em caso de erro
                            transformed_user[key] = value
                    else:
                        transformed_user[key] = value
            
            transformed_users.append(transformed_user)
            
            # Log de progresso a cada 1000 users
            if (i + 1) % 1000 == 0:
                info("Processados {} users...", i + 1)
        
        # Criar estrutura final dos dados transformados
        transformed_data = {
            'data': transformed_users,
            'metadata': {
                'total_users': len(transformed_users),
                'fields_removed': fields_to_remove,
                'transformations_applied': [
                    'created_at: convertido para formato apenas de data (YYYY-MM-DD)'
                ],
                'transformation_timestamp': datetime.now().isoformat()
            }
        }
        
        # Salvar dados transformados
        info("Salvando dados transformados...")
        with open(transformed_file_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        info("Transformação concluída com sucesso!")
        info("Total de users transformados: {}", len(transformed_users))
        info("Arquivo salvo em: {}", transformed_file_path)
        
        # Mostrar exemplo de um user transformado
        if transformed_users:
            info("Exemplo de user transformado:")
            info("{}", transformed_users[0])
        
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
    info("=== INICIANDO TRANSFORMAÇÃO DE USERS ===")
    success = transform_users()
    
    if success:
        info("=== TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO ===")
    else:
        error("=== TRANSFORMAÇÃO FALHOU ===")
    
    return success

if __name__ == "__main__":
    main() 