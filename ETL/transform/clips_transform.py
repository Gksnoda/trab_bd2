import json
import os
from pathlib import Path
import sys
from datetime import datetime

# Adicionar o diretório ETL ao path para importar o logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def transform_clips():
    """
    Transforma os dados de clips removendo campos desnecessários e filtrando dados.
    
    Campos removidos:
    - embed_url
    - thumbnail_url
    - vod_offset
    - is_featured
    
    Transformações de campos:
    - broadcaster_id → user_id (renomeação para padronização)
    - created_at: mantém apenas a data, remove o horário
    
    Filtros aplicados:
    - Remove clips que tenham video_id vazio
    - Remove clips cujo video_id não existe na tabela de videos (integridade referencial)
    - Remove clips que tenham game_id vazio
    """
    try:
        # Determinar o diretório base do projeto
        current_dir = Path(__file__).parent
        project_root = current_dir.parent  # ETL/transform -> ETL
        
        # Caminhos dos arquivos
        raw_file_path = project_root / "data" / "raw" / "clips.json"
        videos_file_path = project_root / "data" / "transformed" / "videos_transformed.json"
        transformed_dir = project_root / "data" / "transformed"
        transformed_file_path = transformed_dir / "clips_transformed.json"
        
        # Criar diretório transformed se não existir
        transformed_dir.mkdir(parents=True, exist_ok=True)
        
        info("Iniciando transformação dos dados de clips...")
        info("Arquivo de origem: {}", raw_file_path)
        info("Arquivo de videos para validação: {}", videos_file_path)
        info("Arquivo de destino: {}", transformed_file_path)
        
        # Verificar se os arquivos existem
        if not raw_file_path.exists():
            error("Arquivo de dados brutos não encontrado: {}", raw_file_path)
            return False
        
        if not videos_file_path.exists():
            error("Arquivo de videos transformados não encontrado: {}", videos_file_path)
            error("Execute primeiro a transformação de videos!")
            return False
        
        # Ler dados de videos transformados para validação
        info("Carregando video_ids válidos...")
        with open(videos_file_path, 'r', encoding='utf-8') as f:
            videos_data = json.load(f)
        
        # Extrair conjunto de video_ids válidos
        valid_video_ids = set()
        if 'data' in videos_data:
            for video in videos_data['data']:
                video_id = video.get('id')
                if video_id:
                    valid_video_ids.add(str(video_id))
        
        info("Total de video_ids válidos carregados: {}", len(valid_video_ids))
        
        # Ler dados brutos de clips
        info("Lendo dados brutos de clips...")
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Verificar estrutura dos dados
        if 'data' not in raw_data:
            error("Estrutura de dados inválida. Campo 'data' não encontrado.")
            return False
        
        clips = raw_data['data']
        info("Total de clips encontrados: {}", len(clips))
        
        # Campos a serem removidos
        fields_to_remove = [
            'embed_url',
            'thumbnail_url',
            'vod_offset',
            'is_featured',
            'creator_name',
            'creator_id',
            "broadcaster_name"
        ]
        
        info("Campos que serão removidos: {}", fields_to_remove)
        
        # Transformar dados
        transformed_clips = []
        clips_removed_empty_video_id = 0
        clips_removed_invalid_video_id = 0
        clips_removed_empty_game_id = 0
        
        for i, clip in enumerate(clips):
            # Filtrar clips com video_id vazio
            video_id = clip.get('video_id', '')
            if not video_id or video_id.strip() == '':
                clips_removed_empty_video_id += 1
                continue
            
            # Filtrar clips cujo video_id não existe na tabela de videos
            if str(video_id) not in valid_video_ids:
                clips_removed_invalid_video_id += 1
                continue
            
            # Filtrar clips com game_id vazio
            game_id = clip.get('game_id', '')
            if not game_id or game_id.strip() == '':
                clips_removed_empty_game_id += 1
                continue
            
            # Criar uma cópia do clip sem os campos indesejados
            transformed_clip = {}
            for key, value in clip.items():
                if key not in fields_to_remove:
                    # Renomear broadcaster_id para user_id
                    if key == 'broadcaster_id':
                        transformed_clip['user_id'] = value
                    # Transformar created_at para conter apenas a data
                    elif key == 'created_at' and value:
                        try:
                            # Parse do timestamp ISO 8601 e extração apenas da data
                            date_only = datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat()
                            transformed_clip[key] = date_only
                        except (ValueError, AttributeError) as e:
                            error("Erro ao processar created_at para clip {}: {}", clip.get('id', 'unknown'), str(e))
                            # Manter o valor original em caso de erro
                            transformed_clip[key] = value
                    else:
                        transformed_clip[key] = value
            
            transformed_clips.append(transformed_clip)
            
            # Log de progresso a cada 1000 clips
            if (i + 1) % 1000 == 0:
                info("Processados {} clips...", i + 1)
        
        info("Clips removidos por video_id vazio: {}", clips_removed_empty_video_id)
        info("Clips removidos por video_id inválido: {}", clips_removed_invalid_video_id)
        info("Clips removidos por game_id vazio: {}", clips_removed_empty_game_id)
        info("Clips mantidos após filtros: {}", len(transformed_clips))
        
        # Criar estrutura final dos dados transformados
        transformed_data = {
            'data': transformed_clips,
            'metadata': {
                'total_clips_original': len(clips),
                'total_clips_transformed': len(transformed_clips),
                'clips_removed_empty_video_id': clips_removed_empty_video_id,
                'clips_removed_invalid_video_id': clips_removed_invalid_video_id,
                'clips_removed_empty_game_id': clips_removed_empty_game_id,
                'valid_video_ids_loaded': len(valid_video_ids),
                'fields_removed': fields_to_remove,
                'field_transformations': [
                    'broadcaster_id renomeado para user_id',
                    'created_at: convertido para formato apenas de data (YYYY-MM-DD)'
                ],
                'filters_applied': [
                    'Removidos clips com video_id vazio',
                    'Removidos clips com video_id que não existe na tabela de videos',
                    'Removidos clips com game_id vazio'
                ],
                'transformation_timestamp': datetime.now().isoformat()
            }
        }
        
        # Salvar dados transformados
        info("Salvando dados transformados...")
        with open(transformed_file_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        info("Transformação concluída com sucesso!")
        info("Total de clips transformados: {}", len(transformed_clips))
        info("Arquivo salvo em: {}", transformed_file_path)
        
        # Mostrar exemplo de um clip transformado
        if transformed_clips:
            info("Exemplo de clip transformado:")
            info("{}", transformed_clips[0])
        
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
    info("=== INICIANDO TRANSFORMAÇÃO DE CLIPS ===")
    success = transform_clips()
    
    if success:
        info("=== TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO ===")
    else:
        error("=== TRANSFORMAÇÃO FALHOU ===")
    
    return success

if __name__ == "__main__":
    main() 