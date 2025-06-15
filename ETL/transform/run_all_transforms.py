import json
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

# Adicionar o diretório ETL ao path para importar o logger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def run_single_transform(script_name):
    """
    Executa um único script de transformação
    """
    try:
        script_path = Path(__file__).parent / script_name
        
        # Importar e executar o módulo dinamicamente
        import importlib.util
        spec = importlib.util.spec_from_file_location("transform_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Executar a função main do módulo
        success = module.main()
        
        return {
            'script': script_name,
            'success': success,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        return {
            'script': script_name,
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

def analyze_transformed_data():
    """
    Analisa os dados transformados e mostra um resumo dos atributos finais
    """
    info("=== ANALISANDO DADOS TRANSFORMADOS ===")
    
    project_root = Path(__file__).parent.parent
    transformed_dir = project_root / "data" / "transformed"
    
    # Definir arquivos de saída esperados
    files_to_analyze = {
        'streams': 'streams_transformed.json',
        'users': 'users_transformed.json', 
        'videos': 'videos_transformed.json',
        'clips': 'clips_transformed.json',
        'games': 'games_transformed.json'
    }
    
    analysis_results = {}
    
    for data_type, filename in files_to_analyze.items():
        file_path = transformed_dir / filename
        
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extrair informações
                total_records = len(data['data']) if 'data' in data else 0
                sample_record = data['data'][0] if data.get('data') else {}
                attributes = list(sample_record.keys()) if sample_record else []
                metadata = data.get('metadata', {})
                
                analysis_results[data_type] = {
                    'total_records': total_records,
                    'attributes': attributes,
                    'metadata': metadata,
                    'file_exists': True
                }
                
                info("✅ {}: {} registros processados", data_type.upper(), total_records)
                info("   Atributos mantidos: {}", attributes)
                
                if 'transformations_applied' in metadata:
                    for transformation in metadata['transformations_applied']:
                        info("   Transformação: {}", transformation)
                
                info("")
                
            else:
                analysis_results[data_type] = {
                    'file_exists': False,
                    'error': f'Arquivo não encontrado: {file_path}'
                }
                error("❌ {}: Arquivo não encontrado - {}", data_type.upper(), file_path)
                
        except Exception as e:
            analysis_results[data_type] = {
                'file_exists': True,
                'error': str(e)
            }
            error("❌ Erro ao analisar {}: {}", data_type.upper(), str(e))
    
    return analysis_results

def display_summary(analysis_results):
    """
    Exibe um resumo consolidado dos resultados
    """
    info("=== RESUMO FINAL DAS TRANSFORMAÇÕES ===")
    info("")
    
    total_records = 0
    successful_transforms = 0
    
    for data_type, results in analysis_results.items():
        if results.get('file_exists') and 'total_records' in results:
            successful_transforms += 1
            total_records += results['total_records']
    
    info("   • Total de registros processados: {}", total_records)
    info("")
    
    info("🔧 ESTRUTURA FINAL DOS DADOS:")
    for data_type, results in analysis_results.items():
        if results.get('file_exists') and 'attributes' in results:
            info("   • {}: {}", data_type.upper(), ', '.join(results['attributes']))
    
    info("")
    info("=== TRANSFORMAÇÕES CONCLUÍDAS ===")

def main():
    """
    Função principal que executa todas as transformações com dependências
    """
    info("Everything will be ok")
    info("=== INICIANDO EXECUÇÃO DE TODAS AS TRANSFORMAÇÕES ===")
    
    # Fase 1: Scripts independentes (podem executar em paralelo)
    phase1_scripts = [
        'streams_transform.py',
        'users_transform.py',
        'videos_transform.py',
        'games_transform.py'
    ]
    
    # Fase 2: Scripts dependentes (precisam executar após fase 1)
    phase2_scripts = [
        'clips_transform.py'  # Depende de videos_transform.py
    ]
    
    info("FASE 1 - Scripts independentes (paralelo):")
    for script in phase1_scripts:
        info("   • {}", script)
    
    info("FASE 2 - Scripts dependentes (sequencial):")
    for script in phase2_scripts:
        info("   • {}", script)
    info("")
    
    # Executar transformações
    start_time = time.time()
    all_results = []
    
    # FASE 1: Executar scripts independentes em paralelo
    info("🚀 FASE 1: Iniciando execução paralela dos scripts independentes...")
    
    phase1_results = []
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Submeter todas as tarefas da fase 1
        future_to_script = {
            executor.submit(run_single_transform, script): script 
            for script in phase1_scripts
        }
        
        # Coletar resultados conforme completam
        for future in as_completed(future_to_script):
            script = future_to_script[future]
            try:
                result = future.result()
                phase1_results.append(result)
                all_results.append(result)
                
                if result['success']:
                    info("✅ FASE 1: {} concluído com sucesso", result['script'])
                else:
                    error("❌ FASE 1: {} falhou: {}", result['script'], result.get('error', 'Erro desconhecido'))
                    
            except Exception as e:
                error("❌ FASE 1: Erro ao executar {}: {}", script, str(e))
                result = {
                    'script': script,
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                phase1_results.append(result)
                all_results.append(result)
    
    # Verificar se a fase 1 foi bem-sucedida
    phase1_successful = sum(1 for r in phase1_results if r['success'])
    phase1_total = len(phase1_results)
    
    info("")
    info("📊 RESULTADO FASE 1: {}/{} transformações bem-sucedidas", phase1_successful, phase1_total)
    
    # FASE 2: Executar scripts dependentes (apenas se videos_transform teve sucesso)
    videos_success = any(r['success'] and r['script'] == 'videos_transform.py' for r in phase1_results)
    
    if videos_success:
        info("")
        info("🚀 FASE 2: Iniciando execução dos scripts dependentes...")
        
        for script in phase2_scripts:
            info("Executando {}...", script)
            try:
                result = run_single_transform(script)
                all_results.append(result)
                
                if result['success']:
                    info("✅ FASE 2: {} concluído com sucesso", result['script'])
                else:
                    error("❌ FASE 2: {} falhou: {}", result['script'], result.get('error', 'Erro desconhecido'))
                    
            except Exception as e:
                error("❌ FASE 2: Erro ao executar {}: {}", script, str(e))
                all_results.append({
                    'script': script,
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
    else:
        error("❌ videos_transform.py falhou. Pulando FASE 2 (clips precisa de videos).")
        info("Scripts não executados da FASE 2: {}", phase2_scripts)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    info("")
    info("⏱️  Tempo total de execução: {:.2f} segundos".format(execution_time))
    info("")
    
    # Verificar resultados finais
    successful = sum(1 for r in all_results if r['success'])
    total = len(all_results)
    
    if successful > 0:
        # Aguardar um pouco para garantir que os arquivos foram salvos
        info("Aguardando finalização da escrita dos arquivos...")
        time.sleep(2)
        
        # Analisar dados transformados
        analysis_results = analyze_transformed_data()
        
        # Exibir resumo final
        display_summary(analysis_results)
        
        return True
    else:
        error("❌ Nenhuma transformação foi bem-sucedida!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 