"""
ORQUESTRADOR PRINCIPAL - Pipeline ETL Completo
Executa todas as etapas do ETL em sequência:
1. Extract (Extração de dados da API Twitch)
2. Transform (Transformação e limpeza dos dados)  
3. Load (Carga no banco PostgreSQL)
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# Adicionar paths para imports
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.logger import info, error

class ETLOrchestrator:
    """Orquestrador principal do pipeline ETL"""
    
    def __init__(self):
        """Inicializa o orquestrador"""
        self.start_time = None
        self.end_time = None
        self.execution_stats = {
            'extract': {'status': 'pending', 'duration': 0, 'records': 0},
            'transform': {'status': 'pending', 'duration': 0, 'records': 0},
            'load': {'status': 'pending', 'duration': 0, 'records': 0}
        }
        self.errors = []
    
    def run_full_pipeline(self) -> bool:
        """
        Executa o pipeline ETL completo
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            info("🚀 === INICIANDO PIPELINE ETL COMPLETO ===")
            self.start_time = datetime.now()
            
            # Etapa 1: Extract
            if not self._run_extract():
                error("💥 Pipeline interrompido na etapa Extract")
                return False
            
            # Etapa 2: Transform  
            if not self._run_transform():
                error("💥 Pipeline interrompido na etapa Transform")
                return False
            
            # Etapa 3: Load
            if not self._run_load():
                error("💥 Pipeline interrompido na etapa Load")
                return False
            
            self.end_time = datetime.now()
            self._generate_final_report()
            
            info("🎉 === PIPELINE ETL CONCLUÍDO COM SUCESSO ===")
            return True
            
        except Exception as e:
            error("💥 Erro crítico no pipeline: {}", e)
            self.errors.append(f"Erro crítico: {e}")
            return False
    
    def _run_extract(self) -> bool:
        """Executa a etapa de Extract"""
        try:
            info("📥 === ETAPA 1: EXTRACT (EXTRAÇÃO) ===")
            step_start = time.time()
            
            # Importar e executar scripts de extração
            sys.path.append(os.path.join(os.path.dirname(__file__), 'extract'))
            
            # Lista de scripts de extração na ordem correta
            extract_scripts = [
                ('extract_users', 'Usuários/Streamers'),
                ('extract_games', 'Jogos/Categorias'),
                ('extract_streams', 'Streams ao Vivo'),
                ('extract_videos', 'Vídeos'),
                ('extract_clips', 'Clips')
            ]
            
            total_records = 0
            
            for script_name, description in extract_scripts:
                try:
                    info("📊 Extraindo dados: {}", description)
                    
                    # Importar e executar o script
                    module = __import__(script_name)
                    if hasattr(module, 'main'):
                        result = module.main()
                        if isinstance(result, dict) and 'records_count' in result:
                            records = result['records_count']
                            total_records += records
                            info("✅ {} extraído: {} registros", description, records)
                        else:
                            info("✅ {} extraído com sucesso", description)
                    else:
                        error("⚠️ Script {} não tem função main()", script_name)
                        
                except Exception as e:
                    error("💥 Erro ao extrair {}: {}", description, e)
                    self.errors.append(f"Extract {description}: {e}")
                    return False
            
            # Atualizar estatísticas
            duration = time.time() - step_start
            self.execution_stats['extract'] = {
                'status': 'success',
                'duration': duration,
                'records': total_records
            }
            
            info("✅ Extract concluído: {} registros em {}s", total_records, f"{duration:.2f}")
            return True
            
        except Exception as e:
            error("💥 Erro na etapa Extract: {}", e)
            self.execution_stats['extract']['status'] = 'error'
            self.errors.append(f"Extract: {e}")
            return False
    
    def _run_transform(self) -> bool:
        """Executa a etapa de Transform"""
        try:
            info("🔄 === ETAPA 2: TRANSFORM (TRANSFORMAÇÃO) ===")
            step_start = time.time()
            
            # Importar e executar o script de transformação
            sys.path.append(os.path.join(os.path.dirname(__file__), 'transform'))
            
            try:
                import run_all_transformations
                result = run_all_transformations.main()
                
                # Extrair estatísticas se disponível
                total_records = 0
                if isinstance(result, dict):
                    total_records = result.get('total_records', 0)
                
                # Atualizar estatísticas
                duration = time.time() - step_start
                self.execution_stats['transform'] = {
                    'status': 'success',
                    'duration': duration,
                    'records': total_records
                }
                
                info("✅ Transform concluído: {} registros em {}s", total_records, f"{duration:.2f}")
                return True
                
            except Exception as e:
                error("💥 Erro ao executar transformações: {}", e)
                self.errors.append(f"Transform: {e}")
                return False
            
        except Exception as e:
            error("💥 Erro na etapa Transform: {}", e)
            self.execution_stats['transform']['status'] = 'error'
            self.errors.append(f"Transform: {e}")
            return False
    
    def _run_load(self) -> bool:
        """Executa a etapa de Load"""
        try:
            info("💾 === ETAPA 3: LOAD (CARGA NO BANCO) ===")
            step_start = time.time()
            
            # Importar e executar scripts de carga
            sys.path.append(os.path.join(os.path.dirname(__file__), 'load'))
            
            try:
                # Primeiro criar o schema
                info("🗄️ Criando schema do banco...")
                import database_schema
                schema = database_schema.DatabaseSchema()
                if not schema.create_database_schema():
                    error("💥 Falha ao criar schema do banco")
                    return False
                
                # Depois executar as cargas
                info("📊 Executando cargas...")
                import run_all_loads
                result = run_all_loads.main()
                
                # Extrair estatísticas se disponível
                total_records = 0
                if isinstance(result, dict):
                    total_records = result.get('total_records', 0)
                
                # Atualizar estatísticas
                duration = time.time() - step_start
                self.execution_stats['load'] = {
                    'status': 'success',
                    'duration': duration,
                    'records': total_records
                }
                
                info("✅ Load concluído: {} registros em {}s", total_records, f"{duration:.2f}")
                return True
                
            except Exception as e:
                error("💥 Erro ao executar carga: {}", e)
                self.errors.append(f"Load: {e}")
                return False
            
        except Exception as e:
            error("💥 Erro na etapa Load: {}", e)
            self.execution_stats['load']['status'] = 'error'
            self.errors.append(f"Load: {e}")
            return False
    
    def _generate_final_report(self):
        """Gera relatório final de execução"""
        try:
            info("📋 === RELATÓRIO FINAL DE EXECUÇÃO ===")
            
            # Calcular tempo total
            total_duration = (self.end_time - self.start_time).total_seconds()
            
            info("⏱️ TEMPO DE EXECUÇÃO:")
            info("   • Início: {}", self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
            info("   • Fim: {}", self.end_time.strftime("%Y-%m-%d %H:%M:%S"))
            info("   • Duração Total: {}s ({}min)", f"{total_duration:.2f}", f"{total_duration/60:.2f}")
            
            info("📊 ESTATÍSTICAS POR ETAPA:")
            
            # Extract
            extract_stats = self.execution_stats['extract']
            info("   📥 EXTRACT:")
            info("      Status: {}", extract_stats['status'].upper())
            info("      Duração: {}s", f"{extract_stats['duration']:.2f}")
            info("      Registros: {}", extract_stats['records'])
            
            # Transform
            transform_stats = self.execution_stats['transform']
            info("   🔄 TRANSFORM:")
            info("      Status: {}", transform_stats['status'].upper())
            info("      Duração: {}s", f"{transform_stats['duration']:.2f}")
            info("      Registros: {}", transform_stats['records'])
            
            # Load
            load_stats = self.execution_stats['load']
            info("   💾 LOAD:")
            info("      Status: {}", load_stats['status'].upper())
            info("      Duração: {}s", f"{load_stats['duration']:.2f}")
            info("      Registros: {}", load_stats['records'])
            
            # Totais
            total_records = sum(stats['records'] for stats in self.execution_stats.values())
            processing_rate = total_records/total_duration if total_duration > 0 else 0
            info("📈 TOTAIS:")
            info("   • Total de Registros Processados: {}", total_records)
            info("   • Taxa de Processamento: {} registros/segundo", f"{processing_rate:.2f}")
            
            # Erros (se houver)
            if self.errors:
                info("⚠️ ERROS ENCONTRADOS:")
                for i, error_msg in enumerate(self.errors, 1):
                    info("   {}. {}", i, error_msg)
            else:
                info("✅ NENHUM ERRO ENCONTRADO")
            
            info("🎯 === PIPELINE ETL FINALIZADO ===")
            
        except Exception as e:
            error("💥 Erro ao gerar relatório final: {}", e)

def main():
    """Função principal"""
    try:
        info("🚀 === ORQUESTRADOR ETL TWITCH ANALYTICS ===")
        info("📅 Execução iniciada em: {}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Criar e executar orquestrador
        orchestrator = ETLOrchestrator()
        success = orchestrator.run_full_pipeline()
        
        if success:
            info("🎉 Pipeline ETL executado com SUCESSO!")
            return {'status': 'success', 'stats': orchestrator.execution_stats}
        else:
            error("💥 Pipeline ETL FALHOU!")
            return {'status': 'error', 'errors': orchestrator.errors}
            
    except Exception as e:
        error("💥 Erro crítico no orquestrador: {}", e)
        return {'status': 'error', 'errors': [str(e)]}

if __name__ == "__main__":
    result = main()
    
    # Exit code baseado no resultado
    if result['status'] == 'success':
        sys.exit(0)
    else:
        sys.exit(1) 