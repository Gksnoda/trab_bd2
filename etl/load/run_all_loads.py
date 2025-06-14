"""
Script Principal de Carga - Orquestra inserção de dados no PostgreSQL
Carrega dados transformados na ordem correta respeitando dependências
"""

import sys
import os
import json
import glob
from datetime import datetime
from typing import Dict, Any, List

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Imports dos módulos de carga
from base_loader import BaseLoader
from database_schema import DatabaseSchema
from data_filter import DataFilter

class DataLoadOrchestrator:
    """Orquestrador principal de carga de dados"""
    
    def __init__(self):
        """Inicializa o orquestrador"""
        self.data_dir = self._ensure_data_dir()
        self.schema_manager = DatabaseSchema()
        self.data_filter = DataFilter()
        self.load_stats = {}
        
        # Ordem de carga respeitando dependências
        self.load_order = [
            'users',    # Sem dependências
            'games',    # Sem dependências  
            'streams',  # Depende de users e games
            'videos',   # Depende de users
            'clips'     # Depende de users e games
        ]
    
    def _ensure_data_dir(self) -> str:
        """Garante que o diretório de dados existe"""
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            info("📁 Diretório de dados criado: {}", data_dir)
        return data_dir
    
    def find_latest_transformed_file(self) -> str:
        """
        Encontra o arquivo de dados transformados mais recente
        
        Returns:
            Nome do arquivo mais recente ou string vazia se não encontrar
        """
        # Buscar arquivos com padrão de transformação
        pattern = os.path.join(self.data_dir, "*transformed*.json")
        files = glob.glob(pattern)
        
        if not files:
            error("❌ Nenhum arquivo de dados transformados encontrado em {}", self.data_dir)
            return ""
        
        # Pegar o arquivo mais recente baseado na data de modificação
        latest_file = max(files, key=os.path.getmtime)
        filename = os.path.basename(latest_file)
        
        info("📂 Arquivo de dados transformados mais recente: {}", filename)
        return filename
    
    def load_transformed_data(self, file_path: str) -> Dict[str, Any]:
        """
        Carrega dados transformados de arquivo JSON
        
        Args:
            file_path: Caminho para o arquivo de dados transformados
            
        Returns:
            Dicionário com dados transformados
        """
        try:
            full_path = os.path.join(self.data_dir, file_path)
            
            if not os.path.exists(full_path):
                error("❌ Arquivo não encontrado: {}", full_path)
                return {}
            
            with open(full_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            info("📂 Dados transformados carregados de: {}", file_path)
            
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
    
    def setup_database(self) -> bool:
        """
        Configura o banco de dados (cria tabelas se não existirem)
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            info("🗄️ Configurando banco de dados...")
            
            # Criar schema se necessário
            success = self.schema_manager.create_database_schema()
            
            if success:
                info("✅ Banco de dados configurado com sucesso")
            else:
                error("❌ Falha ao configurar banco de dados")
            
            return success
            
        except Exception as e:
            error("💥 Erro ao configurar banco: {}", e)
            return False
    
    def load_single_table(self, table_name: str, data: List[Dict]) -> Dict[str, int]:
        """
        Carrega dados em uma única tabela
        
        Args:
            table_name: Nome da tabela
            data: Dados a carregar
            
        Returns:
            Estatísticas da carga
        """
        try:
            info("📝 Carregando dados na tabela: {}", table_name)
            
            # Criar loader para a tabela
            loader = BaseLoader(table_name)
            
            # Conectar ao banco
            if not loader.connect():
                error("❌ Falha ao conectar para carregar {}", table_name)
                return {'total': 0, 'inserted': 0, 'errors': 1}
            
            try:
                # Verificar se tabela existe
                if not loader.check_table_exists():
                    error("❌ Tabela {} não existe", table_name)
                    return {'total': 0, 'inserted': 0, 'errors': 1}
                
                # Carregar dados
                stats = loader.load_data(data, batch_size=500, use_upsert=True)
                
                info("✅ Carga de {} concluída", table_name)
                return stats
                
            finally:
                loader.disconnect()
                
        except Exception as e:
            error("💥 Erro ao carregar {}: {}", table_name, e)
            return {'total': len(data) if data else 0, 'inserted': 0, 'errors': 1}
    
    def load_all_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        """
        Carrega todos os dados na ordem correta
        
        Args:
            transformed_data: Dados transformados completos
            
        Returns:
            Dicionário com estatísticas por tabela
        """
        info("🚀 === INICIANDO CARGA COMPLETA ===")
        
        if 'data' not in transformed_data:
            error("❌ Chave 'data' não encontrada nos dados transformados")
            return {}
        
        # Aplicar filtro de integridade referencial
        info("🔍 Aplicando filtro de integridade referencial...")
        filtered_data = self.data_filter.filter_data_for_integrity(transformed_data)
        
        data_section = filtered_data['data']
        all_stats = {}
        
        # Carregar na ordem correta
        for table_name in self.load_order:
            if table_name in data_section:
                table_data = data_section[table_name]
                
                if table_data and len(table_data) > 0:
                    info("📦 Carregando {} ({} registros)", table_name, len(table_data))
                    stats = self.load_single_table(table_name, table_data)
                    all_stats[table_name] = stats
                else:
                    info("⚠️ Tabela {} sem dados para carregar", table_name)
                    all_stats[table_name] = {'total': 0, 'inserted': 0, 'errors': 0}
            else:
                info("⚠️ Tabela {} não encontrada nos dados transformados", table_name)
                all_stats[table_name] = {'total': 0, 'inserted': 0, 'errors': 0}
        
        self.load_stats = all_stats
        
        info("✅ === CARGA COMPLETA FINALIZADA ===")
        return all_stats
    
    def get_database_summary(self) -> Dict[str, int]:
        """
        Retorna resumo atual do banco de dados
        
        Returns:
            Dicionário com contagem por tabela
        """
        try:
            summary = {}
            
            for table_name in self.load_order:
                loader = BaseLoader(table_name)
                
                if loader.connect():
                    try:
                        count = loader.get_table_count()
                        summary[table_name] = count if count >= 0 else 0
                    finally:
                        loader.disconnect()
                else:
                    summary[table_name] = 0
            
            return summary
            
        except Exception as e:
            error("💥 Erro ao obter resumo do banco: {}", e)
            return {}
    
    def log_final_summary(self):
        """Loga resumo final da carga"""
        info("📊 === RESUMO FINAL DA CARGA ===")
        
        # Estatísticas de carga
        total_processed = 0
        total_inserted = 0
        total_errors = 0
        
        for table_name, stats in self.load_stats.items():
            total_processed += stats.get('total', 0)
            total_inserted += stats.get('inserted', 0)
            total_errors += stats.get('errors', 0)
            
            info("  📋 {}: {} → {} inseridos", 
                 table_name.capitalize(), 
                 stats.get('total', 0), 
                 stats.get('inserted', 0))
        
        # Totais gerais
        info("📊 TOTAIS:")
        info("  📦 Registros processados: {}", total_processed)
        info("  ✅ Registros inseridos: {}", total_inserted)
        info("  ❌ Erros: {}", total_errors)
        
        # Taxa de sucesso
        success_rate = 0
        if total_processed > 0:
            success_rate = (total_inserted / total_processed) * 100
        
        info("  📈 Taxa de sucesso: {:.1f}%", success_rate)
        
        # Estado atual do banco
        info("📊 ESTADO ATUAL DO BANCO:")
        db_summary = self.get_database_summary()
        for table_name, count in db_summary.items():
            info("  🗄️ {}: {} registros", table_name.capitalize(), count)
        
        info("📊 =" * 50)

def main():
    """Função principal"""
    info("🚀 === SCRIPT PRINCIPAL DE CARGA ===")
    
    # Criar orquestrador
    orchestrator = DataLoadOrchestrator()
    
    # Configurar banco de dados
    if not orchestrator.setup_database():
        error("❌ Falha ao configurar banco de dados")
        return
    
    # Buscar arquivo de dados transformados
    transformed_file = orchestrator.find_latest_transformed_file()
    
    if not transformed_file:
        error("❌ Nenhum arquivo de dados transformados encontrado")
        return
    
    # Carregar dados transformados
    transformed_data = orchestrator.load_transformed_data(transformed_file)
    
    if not transformed_data:
        error("❌ Falha ao carregar dados transformados")
        return
    
    # Carregar todos os dados
    load_stats = orchestrator.load_all_data(transformed_data)
    
    # Log resumo final
    orchestrator.log_final_summary()
    
    info("🏁 Carga concluída!")

if __name__ == "__main__":
    main() 