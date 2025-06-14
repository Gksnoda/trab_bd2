"""
Classe Base para Carregamento de Dados
Contém funcionalidades comuns para inserção no PostgreSQL
"""

import sys
import os
from typing import List, Dict, Any, Optional
import json

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da configuração do banco
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from database_client import DatabaseClient

class BaseLoader:
    """Classe base para carregamento de dados"""
    
    def __init__(self, table_name: str):
        """
        Inicializa o loader base
        
        Args:
            table_name: Nome da tabela no banco de dados
        """
        self.table_name = table_name
        self.db_client = DatabaseClient()
        self.stats = {
            'total': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
    
    def connect(self) -> bool:
        """
        Conecta ao banco de dados (usando context manager)
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            # Testar conexão usando context manager
            with self.db_client.get_connection() as conn:
                return conn is not None
        except Exception as e:
            error("💥 Erro ao conectar com banco: {}", e)
            return False
    
    def disconnect(self):
        """Desconecta do banco de dados (não necessário com context manager)"""
        # Context manager cuida automaticamente da desconexão
        pass
    
    def check_table_exists(self) -> bool:
        """
        Verifica se a tabela existe no banco
        
        Returns:
            True se existe, False caso contrário
        """
        try:
            sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (self.table_name,))
                result = cursor.fetchone()
                
                if result and len(result) > 0:
                    return result[0]  # Primeiro campo do resultado
                
                return False
            
        except Exception as e:
            error("💥 Erro ao verificar se tabela {} existe: {}", self.table_name, e)
            return False
    
    def get_table_count(self) -> int:
        """
        Retorna número de registros na tabela
        
        Returns:
            Quantidade de registros ou -1 se erro
        """
        try:
            sql = f"SELECT COUNT(*) FROM {self.table_name};"
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result and len(result) > 0:
                    return result[0]
                
                return 0
            
        except Exception as e:
            error("💥 Erro ao contar registros em {}: {}", self.table_name, e)
            return -1
    
    def record_exists(self, record_id: str) -> bool:
        """
        Verifica se um registro já existe na tabela
        
        Args:
            record_id: ID do registro a verificar
            
        Returns:
            True se existe, False caso contrário
        """
        try:
            sql = f"SELECT 1 FROM {self.table_name} WHERE id = %s LIMIT 1;"
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (record_id,))
                result = cursor.fetchone()
                
                return result is not None
            
        except Exception as e:
            error("💥 Erro ao verificar existência do registro {}: {}", record_id, e)
            return False
    
    def build_insert_sql(self, fields: List[str]) -> str:
        """
        Constrói SQL de inserção para execute_values
        
        Args:
            fields: Lista de campos a inserir
            
        Returns:
            Query SQL de INSERT (sem placeholders - execute_values adiciona automaticamente)
        """
        fields_str = ', '.join(fields)
        
        sql = f"""
        INSERT INTO {self.table_name} ({fields_str}) 
        VALUES %s
        ON CONFLICT (id) DO NOTHING
        """
        
        return sql
    
    def build_upsert_sql(self, fields: List[str], conflict_field: str = 'id') -> str:
        """
        Constrói SQL de inserção com atualização em caso de conflito para execute_values
        
        Args:
            fields: Lista de campos a inserir/atualizar
            conflict_field: Campo de conflito (default: id)
            
        Returns:
            Query SQL de UPSERT (sem placeholders - execute_values adiciona automaticamente)
        """
        fields_str = ', '.join(fields)
        
        # Montar cláusula UPDATE para campos que não são chave
        update_fields = [f for f in fields if f != conflict_field]
        update_clause = ', '.join([f"{field} = EXCLUDED.{field}" for field in update_fields])
        
        sql = f"""
        INSERT INTO {self.table_name} ({fields_str}) 
        VALUES %s
        ON CONFLICT ({conflict_field}) DO UPDATE SET {update_clause}
        """
        
        return sql
    
    def insert_batch(self, records: List[Dict], use_upsert: bool = False) -> int:
        """
        Insere lote de registros
        
        Args:
            records: Lista de registros a inserir
            use_upsert: Se True, usa UPSERT, senão usa INSERT simples
            
        Returns:
            Número de registros inseridos
        """
        if not records:
            return 0
        
        try:
            # Pegar campos do primeiro registro
            fields = list(records[0].keys())
            
            # Construir SQL
            if use_upsert:
                sql = self.build_upsert_sql(fields)
            else:
                sql = self.build_insert_sql(fields)
            
            # Preparar dados
            batch_data = []
            for record in records:
                values = [record.get(field) for field in fields]
                batch_data.append(values)
            
            # Executar batch com context manager usando execute_values
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                
                # Usar execute_values do psycopg2 para inserção em lote
                from psycopg2.extras import execute_values
                execute_values(cursor, sql, batch_data)
                
                rows_affected = cursor.rowcount
                conn.commit()
                
                return rows_affected if rows_affected is not None else 0
            
        except Exception as e:
            error("💥 Erro ao inserir lote em {}: {}", self.table_name, e)
            return 0
    
    def load_data(self, data: List[Dict], batch_size: int = 1000, use_upsert: bool = False) -> Dict[str, int]:
        """
        Carrega dados na tabela
        
        Args:
            data: Lista de dados a carregar
            batch_size: Tamanho do lote para inserção
            use_upsert: Se True, usa UPSERT, senão usa INSERT simples
            
        Returns:
            Dicionário com estatísticas da carga
        """
        if not data:
            info("⚠️ Nenhum dado para carregar em {}", self.table_name)
            return self.stats
        
        self.stats['total'] = len(data)
        
        try:
            info("📦 Carregando {} registros em {} (lotes de {})", 
                 len(data), self.table_name, batch_size)
            
            # Processar em lotes
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(data) + batch_size - 1) // batch_size
                
                info("📝 Processando lote {}/{} ({} registros)", 
                     batch_num, total_batches, len(batch))
                
                # Inserir lote
                inserted = self.insert_batch(batch, use_upsert)
                
                if use_upsert:
                    self.stats['inserted'] += inserted  # No upsert, pode ser insert ou update
                else:
                    self.stats['inserted'] += inserted
                    self.stats['skipped'] += len(batch) - inserted  # Conflitos ignorados
                
                info("✅ Lote {}: {} registros processados", batch_num, inserted)
            
            # Log final
            self.log_load_stats()
            
        except Exception as e:
            error("💥 Erro geral ao carregar dados em {}: {}", self.table_name, e)
            self.stats['errors'] += 1
        
        return self.stats
    
    def log_load_stats(self):
        """Loga estatísticas da carga"""
        info("📊 === ESTATÍSTICAS CARGA - {} ===", self.table_name.upper())
        info("  📦 Total de registros: {}", self.stats['total'])
        info("  ✅ Inseridos: {}", self.stats['inserted'])
        info("  🔄 Atualizados: {}", self.stats['updated'])
        info("  ⏭️ Ignorados: {}", self.stats['skipped'])
        info("  ❌ Erros: {}", self.stats['errors'])
        
        # Calcular taxa de sucesso
        success_rate = 0
        if self.stats['total'] > 0:
            success_rate = ((self.stats['inserted'] + self.stats['updated']) / self.stats['total']) * 100
        
        info("  📈 Taxa de sucesso: {:.1f}%", success_rate)
        info("📊 =" * 40)
    
    def get_load_stats(self) -> Dict[str, int]:
        """Retorna estatísticas da carga"""
        return self.stats.copy()
    
    def clear_table(self) -> bool:
        """
        Remove todos os dados da tabela (cuidado!)
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            info("🗑️ Limpando tabela {}...", self.table_name)
            
            sql = f"DELETE FROM {self.table_name};"
            with self.db_client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
            
            count = self.get_table_count()
            info("✅ Tabela {} limpa. Registros restantes: {}", self.table_name, count)
            
            return True
            
        except Exception as e:
            error("💥 Erro ao limpar tabela {}: {}", self.table_name, e)
            return False 