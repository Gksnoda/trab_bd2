"""
Classe Base para Transformação de Dados
Contém funcionalidades comuns para limpeza e validação
"""

import sys
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import re

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

class BaseTransformer:
    """Classe base para transformação de dados"""
    
    def __init__(self):
        """Inicializa o transformador base"""
        self.stats = {
            'processed': 0,
            'cleaned': 0,
            'removed_null': 0,
            'removed_duplicates': 0,
            'date_conversions': 0,
            'validation_errors': 0
        }
    
    def clean_null_values(self, items: List[Dict], required_fields: List[str]) -> List[Dict]:
        """
        Remove itens com campos obrigatórios nulos/vazios
        
        Args:
            items: Lista de itens a limpar
            required_fields: Campos obrigatórios que não podem ser nulos
            
        Returns:
            Lista filtrada sem itens com campos nulos obrigatórios
        """
        if not items:
            return items
        
        original_count = len(items)
        cleaned_items = []
        
        for item in items:
            is_valid = True
            
            for field in required_fields:
                value = item.get(field)
                
                # Verifica se é None, string vazia, ou apenas espaços
                if value is None or (isinstance(value, str) and not value.strip()):
                    is_valid = False
                    info("❌ Item removido - campo '{}' nulo/vazio: {}", 
                           field, item.get('id', 'ID desconhecido'))
                    break
            
            if is_valid:
                cleaned_items.append(item)
            else:
                self.stats['removed_null'] += 1
        
        removed_count = original_count - len(cleaned_items)
        self.stats['cleaned'] += removed_count
        
        info("🧹 Limpeza nulos: {} itens → {} válidos, {} removidos", 
             original_count, len(cleaned_items), removed_count)
        
        return cleaned_items
    
    def standardize_datetime(self, datetime_str: str) -> Optional[str]:
        """
        Padroniza formato de data/hora para ISO 8601 UTC
        
        Args:
            datetime_str: String de data/hora a padronizar
            
        Returns:
            String padronizada ou None se inválida
        """
        if not datetime_str:
            return None
        
        try:
            # Formatos comuns da API Twitch
            formats = [
                "%Y-%m-%dT%H:%M:%SZ",           # 2025-06-14T16:17:04Z
                "%Y-%m-%dT%H:%M:%S.%fZ",       # 2025-06-14T16:17:04.123Z
                "%Y-%m-%d %H:%M:%S",           # 2025-06-14 16:17:04
                "%Y-%m-%dT%H:%M:%S",           # 2025-06-14T16:17:04
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(datetime_str, fmt)
                    # Garantir UTC timezone
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    
                    # Retornar formato ISO 8601 padrão
                    standardized = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    self.stats['date_conversions'] += 1
                    return standardized
                    
                except ValueError:
                    continue
            
            info("⚠️ Formato de data não reconhecido: {}", datetime_str)
            return None
            
        except Exception as e:
            error("💥 Erro ao padronizar data '{}': {}", datetime_str, e)
            return None
    
    def validate_integer(self, value: Any, field_name: str, min_value: int = 0) -> Optional[int]:
        """
        Valida e converte valor para inteiro
        
        Args:
            value: Valor a validar
            field_name: Nome do campo (para logs)
            min_value: Valor mínimo aceito
            
        Returns:
            Inteiro válido ou None se inválido
        """
        if value is None:
            return None
        
        try:
            int_value = int(value)
            
            if int_value < min_value:
                info("⚠️ Valor negativo em '{}': {}", field_name, int_value)
                return min_value
            
            return int_value
            
        except (ValueError, TypeError):
            self.stats['validation_errors'] += 1
            info("⚠️ Valor inválido para inteiro em '{}': {}", field_name, value)
            return None
    
    def validate_string(self, value: Any, field_name: str, max_length: int = None) -> Optional[str]:
        """
        Valida e limpa string
        
        Args:
            value: Valor a validar
            field_name: Nome do campo (para logs)
            max_length: Comprimento máximo (opcional)
            
        Returns:
            String válida ou None se inválida
        """
        if value is None:
            return None
        
        try:
            str_value = str(value).strip()
            
            if not str_value:
                return None
            
            # Truncar se muito longo
            if max_length and len(str_value) > max_length:
                info("⚠️ String truncada em '{}': {} chars → {}", 
                       field_name, len(str_value), max_length)
                str_value = str_value[:max_length]
            
            return str_value
            
        except Exception as e:
            self.stats['validation_errors'] += 1
            info("⚠️ Erro ao validar string em '{}': {}", field_name, e)
            return None
    
    def remove_duplicates(self, items: List[Dict], key_field: str) -> List[Dict]:
        """
        Remove duplicatas baseado em campo chave
        
        Args:
            items: Lista de itens
            key_field: Campo usado como chave única
            
        Returns:
            Lista sem duplicatas
        """
        if not items:
            return items
        
        original_count = len(items)
        seen_keys = set()
        unique_items = []
        
        for item in items:
            key = item.get(key_field)
            
            if key and key not in seen_keys:
                seen_keys.add(key)
                unique_items.append(item)
            else:
                self.stats['removed_duplicates'] += 1
                info("🔄 Duplicata removida - chave '{}': {}", key_field, key)
        
        removed_count = original_count - len(unique_items)
        
        info("🔄 Remoção duplicatas: {} itens → {} únicos, {} duplicatas removidas", 
             original_count, len(unique_items), removed_count)
        
        return unique_items
    
    def get_transformation_stats(self) -> Dict[str, int]:
        """Retorna estatísticas da transformação"""
        return self.stats.copy()
    
    def log_final_stats(self, data_type: str):
        """Loga estatísticas finais da transformação"""
        info("📊 === ESTATÍSTICAS TRANSFORMAÇÃO - {} ===", data_type.upper())
        info("  📦 Itens processados: {}", self.stats['processed'])
        info("  🧹 Itens limpos: {}", self.stats['cleaned'])
        info("  ❌ Removidos (nulos): {}", self.stats['removed_null'])
        info("  🔄 Removidos (duplicatas): {}", self.stats['removed_duplicates'])
        info("  📅 Conversões de data: {}", self.stats['date_conversions'])
        info("  ⚠️ Erros de validação: {}", self.stats['validation_errors'])
        info("📊 ================================================") 
