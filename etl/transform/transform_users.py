"""
Transformador de Dados de Usuários
Limpa e valida dados de usuários da API Twitch
"""

import sys
import os
from typing import List, Dict, Any

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

# Import da classe base
from base_transformer import BaseTransformer

class UserTransformer(BaseTransformer):
    """Transformador específico para dados de usuários"""
    
    def __init__(self):
        """Inicializa o transformador de usuários"""
        super().__init__()
        
        # Campos obrigatórios para usuários
        self.required_fields = [
            'id',
            'login',
            'display_name'
        ]
        
        # Limites de tamanho para strings
        self.string_limits = {
            'login': 25,           # Twitch username limit
            'display_name': 50,    # Display name limit  
            'description': 300,    # Bio description limit
            'broadcaster_type': 20 # partner/affiliate/normal
        }
    
    def transform_users(self, users_data: List[Dict]) -> List[Dict]:
        """
        Transforma dados brutos de usuários
        
        Args:
            users_data: Lista de dados brutos de usuários
            
        Returns:
            Lista de usuários transformados e validados
        """
        if not users_data:
            info("⚠️ Nenhum dado de usuário para transformar")
            return []
        
        info("🔄 Iniciando transformação de {} usuários...", len(users_data))
        self.stats['processed'] = len(users_data)
        
        # 1. Limpar valores nulos em campos obrigatórios
        cleaned_users = self.clean_null_values(users_data, self.required_fields)
        
        # 2. Transformar cada usuário individualmente
        transformed_users = []
        for user in cleaned_users:
            transformed_user = self._transform_single_user(user)
            if transformed_user:
                transformed_users.append(transformed_user)
        
        # 3. Remover duplicatas baseado no ID
        unique_users = self.remove_duplicates(transformed_users, 'id')
        
        # 4. Log estatísticas finais
        self.log_final_stats('users')
        
        info("✅ Transformação usuários concluída: {} usuários válidos", len(unique_users))
        return unique_users
    
    def _transform_single_user(self, user: Dict) -> Dict:
        """
        Transforma um único usuário
        
        Args:
            user: Dados brutos do usuário
            
        Returns:
            Usuário transformado ou None se inválido
        """
        try:
            transformed = {}
            
            # ID do usuário (obrigatório)
            user_id = self.validate_string(user.get('id'), 'id')
            if not user_id:
                return None
            transformed['id'] = user_id
            
            # Login (username) - obrigatório, lowercase
            login = self.validate_string(user.get('login'), 'login', self.string_limits['login'])
            if not login:
                return None
            transformed['login'] = login.lower()  # Garantir lowercase
            
            # Display name - obrigatório
            display_name = self.validate_string(
                user.get('display_name'), 
                'display_name', 
                self.string_limits['display_name']
            )
            if not display_name:
                return None
            transformed['display_name'] = display_name
            
            # Tipo de broadcaster (opcional)
            broadcaster_type = self.validate_string(
                user.get('broadcaster_type'), 
                'broadcaster_type', 
                self.string_limits['broadcaster_type']
            )
            # Se vazio, definir como 'normal'
            transformed['broadcaster_type'] = broadcaster_type if broadcaster_type else 'normal'
            
            # Descrição/Bio (opcional)
            description = self.validate_string(
                user.get('description'), 
                'description', 
                self.string_limits['description']
            )
            transformed['description'] = description
            
            # Data de criação (obrigatório)
            created_at = self.standardize_datetime(user.get('created_at'))
            if not created_at:
                info("⚠️ Usuário {} sem data de criação válida", login)
                return None
            transformed['created_at'] = created_at
            
            # URLs de imagens (opcionais, mas padronizar se existirem)
            profile_image_url = self.validate_string(user.get('profile_image_url'), 'profile_image_url')
            transformed['profile_image_url'] = profile_image_url
            
            # offline_image_url removido - não está no MER
            
            # Tipo de usuário (opcional)
            user_type = self.validate_string(user.get('type'), 'type', 20)
            transformed['type'] = user_type if user_type else 'normal'
            
            # view_count removido - não é inserido pelos transformadores
            
            return transformed
            
        except Exception as e:
            error("💥 Erro ao transformar usuário {}: {}", 
                  user.get('login', 'desconhecido'), e)
            self.stats['validation_errors'] += 1
            return None
    
    def validate_transformed_users(self, users: List[Dict]) -> bool:
        """
        Valida se todos os usuários transformados estão corretos
        
        Args:
            users: Lista de usuários transformados
            
        Returns:
            True se todos válidos, False caso contrário
        """
        if not users:
            return False
        
        info("🔍 Validando {} usuários transformados...", len(users))
        
        valid_count = 0
        for user in users:
            if self._validate_single_user(user):
                valid_count += 1
        
        is_valid = valid_count == len(users)
        
        if is_valid:
            info("✅ Todos os {} usuários são válidos", len(users))
        else:
            error("❌ Apenas {}/{} usuários são válidos", valid_count, len(users))
        
        return is_valid
    
    def _validate_single_user(self, user: Dict) -> bool:
        """Valida um único usuário transformado"""
        required_keys = ['id', 'login', 'display_name', 'created_at']
        
        for key in required_keys:
            if key not in user or not user[key]:
                info("⚠️ Usuário inválido - campo '{}' ausente: {}", 
                       key, user.get('login', 'desconhecido'))
                return False
        
        return True 
