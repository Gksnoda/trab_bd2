"""
Script de Execução Rápida do Pipeline ETL
Executa o orquestrador principal de forma simplificada
"""

import sys
import os

# Adicionar path para imports
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl.utils.logger import info, error

def main():
    """Executa o pipeline ETL completo"""
    try:
        info("🚀 Iniciando Pipeline ETL Twitch Analytics...")
        
        # Importar e executar orquestrador
        from orchestrator import main as run_orchestrator
        
        result = run_orchestrator()
        
        if result['status'] == 'success':
            info("🎉 Pipeline ETL concluído com SUCESSO!")
            info("📊 Estatísticas disponíveis no log acima")
            return True
        else:
            error("💥 Pipeline ETL FALHOU!")
            error("❌ Erros: {}", result.get('errors', []))
            return False
            
    except Exception as e:
        error("💥 Erro ao executar pipeline: {}", e)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 