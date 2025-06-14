"""
Script Principal de Extração - Executa todos os extratores
"""

import sys
import os
from datetime import datetime

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

def main():
    """Função principal de teste"""
    info("🚀 === SCRIPT PRINCIPAL DE EXTRAÇÃO ===")
    info("Teste inicial - scripts criados com sucesso!")

if __name__ == "__main__":
    main() 