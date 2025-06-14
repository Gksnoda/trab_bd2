"""
Executar todos os testes do ETL
Este script executa os testes de API da Twitch e do banco de dados
"""

import os
import sys
import subprocess

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error


def executar_todos_testes():
    """
    Executa todos os testes disponíveis
    """
    info("🚀 === EXECUTANDO TODOS OS TESTES DO ETL ===")
    
    # Diretório atual dos testes
    testes_dir = os.path.dirname(__file__)
    
    # Lista de testes para executar
    testes = [
        {
            'nome': 'Teste API Twitch',
            'arquivo': 'teste_api_twitch.py',
            'descricao': 'Testa conexão e endpoints da API da Twitch'
        },
        {
            'nome': 'Teste Banco de Dados',
            'arquivo': 'teste_banco_dados.py',
            'descricao': 'Testa conexão e operações no PostgreSQL'
        }
    ]
    
    resultados = []
    
    for teste in testes:
        info(f"\n📋 Executando: {teste['nome']}")
        info(f"   Descrição: {teste['descricao']}")
        info("=" * 60)
        
        # Caminho completo do arquivo de teste
        teste_path = os.path.join(testes_dir, teste['arquivo'])
        
        try:
            # Executar o teste usando uv run
            result = subprocess.run(
                ['uv', 'run', teste_path],
                cwd=os.path.join(testes_dir, '..', '..'),  # Executar do diretório raiz
                capture_output=True,
                text=True,
                timeout=30  # Timeout de 30 segundos
            )
            
            if result.returncode == 0:
                info(f"✅ {teste['nome']} - PASSOU")
                resultados.append({'teste': teste['nome'], 'status': 'PASSOU'})
            else:
                error(f"❌ {teste['nome']} - FALHOU")
                error(f"Erro: {result.stderr}")
                resultados.append({'teste': teste['nome'], 'status': 'FALHOU', 'erro': result.stderr})
                
        except subprocess.TimeoutExpired:
            error(f"⏰ {teste['nome']} - TIMEOUT (mais de 30 segundos)")
            resultados.append({'teste': teste['nome'], 'status': 'TIMEOUT'})
        except Exception as e:
            error(f"💥 {teste['nome']} - ERRO INESPERADO: {e}")
            resultados.append({'teste': teste['nome'], 'status': 'ERRO', 'erro': str(e)})
    
    # Relatório final
    info("\n" + "="*60)
    info("📊 RELATÓRIO FINAL DOS TESTES")
    info("=" * 60)
    
    passou = 0
    falhou = 0
    
    for resultado in resultados:
        status_emoji = "✅" if resultado['status'] == 'PASSOU' else "❌"
        info(f"{status_emoji} {resultado['teste']}: {resultado['status']}")
        
        if 'erro' in resultado:
            info(f"   💬 Detalhes: {resultado['erro'][:100]}...")
        
        if resultado['status'] == 'PASSOU':
            passou += 1
        else:
            falhou += 1
    
    info(f"\n📈 RESUMO: {passou} passou(ram), {falhou} falhou/falharam")
    
    if falhou == 0:
        info("🎉 TODOS OS TESTES PASSARAM! Sistema pronto para ETL.")
        return True
    else:
        error("⚠️ Alguns testes falharam. Verifique as configurações antes de prosseguir.")
        return False

if __name__ == "__main__":
    sucesso = executar_todos_testes()
    if sucesso:
        sys.exit(0)
    else:
        sys.exit(1) 