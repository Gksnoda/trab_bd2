import subprocess
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import info, error

def run_script(script_name, description):
    """
    Executa um script Python usando subprocess
    
    Args:
        script_name (str): Nome do arquivo do script
        description (str): Descrição do que o script faz
        
    Returns:
        tuple: (success: bool, duration: float, error_msg: str)
    """
    start_time = time.time()
    
    try:
        # Executar o script usando uv run
        result = subprocess.run(
            ["uv", "run", script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=1800  # Timeout de 30 minutos por script
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            return True, duration, ""
        else:
            return False, duration, result.stderr or result.stdout
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        return False, duration, "Script excedeu o timeout de 30 minutos"
        
    except Exception as e:
        duration = time.time() - start_time
        return False, duration, str(e)

def run_all_extractions():
    """
    Executa todos os scripts de extração em sequência
    """
    start_time = time.time()
    
    info("=" * 60)
    info("INICIANDO PROCESSO COMPLETO DE EXTRAÇÃO")
    info("=" * 60)
    
    # Lista de extrações para executar (ordem é importante!)
    extractions = [
        ("streams.py", "Extração de streams da Twitch"),
        ("users.py", "Extração de dados dos usuários"),
        ("videos.py", "Extração de vídeos dos usuários"),
        ("clips.py", "Extração de clips dos usuários"),
        ("games.py", "Extração de dados dos games")
    ]
    
    successful_extractions = []
    failed_extractions = []
    
    for i, (script_name, description) in enumerate(extractions, 1):
        info("")
        info(f"ETAPA {i}/5: {script_name.upper().replace('.PY', '')}")
        info(f"Descrição: {description}")
        info("-" * 50)
        
        # Executar o script
        success, duration, error_msg = run_script(script_name, description)
        
        if success:
            successful_extractions.append((script_name, duration))
            info(f"✅ {script_name} concluído com sucesso em {duration:.1f}s")
        else:
            failed_extractions.append((script_name, error_msg))
            error(f"❌ Erro na execução de {script_name}:")
            error(f"Tempo até o erro: {duration:.1f}s")
            if error_msg:
                error(f"Mensagem de erro: {error_msg}")
            
            # Continuar com as próximas extrações mesmo se uma falhar
            info(f"Continuando com as próximas extrações...")
    
    # Relatório final
    total_time = time.time() - start_time
    
    info("")
    info("=" * 60)
    info("RELATÓRIO FINAL DE EXTRAÇÃO")
    info("=" * 60)
    
    if successful_extractions:
        info("✅ EXTRAÇÕES CONCLUÍDAS COM SUCESSO:")
        for script_name, duration in successful_extractions:
            info(f"   • {script_name}: {duration:.1f}s")
    
    if failed_extractions:
        info("")
        error("❌ EXTRAÇÕES COM ERRO:")
        for script_name, error_msg in failed_extractions:
            error(f"   • {script_name}: {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}")
    
    info("")
    info(f"TEMPO TOTAL: {total_time:.1f}s")
    if len(successful_extractions) == len(extractions):
        info("🎉 TODAS AS EXTRAÇÕES FORAM CONCLUÍDAS COM SUCESSO!")
    else:
        info("⚠️  Algumas extrações falharam. Verifique os logs acima.")
    
    info("=" * 60)
    
    return len(successful_extractions) == len(extractions)

def main():
    """
    Função principal para executar todas as extrações
    """
    try:
        # Verificar se estamos no diretório correto
        current_dir = os.path.basename(os.getcwd())
        if current_dir != "extract":
            error("Este script deve ser executado do diretório ETL/extract/")
            return False
        
        # Executar todas as extrações
        success = run_all_extractions()
        
        return success
        
    except KeyboardInterrupt:
        error("")
        error("❌ Processo interrompido pelo usuário (Ctrl+C)")
        error("Algumas extrações podem não ter sido concluídas.")
        return False
        
    except Exception as e:
        error(f"❌ Erro fatal no processo de extração: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 