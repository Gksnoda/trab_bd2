import inspect
import os
import json
import pprint
from datetime import datetime
from enum import Enum, auto

# Códigos ANSI para cores
RESET = "\033[0m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"

class LogLevel(Enum):
    INFO = 20
    ERROR = 40

def get_caller_info():
    """
    Obtém informações sobre o arquivo e linha que chamou a função de log.
    Pula os frames correspondentes ao próprio módulo de logging.
    """
    current_frame = inspect.currentframe()
    
    # Navegar pelo stack até encontrar um frame que não seja do arquivo logger.py
    frame = current_frame.f_back  # Primeiro, pule a função get_caller_info
    
    # Se a chamada vier de dentro do módulo logger
    if frame and os.path.basename(frame.f_code.co_filename) == 'logger.py':
        frame = frame.f_back
    
    # Se ainda estiver no logger.py (situação improvável)
    while frame and os.path.basename(frame.f_code.co_filename) == 'logger.py':
        frame = frame.f_back
    
    # Se não encontrou nenhum frame externo
    if not frame:
        return 'unknown', 0
    
    filename = os.path.basename(frame.f_code.co_filename)
    line_number = frame.f_lineno
    
    # Libere a referência ao frame atual para evitar referências circulares
    current_frame = None
    
    return filename, line_number

def format_value(value):
    """
    Formata um valor para exibição, usando pretty printing para tipos complexos
    como dicionários, listas, etc.
    """
    if isinstance(value, (dict, list, tuple, set)):
        try:
            # Primeiro tenta usar json para formatação mais limpa
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        except:
            # Se falhar (por exemplo, objetos não serializáveis), usa pprint
            return pprint.pformat(value)
    return str(value)

def logger(message, *args, level=LogLevel.INFO):
    """
    AVISO: NÃO PODE USAR EM FORMATO fstring, pois não funciona com variáveis em fstrings.

    Função de logger simplificada que usa apenas INFO e ERROR.
    
    - INFO: Texto normal com variáveis em verde
    - ERROR: Texto em vermelho com variáveis em ciano
    
    Suporta pretty printing para dicionários e outros objetos complexos.
    """
    # Obter informações do chamador real
    filename, line_number = get_caller_info()
    
    # Obter hora atual
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Formatação do nível de log
    level_name = level.name
    level_str = f"[{level_name}]"
    
    # Montar o prefixo da mensagem
    prefix = f"[{current_time}] [{filename}: {line_number}] {level_str} : "
    
    # Formatar a mensagem de acordo com o nível
    if level == LogLevel.ERROR:
        # Para ERROR: texto em vermelho, variáveis em ciano
        formatted_values = []
        for arg in args:
            # Formata o valor e aplica a cor ciano
            formatted_arg = format_value(arg)
            formatted_values.append(f"{CYAN}{formatted_arg}{RED}")
        
        try:
            msg_content = message.format(*formatted_values)
        except:
            formatted_values_str = " ".join([str(val) for val in formatted_values])
            msg_content = f"{message} {formatted_values_str}"
        
        # Aplicar vermelho ao texto e ao prefixo
        final_output = f"{RED}{prefix}{msg_content}{RESET}"
        print(final_output)
        return
    
    else:  # INFO - texto normal com variáveis em verde
        formatted_values = []
        for arg in args:
            # Formata o valor e aplica a cor verde
            formatted_arg = format_value(arg)
            formatted_values.append(f"{GREEN}{formatted_arg}{RESET}")
        
        try:
            final_message = message.format(*formatted_values)
        except:
            formatted_values_str = " ".join([str(val) for val in formatted_values])
            final_message = f"{message} {formatted_values_str}"
            
        print(f"{prefix}{final_message}")

# Funções de conveniência
def info(message, *args):
    logger(message, *args, level=LogLevel.INFO)

def error(message, *args):
    logger(message, *args, level=LogLevel.ERROR)


# Exemplo de uso
if __name__ == "__main__":
    nome = "Python"
    idade = 32
    preco = 0.0
    
    # Exemplos com valores simples
    info("Esta é uma mensagem de INFO: {}", nome)
    error("Esta é uma mensagem de ERROR: {}", nome)
    
    # Exemplos com múltiplas variáveis
    info("Variáveis: {}, {} e {}", nome, idade, preco)
    error("Erro ao processar {} com valor {}", nome, idade)
    
    # Exemplos com dicionários e listas
    dados = {"nome": "Python", "idade": 32, "linguagens": ["Python", "Java", "C++"]}
    info("Dados do usuário: {}", dados)
    error("Erro nos dados: {}", dados)
    
    # Lista complexa
    lista_complexa = [
        {"id": 1, "nome": "Item 1", "detalhes": {"cor": "azul", "tamanho": "M"}},
        {"id": 2, "nome": "Item 2", "detalhes": {"cor": "verde", "tamanho": "G"}}
    ]
    info("Lista de itens: {}", lista_complexa)