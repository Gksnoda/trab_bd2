"""
Teste integrado das configurações do ETL
Valida API Twitch, banco PostgreSQL e funcionalidades básicas
"""

import sys
import os

# Adicionar o diretório raiz ao path para importar o logger
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from etl.utils.logger import info, error

from settings import validate_config, TwitchAPIConfig, DatabaseConfig, TableSchemas
from twitch_client import TwitchAPIClient
from database_client import DatabaseClient

class ConfigTester:
    """Classe para testar todas as configurações do ETL"""
    
    def __init__(self):
        self.twitch_client = None
        self.db_client = None
        self.test_results = {}
    
    def run_all_tests(self) -> bool:
        """
        Executa todos os testes de configuração
        
        Returns:
            True se todos os testes passaram, False caso contrário
        """
        info("🚀 === INICIANDO TESTES DE CONFIGURAÇÃO ===")
        
        tests = [
            ("Validação de Configurações", self.test_config_validation),
            ("Conexão API Twitch", self.test_twitch_connection),
            ("Conexão Banco PostgreSQL", self.test_database_connection),
            ("Criação de Tabelas", self.test_table_creation),
            ("Integração API → Banco", self.test_integration)
        ]
        
        all_passed = True
        
        for test_name, test_func in tests:
            info("\n📋 Executando: {}", test_name)
            info("=" * 60)
            
            try:
                result = test_func()
                self.test_results[test_name] = result
                
                if result:
                    info("✅ {} - PASSOU", test_name)
                else:
                    error("❌ {} - FALHOU", test_name)
                    all_passed = False
                    
            except Exception as e:
                error("💥 {} - ERRO: {}", test_name, e)
                self.test_results[test_name] = False
                all_passed = False
        
        # Relatório final
        self.print_final_report(all_passed)
        return all_passed
    
    def test_config_validation(self) -> bool:
        """Testa validação das configurações básicas"""
        
        # Verificar se todas as variáveis de ambiente estão presentes
        errors = validate_config()
        
        if errors:
            error("❌ Configurações inválidas:")
            for err in errors:
                error("   - {}", err)
            return False
        
        info("✅ Todas as configurações básicas estão válidas")
        
        # Verificar esquemas das tabelas
        table_count = len(TableSchemas.CREATION_ORDER)
        info("✅ {} esquemas de tabela definidos", table_count)
        
        # Verificar endpoints da API
        endpoint_count = len(TwitchAPIConfig.ENDPOINTS)
        info("✅ {} endpoints da API configurados", endpoint_count)
        
        return True
    
    def test_twitch_connection(self) -> bool:
        """Testa conexão e funcionalidades da API Twitch"""
        
        try:
            self.twitch_client = TwitchAPIClient()
            
            # Teste 1: Validar token
            if not self.twitch_client.validate_token():
                error("❌ Token da API Twitch inválido")
                return False
            
            # Teste 2: Buscar dados de usuário
            users = self.twitch_client.get_users(['twitchdev'])
            if not users:
                error("❌ Não foi possível buscar dados de usuário")
                return False
            
            info("✅ Dados de usuário obtidos: {}", users[0]['display_name'])
            
            # Teste 3: Buscar jogos populares
            games = self.twitch_client.get_top_games(3)
            if not games:
                error("❌ Não foi possível buscar jogos populares")
                return False
            
            info("✅ {} jogos populares encontrados", len(games))
            
            # Teste 4: Buscar streams
            streams = self.twitch_client.get_streams(limit=3)
            info("✅ {} streams ao vivo encontradas", len(streams))
            
            return True
            
        except Exception as e:
            error("💥 Erro no teste da API Twitch: {}", e)
            return False
    
    def test_database_connection(self) -> bool:
        """Testa conexão e operações básicas do banco"""
        
        try:
            self.db_client = DatabaseClient()
            
            # Teste 1: Conexão básica
            if not self.db_client.test_connection():
                error("❌ Não foi possível conectar ao banco")
                return False
            
            # Teste 2: Verificar tabelas existentes
            table_info = self.db_client.get_table_info()
            info("✅ {} tabelas encontradas no banco", len(table_info))
            
            return True
            
        except Exception as e:
            error("💥 Erro no teste do banco: {}", e)
            return False
    
    def test_table_creation(self) -> bool:
        """Testa criação das tabelas do MER"""
        
        if not self.db_client:
            error("❌ Cliente do banco não inicializado")
            return False
        
        try:
            # Remover tabelas existentes (reset)
            info("🧹 Removendo tabelas existentes...")
            self.db_client.drop_tables()
            
            # Criar todas as tabelas
            info("🏗️ Criando tabelas baseadas no MER...")
            if not self.db_client.create_tables():
                error("❌ Falha ao criar tabelas")
                return False
            
            # Verificar se todas foram criadas
            table_info = self.db_client.get_table_info()
            expected_tables = [name for name, _ in TableSchemas.CREATION_ORDER]
            
            for table_name in expected_tables:
                if table_name not in table_info:
                    error("❌ Tabela {} não foi criada", table_name)
                    return False
                info("✅ Tabela {} criada com sucesso", table_name)
            
            return True
            
        except Exception as e:
            error("💥 Erro no teste de criação de tabelas: {}", e)
            return False
    
    def test_integration(self) -> bool:
        """Testa integração completa: API → Transform → Banco"""
        
        if not self.twitch_client or not self.db_client:
            error("❌ Clientes não inicializados")
            return False
        
        try:
            # Teste 1: Buscar e inserir usuários
            info("🧪 Testando fluxo: API → Banco (Usuários)")
            users = self.twitch_client.get_users(['twitchdev', 'ninja'])
            
            if users:
                inserted = self.db_client.insert_users(users)
                if inserted > 0:
                    info("✅ {} usuários inseridos no banco", inserted)
                else:
                    error("❌ Nenhum usuário foi inserido")
                    return False
            
            # Teste 2: Buscar e inserir jogos
            info("🧪 Testando fluxo: API → Banco (Jogos)")
            games = self.twitch_client.get_top_games(5)
            
            if games:
                inserted = self.db_client.insert_games(games)
                if inserted > 0:
                    info("✅ {} jogos inseridos no banco", inserted)
                else:
                    error("❌ Nenhum jogo foi inserido")
                    return False
            
            # Teste 3: Verificar dados no banco
            info("🧪 Verificando dados inseridos...")
            table_info = self.db_client.get_table_info()
            
            total_records = sum(table_info.values())
            if total_records > 0:
                info("✅ {} registros total no banco", total_records)
                return True
            else:
                error("❌ Nenhum registro encontrado no banco")
                return False
            
        except Exception as e:
            error("💥 Erro no teste de integração: {}", e)
            return False
    
    def print_final_report(self, all_passed: bool):
        """Imprime relatório final dos testes"""
        
        info("\n" + "="*60)
        info("📊 RELATÓRIO FINAL DOS TESTES DE CONFIGURAÇÃO")
        info("=" * 60)
        
        # Estatísticas
        passed = sum(1 for result in self.test_results.values() if result)
        total = len(self.test_results)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASSOU" if result else "❌ FALHOU"
            info("{}: {}", test_name, status)
        
        info("\n📈 RESUMO: {}/{} testes passaram", passed, total)
        
        if all_passed:
            info("🎉 TODOS OS TESTES PASSARAM!")
            info("✅ Sistema pronto para ETL completo")
            info("\n🚀 Próximos passos:")
            info("   1. Criar scripts de extração")
            info("   2. Implementar transformações")
            info("   3. Completar processo de carga")
        else:
            error("⚠️ Alguns testes falharam")
            error("❌ Corrija os problemas antes de prosseguir")

def main():
    """Função principal para executar os testes"""
    tester = ConfigTester()
    success = tester.run_all_tests()
    
    if success:
        info("✅ Configuração validada com sucesso!")
        return 0
    else:
        error("❌ Falhas na configuração encontradas!")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main()) 