import sys
import os
import pytest
import yaml
from unittest.mock import MagicMock
from pathlib import Path

# Adiciona diretório raiz ao path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Configura os mocks básicos necessários
class TestesSimples:
    
    def setup_method(self):
        """Configuração básica para cada teste"""
        # Mock básico para dbutils
        self.dbutils = MagicMock()
        self.spark = MagicMock()
        self.display = MagicMock()
        
        # Configura o mock de nome de usuário
        user_mock = MagicMock()
        user_mock.toString.return_value = "Usuário Teste (usuario_teste@databricks.com)"
        self.dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value.userName.return_value = user_mock
        
        # Adiciona ao builtins
        import builtins
        builtins.dbutils = self.dbutils
        builtins.spark = self.spark
        builtins.display = self.display
        
        # Limpa o módulo do cache se existir
        if 'create_table_job' in sys.modules:
            del sys.modules['create_table_job']
    
    def teardown_method(self):
        """Limpeza após cada teste"""
        import builtins
        if hasattr(builtins, 'dbutils'):
            delattr(builtins, 'dbutils')
        if hasattr(builtins, 'spark'):
            delattr(builtins, 'spark')
        if hasattr(builtins, 'display'):
            delattr(builtins, 'display')
    
    def test_nome_do_schema(self):
        """Testa se o nome do schema é extraído corretamente"""
        # Importa a função que queremos testar
        from create_table_job import get_schema_name
        
        # Executa o teste
        nome_schema = get_schema_name()
        assert nome_schema == "usuario_teste"
    
    def test_criacao_dataframe(self):
        """Testa se o DataFrame é criado com a estrutura correta"""
        # Configura o widget para retornar o catálogo
        self.dbutils.widgets.get.return_value = "catalogo_teste"
        
        # Configura o mock do DataFrame
        df_mock = MagicMock()
        self.spark.createDataFrame.return_value = df_mock
        df_mock.write.format.return_value.mode.return_value.saveAsTable = MagicMock()
        
        # Importa o módulo
        import create_table_job
        
        # Verifica se o createDataFrame foi chamado com os parâmetros corretos
        self.spark.createDataFrame.assert_called_once()
        
        # Extrai os argumentos da chamada
        args = self.spark.createDataFrame.call_args[0]
        data = args[0]
        
        # Verifica a estrutura dos dados
        assert len(data) == 6, "Devem existir 6 linhas de dados"
        assert len(data[0]) == 5, "Cada linha deve ter 5 colunas"
    
    def test_execucao_de_queries_sql(self):
        """Testa se as queries SQL são executadas corretamente"""
        # Configura o widget para retornar o catálogo
        catalogo = "catalogo_teste"
        self.dbutils.widgets.get.return_value = catalogo
        
        # Rastreia as queries SQL
        queries = []
        def mock_sql(query):
            queries.append(query)
            return MagicMock()
        
        self.spark.sql = mock_sql
        
        # Importa o módulo
        import create_table_job
        
        # Verifica as queries essenciais
        assert any(f"USE CATALOG {catalogo}" in q for q in queries), "Deve usar o catálogo correto"
        assert any("CREATE OR REPLACE TABLE funcionarios" in q for q in queries), "Deve criar a tabela funcionarios"
    
    def test_integracao_com_bundle(self):
        """Testa a integração com as variáveis do bundle para ambientes diferentes"""
        # Carrega a configuração do bundle
        bundle_path = os.path.join(PROJECT_ROOT, 'databricks.yml')
        with open(bundle_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Testa para ambiente de dev
        catalogo_dev = config['targets']['dev']['variables']['catalogo']
        self.dbutils.widgets.get.return_value = catalogo_dev
        
        queries_dev = []
        def mock_sql_dev(query):
            queries_dev.append(query)
            return MagicMock()
        
        self.spark.sql = mock_sql_dev
        
        # Importa o módulo com configuração de dev
        if 'create_table_job' in sys.modules:
            del sys.modules['create_table_job']
        import create_table_job
        
        # Verifica se usa o catálogo de dev
        assert any(f"USE CATALOG {catalogo_dev}" in q for q in queries_dev), f"Deve usar o catálogo de DEV: {catalogo_dev}"
        
        # Limpa para o próximo teste
        self.teardown_method()
        self.setup_method()
        
        # Testa para ambiente de qa
        catalogo_qa = config['targets']['qa']['variables']['catalogo']
        self.dbutils.widgets.get.return_value = catalogo_qa
        
        queries_qa = []
        def mock_sql_qa(query):
            queries_qa.append(query)
            return MagicMock()
        
        self.spark.sql = mock_sql_qa
        
        # Importa o módulo com configuração de qa
        if 'create_table_job' in sys.modules:
            del sys.modules['create_table_job']
        import create_table_job
        
        # Verifica se usa o catálogo de qa
        assert any(f"USE CATALOG {catalogo_qa}" in q for q in queries_qa), f"Deve usar o catálogo de QA: {catalogo_qa}" 