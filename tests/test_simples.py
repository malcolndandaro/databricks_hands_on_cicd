import sys
import os
import pytest
import yaml
import builtins
import re
from unittest.mock import MagicMock
from pathlib import Path

# Adiciona diretório raiz ao path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Valores padrão esperados para os catálogos
EXPECTED_DEV_CATALOG = "dev_hands_on"
EXPECTED_QA_CATALOG = "hml_hands_on"

# Nome esperado para a tabela
EXPECTED_TABLE_NAME = "funcionarios"

# Configura os mocks básicos necessários
class TestesSimples:
    
    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        """Configuração básica para cada teste com cleanup automático"""
        # Mock básico para dbutils, spark e display
        self.dbutils = MagicMock()
        self.spark = MagicMock()
        self.display = MagicMock()
        
        # Configura o mock de nome de usuário
        user_mock = MagicMock()
        user_mock.toString.return_value = "Usuário Teste (usuario_teste@databricks.com)"
        self.dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value.userName.return_value = user_mock
        
        # Adiciona ao builtins
        builtins.dbutils = self.dbutils
        builtins.spark = self.spark
        builtins.display = self.display
        
        # Limpa o módulo do cache se existir
        if 'create_table_job' in sys.modules:
            del sys.modules['create_table_job']
            
        # Executa o teste
        yield
        
        # Limpeza após o teste
        if hasattr(builtins, 'dbutils'):
            delattr(builtins, 'dbutils')
        if hasattr(builtins, 'spark'):
            delattr(builtins, 'spark')
        if hasattr(builtins, 'display'):
            delattr(builtins, 'display')
    
    
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
        
        # Verifica se a tabela funcionarios foi criada com validação mais precisa
        create_table_queries = [q for q in queries if "CREATE OR REPLACE TABLE" in q]
        assert create_table_queries, "Deve existir ao menos uma query de criação de tabela"
        
        # Extrai os nomes das tabelas criadas usando expressão regular
        table_names = []
        for query in create_table_queries:
            match = re.search(r"CREATE OR REPLACE TABLE\s+(\w+)", query)
            if match:
                table_names.append(match.group(1))
        
        # Verifica se a tabela esperada foi criada
        assert EXPECTED_TABLE_NAME in table_names, \
            f"A tabela {EXPECTED_TABLE_NAME} não foi criada. Tabelas encontradas: {table_names}"
    
    @pytest.fixture
    def setup_bundle_config(self):
        """Carrega a configuração do bundle"""
        bundle_path = os.path.join(PROJECT_ROOT, 'databricks.yml')
        with open(bundle_path, 'r') as file:
            return yaml.safe_load(file)
    
    def test_valores_catalogos_fixos(self, setup_bundle_config):
        """Verifica se os valores dos catálogos permanecem com os valores esperados"""
        config = setup_bundle_config
        
        # Verifica ambiente DEV
        catalogo_dev = config['targets']['dev']['variables']['catalogo']
        assert catalogo_dev == EXPECTED_DEV_CATALOG, \
            f"O catálogo de DEV foi alterado! Esperado: '{EXPECTED_DEV_CATALOG}', Encontrado: '{catalogo_dev}'. " \
            f"Não altere o valor do catálogo no databricks.yml!"
        
        # Verifica ambiente QA
        catalogo_qa = config['targets']['qa']['variables']['catalogo']
        assert catalogo_qa == EXPECTED_QA_CATALOG, \
            f"O catálogo de QA foi alterado! Esperado: '{EXPECTED_QA_CATALOG}', Encontrado: '{catalogo_qa}'. " \
            f"Não altere o valor do catálogo no databricks.yml!"
    
    # def test_validate_cpf_dataframe(self):
    #     """Testa se a função validate_cpf_dataframe valida corretamente os CPFs"""
    #     # Configura o widget para retornar o catálogo
    #     self.dbutils.widgets.get.return_value = "catalogo_teste"
        
    #     # Importa a função a ser testada
    #     from create_table_job import validate_cpf_dataframe
        
    #     # Dados de teste com CPFs válidos e inválidos
    #     data = [
    #         (1, "CPF Válido 1", "123.456.789-09"),    # CPF válido
    #         (2, "CPF Válido 2", "111.444.777-35"),    # CPF válido
    #         (3, "CPF Inválido Dígitos", "123.456.789-00"),  # CPF inválido (dígitos verificadores incorretos)
    #         (4, "CPF Inválido Formato", "123456"),    # CPF inválido (formato incorreto)
    #         (5, "CPF Inválido Repetido", "111.111.111-11")  # CPF inválido (todos dígitos iguais)
    #     ]
        
    #     # Cria schema para o DataFrame de teste
    #     from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    #     schema = StructType([
    #         StructField("id", IntegerType(), False),
    #         StructField("nome", StringType(), False),
    #         StructField("cpf", StringType(), False)
    #     ])
        
    #     # Configura os mocks para simular o comportamento do Spark
    #     df_mock = MagicMock()
    #     df_resultado = MagicMock()
        
    #     # Configura o comportamento do withColumn
    #     df_mock.withColumn.return_value = df_resultado
        
    #     # Executa a função de validação
    #     resultado = validate_cpf_dataframe(df_mock)
        
    #     # Verifica se withColumn foi chamado para adicionar a coluna de validação
    #     df_mock.withColumn.assert_called_once()
    #     nome_coluna, _ = df_mock.withColumn.call_args[0]
    #     assert nome_coluna == "cpf_valido", "A coluna adicionada deve se chamar 'cpf_valido'"
        
    #     # Verifica se o resultado é o DataFrame validado
    #     assert resultado == df_resultado
    
    def test_validate_cpf_logic(self):
        """Testa diretamente a lógica de validação de CPF da função no módulo"""
        # Importa a função a ser testada
        from create_table_job import validate_cpf
        
        # CPFs válidos para teste
        cpfs_validos = [
            "123.456.789-09",
            "111.444.777-35"
        ]
        
        # CPFs inválidos para teste
        cpfs_invalidos = [
            "123.456.789-00",  # dígitos verificadores incorretos
            "123456",          # formato incorreto (comprimento)
            "111.111.111-11"   # dígitos repetidos
        ]
        
        # Testa CPFs válidos
        for cpf in cpfs_validos:
            assert validate_cpf(cpf) is True, f"CPF válido {cpf} não foi reconhecido"
        
        # Testa CPFs inválidos
        for cpf in cpfs_invalidos:
            assert validate_cpf(cpf) is False, f"CPF inválido {cpf} foi incorretamente validado"
        