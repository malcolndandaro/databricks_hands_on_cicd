import pytest
import os
import sys
import importlib.util
from databricks.connect import DatabricksSession 

# Adiciona o diretório src ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

# Cria uma sessão Spark para testes
@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark para os testes."""
    spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()
    
    yield spark
    
    # Encerra a sessão depois dos testes
    spark.stop()

def test_table_creation(spark_session):
    """
    Testa se o script cria a tabela corretamente com o schema esperado
    e se os dados são inseridos conforme esperado.
    """
    # Caminho para o script a ser testado
    script_path = os.path.join(os.path.dirname(__file__), "..", "src", "create_table_job.py")
    
    
    # Verifica se a tabela foi criada
    catalog_name = "hml_hands_on"
    schema_name = "alfeu_duran"
    table_name = "funcionarios"
    
    # Consulta a tabela criada
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    df = spark_session.sql(f"SELECT * FROM {full_table_name}")
    
    # Verifica se a tabela tem dados
    row_count = df.count()
    assert row_count >= 6, f"A tabela deve ter pelo menos 6 registros, mas tem {row_count}"
    
    # Verifica o schema da tabela
    columns = df.columns
    expected_columns = ["id", "nome", "departamento", "salario", "cpf"]
    for column in expected_columns:
        assert column in columns, f"A coluna {column} não está presente na tabela"
    
    # Verifica se existem valores nulos no campo CPF
    null_cpf_count = df.filter("cpf IS NULL").count()
    assert null_cpf_count == 0, f"Existem {null_cpf_count} registros com CPF nulo"
    
    # Verifica um registro específico (o último registro contém o nome do schema)
    user_record = df.filter(f"nome = '{schema_name}'").collect()
    assert len(user_record) == 1, f"Não foi encontrado registro com nome = {schema_name}"
    assert user_record[0]["departamento"] == "Financeiro", f"O departamento deveria ser 'Financeiro'" 