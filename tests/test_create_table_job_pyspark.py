import os


import pytest
import sys
from databricks.connect import DatabricksSession 
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

# Add src directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

@pytest.fixture(scope="module")
def spark_fixture():
    """Create a Spark session for testing."""
    spark = DatabricksSession.builder.profile("itauhml").serverless(True).getOrCreate()
    yield spark
    spark.stop()

def test_table_creation_with_pyspark_testing(spark_fixture):
    """
    Test if the table is created correctly with the expected schema
    and if the data is inserted as expected using pyspark.testing utilities.
    """
    # Table information
    catalog_name = "hml_hands_on"
    schema_name = "alfeu_duran"
    table_name = "funcionarios"
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    # Query the created table
    df = spark_fixture.sql(f"SELECT * FROM {full_table_name}")
    
    # Test the schema
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("departamento", StringType(), True),
        StructField("salario", DoubleType(), True),
        StructField("cpf", StringType(), True),
        StructField("cpf_valido", BooleanType(), True)
    ])
    
    assertSchemaEqual(df.schema, expected_schema)
    
    # Test for minimum row count
    assert df.count() >= 6, f"The table should have at least 6 records"
    
    # Test for null CPF values
    null_cpf_df = df.filter("cpf IS NULL")
    expected_null_cpf_df = spark_fixture.createDataFrame([], df.schema)
    assertDataFrameEqual(null_cpf_df, expected_null_cpf_df)
    
    # Test for specific record (with user's schema name)
    user_record_df = df.filter(f"nome = '{schema_name}'")
    
    # Create expected dataframe for the user record
    # We only know partial information about this record
    expected_data = [{
        "nome": schema_name,
        "departamento": "Financeiro"
    }]
    
    # Ensure column order is consistent by explicitly selecting columns in the same order
    column_order = ["nome", "departamento"]
    user_record_subset = user_record_df.select(*column_order)
    
    # Create expected DataFrame with the same schema in the same order
    expected_schema = StructType([
        StructField("nome", StringType(), True),
        StructField("departamento", StringType(), True)
    ])
    expected_df = spark_fixture.createDataFrame(expected_data, schema=expected_schema)
    
    assertDataFrameEqual(user_record_subset, expected_df) 