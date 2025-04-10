# Databricks notebook source

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
from pyspark.sql.functions import udf, col, regexp_replace, length


catalog_name = dbutils.widgets.get("catalogo")

print(f"Using catalog: {catalog_name}")

# Get username and format for schema name


def validate_cpf(cpf):
    """
    Validates a Brazilian CPF number.
    
    Args:
        cpf (str): The CPF string to validate
        
    Returns:
        bool: True if the CPF is valid, False otherwise
    """
    # Remove non-digit characters
    cpf = ''.join(filter(str.isdigit, cpf))
    
    # Check if it has 11 digits
    if len(cpf) != 11:
        return False
    
    # Check if all digits are the same (invalid case)
    if len(set(cpf)) == 1:
        return False
    
    # Calculate first verification digit
    sum_of_products = sum(int(cpf[i]) * (10 - i) for i in range(9))
    expected_digit1 = 0 if sum_of_products % 11 < 2 else 11 - (sum_of_products % 11)
    
    # Check first verification digit
    if int(cpf[9]) != expected_digit1:
        return False
    
    # Calculate second verification digit
    sum_of_products = sum(int(cpf[i]) * (11 - i) for i in range(10))
    expected_digit2 = 0 if sum_of_products % 11 < 2 else 11 - (sum_of_products % 11)
    
    # Check second verification digit
    if int(cpf[10]) != expected_digit2:
        return False
    
    return True

def validate_cpf_dataframe(df):
    """
    Adds a validation column to a DataFrame indicating if the CPF is valid.
    
    Args:
        df (DataFrame): A Spark DataFrame containing a 'cpf' column
        
    Returns:
        DataFrame: The input DataFrame with a validation column added
    """
    # Register the UDF
    validate_cpf_udf = udf(validate_cpf, BooleanType())
    
    # Add validation column
    return df.withColumn("cpf_valido", validate_cpf_udf(col("cpf")))


def get_schema_name():
    user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName()
    user_name = user_name.toString().split('@')[0].split('(')[1].replace('.', '_')
    return user_name


schema_name = get_schema_name()
print(f"Using schema: {schema_name}")

# Set up catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Create the table
create_table_sql = """
CREATE OR REPLACE TABLE funcionarios (
  id INT,
  nome STRING,
  departamento STRING,
  salario DOUBLE,
  cpf STRING
) USING DELTA
"""

spark.sql(create_table_sql)

# Prepare data to insert
data = [
    (1, "João Silva", "TI", 5000.00, "123.456.789-00"),
    (2, "Maria Santos", "RH", 4500.00, "987.654.321-00"),
    (3, "Pedro Oliveira", "TI", 4800.00, "456.789.123-00"),
    (4, "Ana Costa", "Financeiro", 5500.00, "789.123.456-00"),
    (5, "Carlos Santos", "Financeiro", 6000.00, "321.654.987-00"),
    (6, schema_name, "Financeiro", 6000.00, "123.456.789-10")
    
    
    
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("nome", StringType(), False),
    StructField("departamento", StringType(), False),
    StructField("salario", DoubleType(), False),
    StructField("cpf", StringType(), False)
])

df = spark.createDataFrame(data, schema)

df = df.transform(validate_cpf_dataframe)

# Save DataFrame as a table
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("funcionarios")



