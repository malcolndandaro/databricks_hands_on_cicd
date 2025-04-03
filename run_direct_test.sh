#!/bin/bash

# Verifica se as dependências estão instaladas
echo "Verificando dependências..."
if ! pip list | grep -q "pytest"; then
  echo "Instalando dependências para o teste..."
  pip install pytest pyspark
fi

# Executa o teste direto
echo "Executando teste de integração direto..."
pytest tests/test_create_table_job_direct.py -v

# Verifica o resultado do teste
if [ $? -eq 0 ]; then
  echo "Teste executado com sucesso! A tabela e schema foram validados."
else
  echo "ERRO: O teste falhou. Verifique se o script create_table_job.py está funcionando corretamente."
fi 