# CI/CD Hands-On

Este repositório contém o código e configurações necessárias para o workshop de CI/CD com Databricks.

## Primeiros Passos

### Importando um Job Existente

Para começar, você precisará importar um job existente do Databricks. Use o seguinte comando:

```bash
databricks bundle generate job --existing-job-id 663063874671210 -t dev
```

Onde:
- `--existing-job-id 663063874671210` especifica o ID do job que você deseja importar
- `-t dev` especifica o ambiente de destino (target environment) de onde o job será importado. Neste caso, estamos importando do ambiente de desenvolvimento (`dev`).

Este comando irá gerar os arquivos de configuração necessários para que você possa trabalhar com o job localmente e, posteriormente, implantá-lo em diferentes ambientes através do pipeline de CI/CD.

#### Exemplo de Execução

Quando executamos o comando para importar um job existente, podemos ver que o conteúdo do job é importado para as respectivas pastas do projeto:

```bash
> databricks bundle generate job --existing-job-id 393880860618601 -t dev -p itaudev
File successfully saved to src/create_table_job.py
Job configuration successfully saved to resources/rls_column_masking_job.yml
```

Como podemos observar:
- O arquivo SQL do job foi salvo no diretório `src/`
- A configuração do job foi salva no diretório `resources/`

## Configuração de Variáveis de Ambiente

### Configuração do Catálogo

No job importado (lab_cicd_criar_tabela_customers.job.yml), você pode observar que usamos variáveis parametrizadas para garantir a portabilidade entre diferentes ambientes. Uma configuração chave é a referência ao catálogo usando a sintaxe de variável:

```
catalogo: ${catalogo}
```

Essa abordagem permite que o mesmo job seja executado em diferentes ambientes (dev, qa, prod), utilizando o catálogo apropriado para cada um. O valor da variável `${catalogo}` é substituído automaticamente durante a implantação, com base no ambiente de destino.

### Como Funciona

1. No arquivo `databricks.yml`, definimos os diferentes ambientes e suas respectivas configurações.
2. Para cada ambiente (target), especificamos o valor que deve ser usado para a variável `catalogo`.
3. Quando implantamos ou executamos o job em um ambiente específico, o sistema substitui a variável `${catalogo}` pelo valor configurado para aquele ambiente.

Por exemplo:
- No ambiente `dev`, `${catalogo}` pode ser substituído por `catalogo_dev`
- No ambiente `qa`, `${catalogo}` pode ser substituído por `catalogo_qa`
- No ambiente `prod`, `${catalogo}` pode ser substituído por `catalogo_prod`

### Exemplo de Deployment com Target Environment

Para fazer o deployment do job para um ambiente específico, use o comando:

```bash
databricks bundle deploy -t dev
```

Este comando implantará o job no ambiente de desenvolvimento (`dev`), substituindo a variável `${catalogo}` pelo valor definido para este ambiente no arquivo `databricks.yml`.

Para implantar em outros ambientes, basta alterar o valor do parâmetro `-t`:

```bash
databricks bundle deploy -t qa     # Deploy para ambiente de QA
databricks bundle deploy -t prod   # Deploy para ambiente de produção
```

Essa abordagem elimina a necessidade de manter diferentes versões do código para cada ambiente, simplificando a manutenção e reduzindo a chance de erros.

### Resultado do Deployment

Após executar o comando de deployment para um ambiente específico (neste caso, para o ambiente `dev`), o job será criado no Databricks com o usuário logado como proprietário (owner). Como podemos ver na imagem abaixo, o job "[dev_alfeu_duran] lab_cicd_criar_tabela_customers" foi criado com o usuário "Alfeu Duran" como criador:

![Job criado no Databricks](images/workflow.png)

Observe que:
- O nome do job inclui um prefixo indicando o ambiente (`[dev_alfeu_duran]`)
- O usuário que executou o comando de deployment é automaticamente definido como o proprietário do job
- As tags do job são preservadas durante o deployment

Isso facilita a identificação de quem é responsável por cada job e em qual ambiente ele está implantado.

## Estrutura do Repositório

- `databricks.yml` - Arquivo de configuração principal do Databricks
- `src/` - Contém o código fonte dos notebooks e jobs
- `resources/` - Contém recursos adicionais necessários para os jobs

---

*Nota: Esta documentação será complementada ao longo do desenvolvimento do projeto.* 

## Executando Testes Unitários

Este projeto utiliza pytest para testes unitários e o Databricks Connect para executar testes que interagem com um ambiente Databricks.

### Pré-requisitos para os testes

1. Instalação das dependências de teste:
   ```bash
   pip install -r requirements-test.txt
   ```

2. Configuração do Databricks Connect:
   
   Exporte as variáveis de ambiente necessárias:
   ```bash
   export DATABRICKS_HOST=<seu-host-databricks> # Ex: https://adb-123456789.4.azuredatabricks.net
   export DATABRICKS_TOKEN=<seu-token-de-acesso>
   ```

   Ou configure utilizando a CLI do Databricks:
   ```bash
   databricks configure --token
   ```

### Executando os testes

Para executar todos os testes:
```bash
pytest tests/
```

Para executar um teste específico:
```bash
pytest tests/test_create_table_job.py
```

Para executar com informações detalhadas:
```bash
pytest tests/ -v
```

### Estrutura dos testes

Os testes são organizados da seguinte forma:

- `tests/test_create_table_job.py`: Testes para validar a criação de tabelas e manipulação de dados no Databricks

Os testes utilizam um ambiente isolado no Unity Catalog para não interferir com os dados de produção. 

### Testes Simplificados para Laboratório

Para fins de demonstração e laboratório, também fornecemos uma versão simplificada dos testes que não requer configuração do Databricks Connect:

```bash
# Instalar dependências mínimas
pip install -r requirements-test-simple.txt

# Executar os testes simplificados
pytest tests/test_create_table_job_simple.py -v

# Ou usar o script auxiliar
./run_simple_tests.sh
```

Estes testes simplificados usam apenas mocks e são ideais para demonstrar:
1. Como testar a função de obtenção do nome do esquema
2. Como verificar a recuperação correta do catálogo a partir dos widgets
3. Como validar os comandos SQL executados

Eles são mais rápidos e não exigem conexão com o Databricks, tornando-os ideais para ambientes de laboratório. 

### Teste Direto de Integração

Também fornecemos um teste de integração direto que executa o script Python e valida se a tabela e schema foram criados corretamente:

```bash
# Executar o teste direto
pytest tests/test_create_table_job_direct.py -v

# Ou usar o script auxiliar
./run_direct_test.sh
```

Este teste:
1. Executa o script create_table_job.py diretamente
2. Verifica se a tabela foi criada com o schema correto
3. Valida se todos os dados foram inseridos corretamente
4. Verifica um registro específico que deve estar presente na tabela

Este teste é ideal para demonstrar como validar os resultados reais produzidos pelo script, sem usar mocks. 