# GitHub Actions para CI/CD com Databricks

Este documento explica como o GitHub Actions é utilizado para automatizar o processo de CI/CD (Integração Contínua/Entrega Contínua) com Databricks neste projeto.

## Índice

1. [Visão Geral](#visão-geral)
2. [Fluxo de Trabalho](#fluxo-de-trabalho)
3. [Configuração das Variáveis de Ambiente](#configuração-das-variáveis-de-ambiente)
4. [Workflows Principais](#workflows-principais)
5. [Como Configurar seu Próprio Pipeline](#como-configurar-seu-próprio-pipeline)

## Visão Geral

O GitHub Actions é utilizado neste projeto para automatizar:

- Validação de código (qualidade e formatação)
- Validação de bundle Databricks
- Deployment para ambientes (QA/Prod)
- Execução de jobs após deployment
- Execução de testes unitários

A automação ocorre quando pull requests são abertos para branches específicas ou quando ocorrem pushes diretos para essas branches.

## Fluxo de Trabalho

O fluxo de CI/CD implementado segue esta sequência:

1. **Desenvolvimento local** - Desenvolvedores trabalham em suas branches locais
2. **Pull Request para QA** - Ao abrir um PR para a branch `qa`
   - Execução de verificação de qualidade de código (Flake8)
   - Validação do formato do bundle Databricks
   - Deploy automático para ambiente de QA
   - Execução do job no ambiente QA para validação
   - Execução de testes unitários e de integração
3. **Merge para QA** - Após aprovação, o código é mesclado para a branch `qa`
4. **Promoção para Produção** - Processo similar para promover de QA para Produção

## Configuração das Variáveis de Ambiente

### Segredos do GitHub

Os workflows utilizam secrets do GitHub para armazenar informações sensíveis:

- `DATABRICKS_HOST_HML`: URL do workspace Databricks de homologação
- `DATABRICKS_TOKEN_HML`: Token de acesso para o workspace de homologação
- `DATABRICKS_HOST_PROD`: URL do workspace Databricks de produção (se aplicável)
- `DATABRICKS_TOKEN_PROD`: Token de acesso para o workspace de produção (se aplicável)

### Como configurar os segredos

1. Navegue até as configurações do repositório no GitHub
2. Vá para "Settings" > "Secrets and variables" > "Actions"
3. Clique em "New repository secret"
4. Adicione cada um dos segredos mencionados acima

## Workflows Principais

### Workflow de Validação e Deploy para QA

O arquivo `.github/workflows/validate-deploy-qa.yml` define o workflow acionado em PRs para a branch `qa`.

Este workflow:

1. **Code Quality Check**:
   - Instala e executa Flake8 para verificar a qualidade do código Python
   - Reporta quaisquer problemas encontrados

2. **Validate Bundle**:
   - Instala a CLI do Databricks
   - Valida o formato do bundle com `databricks bundle validate -t qa`

3. **Deploy to QA**:
   - Realiza o deploy do bundle para o ambiente QA com `databricks bundle deploy -t qa`

4. **Run Job in QA**:
   - Executa o job implantado para validar seu funcionamento
   - Usa `databricks bundle run lab_cicd_criar_tabela_funcionario -t qa`

5. **Run Unit Tests**:
   - Executa testes unitários usando pytest para validar a funcionalidade

## Como Configurar seu Próprio Pipeline

Para configurar este pipeline no seu próprio projeto:

1. **Crie a estrutura de diretórios**:
   ```
   .github/
     workflows/
       validate-deploy-qa.yml
   ```

2. **Crie o arquivo de workflow**:
   - Copie e adapte o arquivo `validate-deploy-qa.yml` de acordo com suas necessidades
   - Ajuste os nomes de jobs, targets e comandos conforme necessário

3. **Configure os secrets**:
   - Adicione os tokens e hosts do Databricks como secrets do repositório

4. **Ajuste o databricks.yml**:
   - Configure seus targets (dev, qa, prod) no arquivo `databricks.yml`
   - Defina as variáveis específicas para cada ambiente

5. **Conecte as branches**:
   - Configure as regras de proteção de branch para QA e Prod
   - Exija aprovações de PR antes do merge
   
## Benefícios do CI/CD Automatizado

Ao utilizar o GitHub Actions para CI/CD com Databricks, você obtém:

- **Consistência**: Cada ambiente (dev, qa, prod) usa exatamente o mesmo código
- **Rastreabilidade**: Todas as alterações são rastreadas através de commits e PRs
- **Automação**: Redução do trabalho manual de deployment e testes
- **Qualidade**: Verificações automáticas de qualidade de código e testes
- **Segurança**: Controle de acesso restrito a ambientes de produção

---

Para mais detalhes sobre o Databricks CLI e a sintaxe de bundle, consulte a [documentação oficial do Databricks](https://docs.databricks.com/dev-tools/bundles/index.html). 