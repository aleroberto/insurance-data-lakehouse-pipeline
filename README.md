# insurance-data-lakehouse-pipeline

Projeto de Engenharia de Dados que simula um pipeline regulatório de seguros utilizando arquitetura **Data Lakehouse**.

O objetivo do projeto é reproduzir conceitos utilizados em ambientes corporativos de missão crítica, incluindo ingestão de dados, processamento distribuído com Apache Spark, rastreabilidade, particionamento, camadas analíticas, qualidade de dados e orquestração de pipelines.

---

# Objetivo arquitetural

O projeto busca simular uma plataforma moderna de engenharia de dados utilizando boas práticas aplicadas em ambientes corporativos:

* Arquitetura Data Lakehouse
* Processamento distribuído com Apache Spark
* Separação de camadas Raw, Bronze, Silver e Gold
* Rastreabilidade de dados
* Metadados técnicos
* Padronização de pipelines
* Configuração centralizada
* Preparação para execução em ambientes cloud

---

# Stack

* Python
* PySpark
* Apache Spark
* Docker
* MinIO
* PostgreSQL
* Parquet
* SQL

---

# Arquitetura

Fluxo principal de processamento:

```text
                 Ingestion
                    |
                    v
              data/raw
                    |
                    v
              Spark Processing
                    |
                    v
             data/bronze
                    |
                    v
             data/silver
                    |
                    v
              data/gold
```

---

# Domínios simulados

O projeto utiliza datasets sintéticos inspirados em operações regulatórias de seguros:

* policies
* claims
* financial_flows

---

# Camada Raw

A camada Raw representa os dados originais recebidos pelos processos de ingestão.

Características:

* Dados em formato original
* Preservação da fonte
* Organização por domínio
* Particionamento por data de ingestão

Exemplo:

```text
data/raw/policies/
└── ingestion_date=YYYY-MM-DD/
    └── policies_YYYY-MM-DD.csv
```

---

# Camada Bronze

A camada Bronze é responsável pelo primeiro processamento dos dados utilizando Apache Spark.

Responsabilidades:

* Leitura distribuída dos arquivos Raw
* Conversão CSV → Parquet
* Compressão Snappy
* Particionamento dos dados
* Enriquecimento com metadados técnicos
* Controle de batch de processamento

Metadados adicionados:

* ingestion_timestamp
* processing_date
* source_system
* source_dataset
* batch_id

Exemplo:

```text
data/bronze/policies/

└── processing_date=YYYY-MM-DD/
    └── part-xxxxx.snappy.parquet
```

---

# Configuração centralizada

As configurações do pipeline são centralizadas no módulo:

```text
src/common/config.py
```

Esse módulo concentra parâmetros utilizados pelos processos:

* Caminhos das camadas do Data Lake
* Configurações Spark
* Formato dos arquivos
* Compressão
* Parâmetros gerais

Exemplo:

```python
RAW_PATH

BRONZE_PATH

SILVER_PATH

GOLD_PATH

FILE_FORMAT = "parquet"

COMPRESSION = "snappy"
```

Essa abordagem evita configurações fixas espalhadas pelos scripts e facilita a manutenção e evolução do pipeline.

---

# Estrutura do projeto

```text
insurance-data-lakehouse-pipeline/

│
├── src/
│   │
│   ├── common/
│   │   └── config.py
│   │
│   ├── generators/
│   │
│   ├── ingestion/
│   │   └── ler_dados.py
│   │
│   ├── bronze/
│   │   └── raw_to_bronze.py
│   │
│   ├── silver/
│   │
│   ├── gold/
│   │
│   └── quality/
│
│
├── data/
│   │
│   ├── raw/
│   │
│   ├── bronze/
│   │
│   ├── silver/
│   │
│   └── gold/
│
│
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

# Execução com Docker

O projeto utiliza ambiente containerizado para execução do Apache Spark.

## Subir os containers

```bash
docker compose up -d
```

---

## Acessar o container Spark

```bash
docker exec -it insurance_spark bash
```

---

## Executar pipeline Bronze

Dentro do container:

```bash
/opt/spark/bin/spark-submit src/bronze/raw_to_bronze.py
```

---

# Dependências Python

O projeto utiliza estrutura de pacote Python através do:

```text
pyproject.toml
```

Instalação em modo desenvolvimento:

```bash
pip install -e .
```

Isso permite utilização de imports organizados entre os módulos do projeto.

---

# Funcionalidades implementadas

* Estrutura inicial do projeto
* Ambiente Docker Compose
* Spark containerizado
* Geração de datasets sintéticos
* Processo de ingestão de dados
* Conversão Raw → Bronze
* Processamento distribuído com PySpark
* Escrita em formato Parquet
* Compressão Snappy
* Particionamento por data
* Inclusão de metadados técnicos
* Processamento batch
* Configuração centralizada
* Estrutura Python baseada em pacotes

---

# Próximos passos

Evoluções planejadas:

## Silver Layer

* Limpeza e padronização dos dados
* Tratamento de qualidade
* Normalização de campos
* Regras de negócio

## Gold Layer

* Criação de tabelas analíticas
* Métricas regulatórias
* Data marts
* Modelagem dimensional

## Data Quality

* Validação de schema
* Monitoramento de qualidade
* Regras de consistência
* Checks automatizados

## Orquestração

* Apache Airflow
* Scheduling de pipelines
* Controle de dependências
* Monitoramento de execução

## Camada analítica

* Integração com PostgreSQL
* Consultas analíticas
* Exposição para ferramentas BI

## Cloud

Preparação para execução em ambientes como:

* AWS S3
* AWS Glue
* Amazon EMR
* Lake Formation

---

# Licença

Projeto desenvolvido para fins educacionais e demonstração de arquitetura de Engenharia de Dados.
