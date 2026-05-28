# insurance-data-lakehouse-pipeline

Projeto de engenharia de dados que simula um pipeline regulatório de seguros utilizando arquitetura Data Lakehouse.

O objetivo do projeto é reproduzir conceitos utilizados em ambientes corporativos de missão crítica, incluindo ingestão distribuída de dados, processamento com Spark, rastreabilidade, particionamento, camadas analíticas e orquestração de pipelines.

---

# Stack

* Python
* PySpark
* Docker
* Apache Spark
* MinIO
* PostgreSQL
* Parquet
* SQL

---

# Arquitetura

```text
data/raw
   ↓
Spark
   ↓
data/bronze
   ↓
data/silver
   ↓
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

Armazena os arquivos originais gerados pelos scripts de ingestão.

Exemplo:

```text
data/raw/policies/ingestion_date=YYYY-MM-DD/
```

---

# Camada Bronze

A camada Bronze é responsável por:

* leitura distribuída com Spark
* conversão CSV → Parquet
* compressão Snappy
* particionamento
* adição de metadados técnicos

Metadados adicionados:

* ingestion_timestamp
* processing_date
* source_system
* source_dataset
* batch_id

Exemplo:

```text
data/bronze/policies/
└── processing_date=2026-05-28/
```

---

# Execução com Docker

Subir containers:

```bash
docker compose up -d
```

Entrar no container Spark:

```bash
docker exec -it insurance_spark bash
```

Executar pipeline Bronze:

```bash
/opt/spark/bin/spark-submit src/bronze/raw_to_bronze.py
```

---

# Estrutura inicial

```text
src/
  generators/
  bronze/
  silver/
  gold/
  quality/

data/
  raw/
  bronze/
  silver/
  gold/
```

---

# Funcionalidades implementadas

* Estrutura inicial do projeto
* Docker Compose
* Spark containerizado
* Geração de datasets sintéticos
* Conversão Raw → Bronze
* Escrita em Parquet
* Compressão Snappy
* Particionamento por data
* Metadados técnicos
* Batch processing

---

# Próximos passos

* Silver Layer
* Gold Layer
* Data Quality
* Airflow orchestration
* Analytical marts
* Data governance
* MinIO integration
* PostgreSQL analytical serving layer
