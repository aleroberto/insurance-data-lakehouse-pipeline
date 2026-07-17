from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PATH = PROJECT_ROOT / "data/raw"
BRONZE_PATH = PROJECT_ROOT / "data/bronze"
SILVER_PATH = PROJECT_ROOT / "data/silver"
GOLD_PATH = PROJECT_ROOT / "data/gold"
FILE_FORMAT = "parquet"
COMPRESSION = "snappy"

SPARK_APP_NAME = "insurance-raw-to-bronze"

SPARK_MASTER = "local[*]"
MINIO_ENDPOINT = "http://insurance_minio:9000"

MINIO_ACCESS_KEY = "minio"

MINIO_SECRET_KEY = "minio123"

LAKEHOUSE_BUCKET = "insurance-lake"