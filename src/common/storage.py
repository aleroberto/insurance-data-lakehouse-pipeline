MINIO_ENDPOINT = "http://insurance_minio:9000"

MINIO_ACCESS_KEY = "minio"

MINIO_SECRET_KEY = "minio123"

LAKE_BUCKET = "insurance-lake"

WAREHOUSE_PATH = (
    f"s3a://{LAKE_BUCKET}/warehouse"
)