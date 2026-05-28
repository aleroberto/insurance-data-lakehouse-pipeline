from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, current_date


def create_spark_session():
    return (
        SparkSession.builder
        .appName("insurance-raw-to-bronze")
        .master("local[*]")
        .getOrCreate()
    )


def log_info(message, layout):
    print(
        f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} "
        f"INFO Process{layout.title()}ToBronze: {message}"
    )


def process_to_bronze(spark, layout):
    path_root = Path(__file__).resolve().parents[2]

    input_path = f"{path_root}/data/raw/{layout}/ingestion_date=*/"
    output_path = f"{path_root}/data/bronze/{layout}/"
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_info(f"Input path: {input_path}", layout)
    log_info(f"Output path: {output_path}", layout)
    log_info(f"Batch ID: {batch_id}", layout)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    df_bronze = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("processing_date", current_date())
        .withColumn("source_system", lit("synthetic_generator"))
        .withColumn("source_dataset", lit(layout))
        .withColumn("batch_id", lit(batch_id))
    )

    records_count = df_bronze.count()
    log_info(f"Records processed: {records_count}", layout)

    (
        df_bronze.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("processing_date")
        .parquet(output_path)
    )

    log_info("Bronze write completed successfully", layout)


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        process_to_bronze(spark, "policies")
        process_to_bronze(spark, "claims")
        process_to_bronze(spark, "financial_flows")

    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        raise

    finally:
        spark.stop()