from pathlib import Path
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, current_date
from common.spark import create_spark_session
from common.config import (
    RAW_PATH,
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    FILE_FORMAT,
    COMPRESSION
    )


spark = create_spark_session()

def log_info(message, layout):
    print(
        f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} "
        f"INFO Process{layout.title()}ToBronze: {message}"
    )


def process_to_bronze(spark, layout):

    input_path = (
        RAW_PATH /
        layout /
        "ingestion_date=*"
    )

    output_path = (
        BRONZE_PATH /
        layout 
      
    )

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_info(f"Input path: {input_path}", layout)
    log_info(f"Output path: {output_path}", layout)
    log_info(f"Batch ID: {batch_id}", layout)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
      
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
        .option("compression", COMPRESSION)
        .format(FILE_FORMAT)
        .partitionBy("processing_date")
        .save(str(output_path))

    )

    log_info("Bronze write completed successfully", layout)


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Lista todas as configurações ativas na sua sessão atual
    for key, value in spark.sparkContext.getConf().getAll():
        print(f"{key} = {value}")

    try:
        process_to_bronze(spark, "policies")
        process_to_bronze(spark, "claims")
        process_to_bronze(spark, "financial_flows")

    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        raise

    finally:
        spark.stop()