from pyspark.sql import SparkSession
from common.config import (
    SPARK_APP_NAME,
    SPARK_MASTER
)


    
def create_spark_session():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        ) \
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer"
        )
        .getOrCreate()
    )

