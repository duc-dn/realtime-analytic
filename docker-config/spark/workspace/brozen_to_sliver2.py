from datetime import date

from delta.tables import *
from pyspark.sql.functions import col, from_json, conv, hex, expr, substring, avg, sum as _sum, rank, when
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from pyspark.sql.utils import AnalysisException

from util.logger import logger

from util.convert_timestamp import convert_timestamp

from config import (
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SERVER_HOST,
    KAFKA_BOOTSTRAP_SERVERS
)

from data_config import (
    TABLE_MAPPINGS
)

class IngestionSliver:
    """
    Ingest data from topic ux_data to MINIO
    """

    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder
            .config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:1.0.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,"
                "org.apache.kafka:kafka-clients:2.6.0,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.1.1,"
                "org.apache.commons:commons-pool2:2.6.2,"
                "org.apache.spark:spark-avro_2.12:3.1.1,"
                "org.apache.hadoop:hadoop-aws:3.1.1,"
                "com.amazonaws:aws-java-sdk:1.11.271,"
                "mysql:mysql-connector-java:8.0.30,"
                "org.postgresql:postgresql:42.5.0"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_SERVER_HOST)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config("spark.sql.legacy.castComplexTypesToString.enabled",
                        "false")
            .getOrCreate()
        )

        self._spark_sc = self.spark.sparkContext
        self._spark_sc.setLogLevel('ERROR')
        self.bronze_path = "s3a://datalake/brozen/"
        self.sliver_path = "s3a://datalake/sliver/"
        self.table_mappings = TABLE_MAPPINGS

    def get_delta_table(self, table_name):
        try:
            table = DeltaTable.forPath(
                self.spark, path=self.sliver_path + table_name
            ).alias("target")
            return table    
        except Exception as e:
            logger.warning(e)
            return False

    def processing_bronze_to_sliver(self):
        for topic in TABLE_MAPPINGS.keys():
            logger.info(f'topic: {topic} is being ingested')
            if topic == "cdc.myshop.order_detail":
                df = (
                        self.spark.readStream.option("ignoreChanges", "true")
                        .format("delta")
                        .load(self.bronze_path + topic)
                    )
            
                df = df.drop("before_order_id", "before_product_id")

            else:
                df = (
                        self.spark.readStream.option("ignoreChanges", "true")
                        .format("delta")
                        .load(self.bronze_path + topic)
                    )
            
                df = df.drop("before_id", "ts_ms")

            # df = convert_timestamp(df, "ts_ms")

            # delta_table = self.get_delta_table(topic)

            # if delta_table:
            #     pass
            # else:
                df.writeStream.format("delta") \
                    .option("checkpointLocation", f"s3a://datalake/sliver/checkpoint/{topic}") \
                    .outputMode("append") \
                    .start(self.sliver_path + topic)
                
        self.spark.streams.awaitAnyTermination()
    
    
if __name__ == "__main__":
    ingestion = IngestionSliver()
    ingestion.processing_bronze_to_sliver()