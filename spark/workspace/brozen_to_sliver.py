from datetime import date

from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, conv, hex, expr, substring, avg, sum as _sum, rank, when
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from pyspark.sql.utils import AnalysisException

from get_data.read_table_from_mysql import get_tables
from util.logger import logger
from util.save_data import (
    save_data_to_minio,
    save_to_database
)
from demo import get_schema

from config import (
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SERVER_HOST,
    KAFKA_BOOTSTRAP_SERVERS
)

from data_config import (
    TABLE_MAPPINGS
)

class DeltaSink:
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
        self.base_path = "s3a://datalake"

    def processing_bronze_to_sliver(self):
        orders = (
                    self.spark.readStream.option("ignoreChanges", "true")
                    .format("delta")
                    .load("s3a://datalake/brozen/cdc.inventory.orders")
                    .alias("orders")
                )
        
        products = (
                    self.spark.readStream.option("ignoreChanges", "true")
                    .format("delta")
                    .load("s3a://datalake/brozen/cdc.inventory.products")
                    .alias("products")
                )
        
        customers = (
                    self.spark.readStream.option("ignoreChanges", "true").
                    format("delta").load("s3a://datalake/brozen/cdc.inventory.customers")
                    .alias("customers")
                )

        joinDF = orders.join(customers, customers.id == orders.purchaser, "inner") \
                            .join(products, products.id == orders.product_id, "inner") \
                            .selectExpr("order_number", "order_date as order_time", "email", "purchaser", 
                                        "name as product_name", "quantity", "weight as unit_price", "orders.ts_ms as timestamp")

        calDF = joinDF.withColumn(
  	    "total_price", joinDF.quantity * joinDF.unit_price)

        calDF.writeStream.format("delta") \
            .option("checkpointLocation", "s3a://datalake/sliver/aggreateTable/checkpoint") \
            .outputMode("append") \
            .start("s3a://datalake/sliver/aggreateTable") \
            .awaitTermination() 

