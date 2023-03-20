import logging
import requests
from datetime import date

from pyspark.sql.functions import col, from_json, conv, hex, expr, substring, avg, sum as _sum
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
    UX_DATA_TOPICS
)


class DeltaSink:
    """
    Ingest data from topic ux_data to MINIO
    """

    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.master("local[1]")
            .config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:1.0.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,"
                "org.apache.kafka:kafka-clients:2.6.0,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.1.1,"
                "org.apache.commons:commons-pool2:2.6.2,"
                "org.apache.spark:spark-avro_2.12:3.1.1,"
                "org.apache.hadoop:hadoop-aws:3.2.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.375,"
                "mysql:mysql-connector-java:8.0.30,"
                "org.postgresql:postgresql:42.5.0"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_SERVER_HOST)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

            .getOrCreate()
        )

        self.topic_name = UX_DATA_TOPICS
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    def foreach_batch_function_incremental(self, df: DataFrame, epoch_id: int) -> None:
        """
        Handle each batch when ingesting incremental

        :param df:
        :param epoch_id:
        :return: None
        """

        # get topic name
        topic = df.select("topic").first()[0]

        # get schema id with each batch data
        schema_id = df.select("schema_id").first()[0]

        logger.info(f'Epoch_id: {epoch_id} of topic: {topic} is being ingested')
        schema = get_schema(schema_id=schema_id)


        # decode value from kafka
        df = df.select(
            from_avro(col("fixed_value"), schema).alias("parsed_value")
        )

        df = df.select("parsed_value.after.*")
        # df.show()

        # df.show()
        # df = df.select(
        #         "parsed_value.*"
        #     ) \
        #     .select(
        #         "before",
        #         "after",
        #         "op",
        #         "ts_ms"
        #     )

        # df.show(truncate=False)
        customer_table = get_tables(spark=self.spark, table="customers")
        product_table = get_tables(spark=self.spark, table="products")

        # customer_table.show()
        # product_table.show()

        joinDF = df.join(customer_table, customer_table.id == df.purchaser, "inner") \
                    .join(product_table, product_table.id == df.product_id, "inner") \
                    .selectExpr("order_number", "order_date as order_time", "email", "purchaser", 
                                "name as product_name", "quantity", "weight as unit_price")
        
        # joinDF.show()
        logger.info("processing calDF")
        calDF = joinDF.withColumn(
  	    "total_price", joinDF.quantity * joinDF.unit_price)


        # Nhóm email của người dùng với số tiền mà user này đã chi
        logger.info("processing total_spend_DF")
        total_spent_DF = calDF \
        .groupBy("email") \
        .agg(_sum("total_price")) 
        
        # total_spent_DF.show()

        # Nhóm tên các sản phẩm theo tổng số lượng đã bán và tổng số tiền nhận được
        logger.info("processing total_spend_DF")
        product_DF = calDF \
            .groupBy("product_name") \
            .agg(
                _sum("quantity").alias("products_selled"), 
                _sum("total_price").alias("total_price")
            )
        
        product_DF.show()



        # Nhóm thời gian và tên sản phẩm theo trung bình đơn giá của sản phẩm
        logger.info("processing producgt_DF")
        product_price = calDF \
            .groupBy("order_time","product_name") \
            .agg(
                avg("unit_price").alias("ave_unit_price")
            )
        # product_price.show()

        save_data_to_minio(
            product_DF, path=f"s3a://datalake/brozen",
            partitionField='product_name'
        )

        save_to_database(
            product_DF, table="ave_product",
            username='postgres', password= 'postgres',
            url='jdbc:postgresql://localhost:5432/dwh_streaming'
        )

        # joinDF.show()

    def get_data_stream_reader(
            self, topic_name: str, starting_offsets: str = "earliest"
    ) -> DataStreamReader:
        """
        Reading streaming from kafka topic
        :param topic_name:
        :param starting_offsets:
        :return: stream DataFrame
        """
        kafka_bootstrap_servers = self.kafka_bootstrap_servers

        return (
            self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("subscribe", topic_name)
                .option("startingOffsets", starting_offsets)
                .option("groupIdPrefix", f'spark-kafka-source-{topic_name}')
        )

    def ingest_mutiple_topic(self) -> None:
        """
        Ingest ux data from mutiple kafka topic
        :return: None
        """

        for topic in self.topic_name:
            logger.info(f"Starting ingest topic {topic}")
            df: DataFrame = self.get_data_stream_reader(
                topic
            ).load()

            # df: DataFrame = (
            #     df.withColumn("schema_id", conv(hex(substring("value", 2, 4)), 16, 10))
            #     .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
            #     .select(
            #         "topic",
            #         "schema_id",
            #         "fixed_value",
            #     )
            # )

            df: DataFrame = (
                df.withColumn("key_schema_id", conv(hex(substring("key", 2, 4)), 16, 10))
                    .withColumn("key_binary", expr("substring(key, 6, length(value)-5)"))
                    .withColumn("schema_id", conv(hex(substring("value", 2, 4)), 16, 10))
                    .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
                    .select(
                    "topic",
                    "partition",
                    "offset",
                    "timestamp",
                    "key_schema_id",
                    "schema_id",
                    "key_binary",
                    "fixed_value",
                )
            )

            stream: StreamingQuery = (
                df.writeStream.foreachBatch(
                    self.foreach_batch_function_incremental
                )
                .trigger(processingTime="30 seconds")
                .start()
            )
        self.spark.streams.awaitAnyTermination()

    def run(self) -> None:
        try:
            self.ingest_mutiple_topic()
        except AnalysisException as e:
            logger.error(f'Error: {e}')

if __name__ == "__main__":
    DeltaSink().run()
