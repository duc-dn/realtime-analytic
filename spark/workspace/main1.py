import logging, json
# import requests
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
        self.delta_tbl_loc = "s3a://datalake/brozen/"
        self.table_mappings = TABLE_MAPPINGS
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    def get_delta_table(self, table_name):
        try:
            table = DeltaTable.forPath(
                self.spark, path=self.delta_tbl_loc + table_name
            ).alias("target")
            return table    
        except Exception as e:
            logger.error(e)
            return False


    def foreach_batch_function_incremental(self, df: DataFrame, epoch_id: int) -> None:
        """
        Handle each batch when ingesting incremental

        :param df:
        :param epoch_id:
        :return: None
        """

        # get topic name
        topic = df.select("topic").first()[0]

        # get schema id compatible with each batch data
        schema_id = df.select("schema_id").first()[0]

        logger.info(f'Epoch_id: {epoch_id} of topic: {topic} is being ingested')
        schema = get_schema(schema_id=schema_id)

        # decode value from kafka
        df = df.select(
            from_avro(col("fixed_value"), schema).alias("parsed_value")
        )
        
        # get primary key of delta table
        pk = TABLE_MAPPINGS[topic]["pk"]

        primary_keys_fulfill = [
                    when(col("parsed_value.op") == "d", col(f"parsed_value.before.{pk}"))
                        .otherwise(col(f"parsed_value.after.{pk}"))
                        .alias(f"before_{pk}")
                ]

        # select necessary fields
        df = df.select(
            "parsed_value.op",
            "parsed_value.ts_ms",
            "parsed_value.after.*",
            *primary_keys_fulfill
        )

        by_primary_key = Window.partitionBy(
                pk
            ).orderBy(col("ts_ms").desc())
        
        df = (
            df.withColumn("rank", rank().over(by_primary_key))
                .filter("rank=1")
                .orderBy("ts_ms")
                .drop("rank")
                .dropDuplicates([pk])
        )
        df.show(truncate=True)
        delta_table = self.get_delta_table(topic)
        if delta_table:
            (
                delta_table.alias("target")
                    .merge(
                        source=df.alias("source"), 
                        condition=f"target.{pk} = source.before_{pk}"
                    )
                    .whenMatchedDelete(condition="source.op = 'd'")
                    .whenMatchedUpdateAll(condition="source.op <> 'd'")
                    .whenNotMatchedInsertAll(condition="source.op <> 'd'")
                    .execute()
            )
        else:
            df.where("op <> 'd'").write.format("delta").mode(
                "overwrite"
            ).save(f"s3a://datalake/brozen/{topic}")
            logger.info("="*20 + f"SAVE {topic} SUCESSFULLY" + "="*20)

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

        for topic in self.table_mappings.keys():
            logger.info(f"Starting ingest topic {topic}")
            df: DataFrame = self.get_data_stream_reader(
                topic
            ).load()

            df: DataFrame = (
                df.withColumn("schema_id", conv(hex(substring("value", 2, 4)), 16, 10))
                .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
                .select(
                    "topic",
                    "schema_id",
                    "fixed_value",
                )
            )

            stream: StreamingQuery = (
                df.writeStream.foreachBatch(
                    self.foreach_batch_function_incremental
                )
                .trigger(processingTime="60 seconds")
                # .option("checkpointLocation", f"s3a://datalake/brozen/checkpoint/{topic}")
                .start()
            )
        self.spark.streams.awaitAnyTermination()

    def merge_to_delta(self, df, primary_keys):
        pass

    def run(self) -> None:
        try:
            self.ingest_mutiple_topic()
            logger.info("Table existed!!")
        except AnalysisException as e:
            logger.error(f'Error: {e}')

if __name__ == "__main__":
    DeltaSink().run()
