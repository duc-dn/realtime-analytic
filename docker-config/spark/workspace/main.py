import json
import logging

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SERVER_HOST,
)
from data_config import TABLE_MAPPINGS
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, col, conv, expr, from_json, hex, rank, substring
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from util.convert_timestamp import convert_timestamp
from util.get_schema import get_schema
from util.logger import logger


class DeltaSink:
    """
    Ingest data from topic ux_data to MINIO
    """

    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:2.0.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                "org.apache.kafka:kafka-clients:2.8.0,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.2.0,"
                "org.apache.commons:commons-pool2:2.6.2,"
                "org.apache.spark:spark-avro_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.2.3,"
                "com.amazonaws:aws-java-sdk:1.11.375,"
                "org.apache.spark:spark-tags_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-common:3.2.0",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.codegen.wholeStage", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_SERVER_HOST)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.sql.legacy.castComplexTypesToString.enabled", "false")
            .config("spark.databricks.delta.optimize.repartition.enabled", "true")
            .getOrCreate()
        )

        self._spark_sc = self.spark.sparkContext
        self._spark_sc.setLogLevel("ERROR")
        self.delta_tbl_loc = "s3a://datalake/sliver/"
        self.table_mappings = TABLE_MAPPINGS
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    def get_delta_table(self, table_name):
        try:
            table = DeltaTable.forPath(
                self.spark, path=self.delta_tbl_loc + table_name
            ).alias("target")
            return table
        except Exception as e:
            logger.warning(e)
            return False

    def foreach_batch_function_incremental(self, df: DataFrame, epoch_id: int) -> None:
        """
        Handle each batch when ingesting incremental

        :param df:
        :param epoch_id:
        :return: None
        """

        if df.count() == 0:
            return

        # get topic name
        topic = df.select("topic").first()[0]

        # get schema id compatible with each batch data
        schema_id = df.select("schema_id").first()[0]
        schema = get_schema(schema_id=schema_id)

        logger.info(f"Epoch_id: {epoch_id} of topic: {topic} is being ingested")

        # decode value from kafka
        df = df.select(from_avro(col("fixed_value"), schema).alias("parsed_value"))

        if topic == "cdc.myshop.order_detail":
            pk1 = TABLE_MAPPINGS[topic]["pk1"]
            pk2 = TABLE_MAPPINGS[topic]["pk2"]

            df, condition = self.processing_dataframe(df, pk=None, pk1=pk1, pk2=pk2)
        else:
            pk = TABLE_MAPPINGS[topic]["pk"]
            df, condition = self.processing_dataframe(df, pk=pk, pk1=None, pk2=None)

        is_tscol = False
        if "created_at" in df.columns:
            df = convert_timestamp(df, "created_at")
            is_tscol = True
        elif "updated_at" in df.columns:
            df = convert_timestamp(df, "updated_at")
            is_tscol = True

        print(f"Processing {topic} ...")
        df.show(n=2, truncate=False)

        # ============ START PROCESSING DELTA TABLE =========
        delta_table = self.get_delta_table(topic)
        if delta_table:
            if topic != "cdc.myshop.order_detail":
                by_primary_key = Window.partitionBy(pk).orderBy(col("ts_ms").desc())

                df = (
                    df.withColumn("rank", rank().over(by_primary_key))
                    .filter("rank=1")
                    .orderBy("ts_ms")
                    .drop("rank")
                    .dropDuplicates([pk])
                )

                self.merge_data_to_delta_table(
                    df=df, delta_table=delta_table, condition=condition
                )
            else:
                pk1 = TABLE_MAPPINGS[topic]["pk1"]
                pk2 = TABLE_MAPPINGS[topic]["pk2"]

                self.merge_data_to_delta_table(
                    df=df, delta_table=delta_table, condition=condition
                )
        else:
            self.save_to_minio(df, topic, is_tscol)
            logger.info("=" * 20 + f"SAVE {topic} SUCESSFULLY" + "=" * 20)

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
            .option("groupIdPrefix", f"spark-kafka-source-{topic_name}")
        )

    def ingest_mutiple_topic(self) -> None:
        """
        Ingest data from mutiple kafka topic
        :return: None
        """

        for topic in self.table_mappings.keys():
            logger.info(f"Starting ingest topic {topic}")
            df: DataFrame = self.get_data_stream_reader(topic).load()

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
                df.writeStream.foreachBatch(self.foreach_batch_function_incremental)
                .trigger(processingTime="60 seconds")
                .option(
                    "checkpointLocation", f"s3a://datalake/sliver/checkpoint/{topic}"
                )
                .start()
            )
        self.spark.streams.awaitAnyTermination()

    @staticmethod
    def processing_dataframe(df: DataFrame, pk: str, pk1: str, pk2: str):
        """
        Processing dataframe
        """

        if pk1 and pk2:
            primary_keys_fulfill1 = [
                when(col("parsed_value.op") == "d", col(f"parsed_value.before.{pk1}"))
                .otherwise(col(f"parsed_value.after.{pk1}"))
                .alias(f"before_{pk1}")
            ]

            primary_keys_fulfill2 = [
                when(col("parsed_value.op") == "d", col(f"parsed_value.before.{pk2}"))
                .otherwise(col(f"parsed_value.after.{pk2}"))
                .alias(f"before_{pk2}")
            ]

            df = df.select(
                "parsed_value.op",
                "parsed_value.ts_ms",
                "parsed_value.after.*",
                *primary_keys_fulfill1,
                *primary_keys_fulfill2,
            )

            condition = f"target.{pk1} = source.before_{pk1} \
                        and target.{pk2} = source.before_{pk2}"
        else:
            primary_keys_fulfill = [
                when(col("parsed_value.op") == "d", col(f"parsed_value.before.{pk}"))
                .otherwise(col(f"parsed_value.after.{pk}"))
                .alias(f"before_{pk}")
            ]

            df = df.select(
                "parsed_value.op",
                "parsed_value.ts_ms",
                "parsed_value.after.*",
                *primary_keys_fulfill,
            )

            condition = f"target.{pk} = source.before_{pk}"

        return df, condition

    @staticmethod
    def merge_data_to_delta_table(df: DataFrame, delta_table, condition: str):
        """
        merge into delta table when update, delete operation
        """

        try:
            delta_table.alias("target").merge(
                source=df.alias("source"), condition=condition
            ).whenMatchedDelete(condition="source.op = 'd'").whenMatchedUpdateAll(
                condition="source.op <> 'd'"
            ).whenNotMatchedInsertAll(
                condition="source.op <> 'd'"
            ).execute()
        except Exception as e:
            logger.info(f"Error when merging into delta table {e}")

    @staticmethod
    def save_to_minio(df: DataFrame, topic: str, is_ts_col: bool):
        if is_ts_col:
            (
                df.where("op <> 'd'")
                .write.format("delta")
                .partitionBy(["year", "month", "day"])
                .mode("overwrite")
                .save(f"s3a://datalake/sliver/{topic}")
            )
        else:
            (
                df.where("op <> 'd'")
                .write.format("delta")
                .mode("overwrite")
                .save(f"s3a://datalake/sliver/{topic}")
            )

    def run(self) -> None:
        try:
            self.ingest_mutiple_topic()
            logger.info("Table existed!!")
        except AnalysisException as e:
            logger.error(f"Error: {e}")


if __name__ == "__main__":
    DeltaSink().run()
