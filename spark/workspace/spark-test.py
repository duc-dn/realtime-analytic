from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import sum as _sum, avg, expr, window, from_unixtime, col
from util.logger import logger

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.access.key", "admin")
conf.set("spark.hadoop.fs.s3a.secret.key", "123456789")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
            'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set(
    "spark.jars.packages",
    "io.delta:delta-core_2.12:1.0.1,"
    "org.apache.hadoop:hadoop-aws:3.1.1,"
    "com.amazonaws:aws-java-sdk:1.11.271,"
    "com.amazonaws:aws-java-sdk-bundle:1.11.271,"
    "software.amazon.awssdk:url-connection-client:2.15.40",
)
conf.set("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
)
conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled","true")

spark = (
    SparkSession
    .builder
    .config(conf=conf)
    .getOrCreate()
)

spark_sc = spark.sparkContext
spark_sc.setLogLevel('ERROR')

logger.info("="* 50 + "LET'S GO" + "="*50)

# joinDF.writeStream.format
# Load delta tables and assign aliases
orders = (
            spark.readStream.option("ignoreChanges", "true")
            .format("delta")
            .load("s3a://datalake/brozen/cdc.inventory.orders")
            .alias("orders")
        )
products = (
            spark.readStream.option("ignoreChanges", "true")
            .format("delta")
            .load("s3a://datalake/brozen/cdc.inventory.products")
            .alias("products")
        )
customers = (
            spark.readStream.option("ignoreChanges", "true").
            format("delta").load("s3a://datalake/brozen/cdc.inventory.customers")
            .alias("customers")
        )

joinDF = orders.join(customers, customers.id == orders.purchaser, "inner") \
                    .join(products, products.id == orders.product_id, "inner") \
                    .selectExpr("order_number", "order_date as order_time", "email", "purchaser", 
                                "name as product_name", "quantity", "weight", "orders.ts_ms as timestamp") \
                    .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

# joinDF.writeStream.format("console").start().awaitTermination()   

if __name__ == '__main__':
    logger.info("="*50 + "START WRITE STREAM" + "="*50)

    block_sz = 1024

    test = (
        joinDF
        .writeStream
        .outputMode("append")
        .format("delta")
        .option("parquet.block.size", block_sz)
        .option("checkpointLocation", "s3a://datalake/sliver/checkpoint")
        .start("s3a://datalake/sliver")
    )

    test.awaitTermination()