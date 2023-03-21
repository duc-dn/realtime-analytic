from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import sum as _sum, avg, expr, window, from_unixtime, col

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.access.key", "admin")
conf.set("spark.hadoop.fs.s3a.secret.key", "123456789")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
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
    .master("local[*]")
    .getOrCreate()
)

spark_sc = spark.sparkContext
spark_sc.setLogLevel('ERROR')


# joinDF.writeStream.format
# Load delta tables and assign aliases
orders = (spark.readStream.option("ignoreChanges", "true").format("delta").load("s3a://datalake/brozen/cdc.inventory.orders")
            .alias("orders"))
products = (spark.readStream.option("ignoreChanges", "true").format("delta").load("s3a://datalake/brozen/cdc.inventory.products")
              .alias("products"))
customers = (spark.readStream.option("ignoreChanges", "true").format("delta").load("s3a://datalake/brozen/cdc.inventory.customers")
               .alias("customers"))

joinDF = orders.join(customers, customers.id == orders.purchaser, "inner") \
                    .join(products, products.id == orders.product_id, "inner") \
                    .selectExpr("order_number", "order_date as order_time", "email", "purchaser", 
                                "name as product_name", "quantity", "weight", "orders.ts_ms as timestamp") \
                    .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

# joinDF.writeStream.format("console").start().awaitTermination()

test = (
    joinDF
    .withWatermark("timestamp", "1 minutes")
    .groupBy(col("email"), "timestamp")
    .agg(
        _sum("weight").alias("total_weight")
    )
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "s3a://datalake/sliver/checkpoint")
    .start("s3a://datalake/sliver")
)
test.awaitTermination()


# Join the dataframes using SQL syntax
# joinDF = (orders.join(customers, "customers.id == orders.purchaser", "inner")
#               .join(products, "products.id == orders.product_id", "inner")
#               .selectExpr("order_number", "order_date as order_time", "email", "purchaser", 
#                           "name as product_name", "quantity", "weight as unit_price"))

# # Write the result to console
# joinDF.writeStream.format("console").start().awaitTermination()
# total_spent_DF = calDF \
#         .groupBy("email") \
#         .agg(_sum("total_price")) 

# product_DF = calDF \
#             .groupBy("product_name") \
#             .agg(
#                 _sum("quantity").alias("products_selled"), 
#                 _sum("total_price").alias("total_price")
#             )

# product_price = calDF \
#             .groupBy("order_time","product_name") \
#             .agg(
#                 avg("unit_price").alias("ave_unit_price")
#             )