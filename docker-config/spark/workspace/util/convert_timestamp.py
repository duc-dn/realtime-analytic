from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, dayofmonth, month, year


def convert_timestamp(df, ts_field: str):
    df = df.withColumn("timestamp", (col(ts_field)).cast("timestamp"))

    df = (
        df.withColumn("year", year(df.timestamp))
        .withColumn("month", month(df.timestamp))
        .withColumn("day", dayofmonth(df.timestamp))
    )
    return df.drop(col("timestamp"))
