```
df = spark.read.format("delta").load("s3a://datalake/demo")
```

```
df.createOrReplaceTempView("demo")
```

