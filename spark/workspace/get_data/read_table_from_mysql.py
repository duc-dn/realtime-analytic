def get_tables(spark, table):
    return spark.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/inventory") \
        .option("dbtable", table) \
        .option("user", "mysqluser") \
        .option("password", "mysqlpw") \
        .load()

