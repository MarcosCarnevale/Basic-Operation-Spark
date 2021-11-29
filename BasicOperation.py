# Create by Marcos Carnevale on 2021-11-01.
# coding: utf-8
#
# Enviroment: PySpark 3.6.0, Python 3.7.4
# Version: 0.1

# Start Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BasicOperation").getOrCreate()

# Import Spark SQL
from pyspark.sql import SqlContext
sqlContext = SqlContext(spark.sparkContext)

# Import Spark Context
from pyspark.sql import SparkContext
sc = SparkContext.getOrCreate()

# Import Spark DataFrame
from pyspark.sql.types import *

# Import Spark Functions
from pyspark.sql.functions import *

# Import Spark Window Functions
from pyspark.sql.window import Window


# Configure Spark
spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.sql.shuffle.partitions", "10", True)
spark.conf.set("spark.sql.shuffle.partitions", "10", "local[*]")


# Create DataFrame
sample_data = [(1, "name1"), (2, "name2"), (3, "name3"), (4, "name4"), (5, "name5"), (6, "name6"), (7, "name7"), (8, "name8"), (9, "name9"), (10, "name10")]
spark.createDataFrame(sample_data, ['id', 'name']).write.mode("overwrite").parquet("table")
df = spark.read.parquet("table")

# Print DataFrame
df.show()

# Print DataFrame Schema
df.printSchema()

# Print DataFrame Columns
df.columns

# Print DataFrame Row Count
df.count()

# Filter DataFrame
df.filter(df.id > 5).show()
df.where(df.id > 5).show()
df.where(col("id") > 5).show()

# Filter DataFrame with multiple conditions
df.filter(df.id > 5).filter(df.id < 10).show()
df.where(col("id") > 5).where(col("id") < 10).show()

# Create a new column
df.withColumn("new_column", df.id + 1).show()
df.withColumn("new_column", col("id") + 1).show()

# Apply a case statement to a column
df.withColumn("new_column", when(df.id > 5, "bigger than 5").otherwise("smaller than 5")).show()

# Rename a column
df.withColumnRenamed("id", "new_id").show()

# Drop a column
df.drop("id").show()

# Group DataFrame
df.groupBy("id").count().show()

# Group DataFrame with multiple columns
df.groupBy("id", "name").count().show()

# Group DataFrame with multiple columns and multiple aggregations
df.groupBy("id", "name").agg(count("id"), max("id"), min("id"), avg("id"), sum("id")).show()

# Order DataFrame
df.orderBy(df.id.desc()).show()

# Order DataFrame with multiple columns
df.orderBy(df.id.desc(), df.name.asc()).show()

# Limit DataFrame
df.limit(5).show()

# Offset DataFrame
df.offset(5).show()

# Union DataFrame
df_a = df.filter(df.id > 5)
df_b = df.filter(df.id < 5)
df = df_a.union(df_b).show()

# Intersect DataFrame
df_a = df.filter(df.id > 5)
df_b = df.filter(df.id < 5)
df = df_a.intersect(df_b).show()

# Except DataFrame
df_a = df.filter(df.id > 5)
df_b = df.filter(df.id < 5)
df = df_a.except(df_b).show()

# Distinct DataFrame
df.distinct().show()
df.dropDuplicates().show()

# Create a new DataFrame adress
sample_data = [(1, "adress1"), (2, "adress2"), (3, "adress3"), (4, "adress4"), (5, "adress5"), (6, "adress6")]
spark.createDataFrame(sample_data, ['id', 'name']).write.mode("overwrite").parquet("table")
df_address = spark.read.parquet("table_address")

# Join DataFrame
df.join(df_address, df.id == df_address.id).show()
df.join(df_address, df.id == df_address.id, "inner").show()
df.join(df_address, df.id == df_address.id, "left").show()
df.join(df_address, df.id == df_address.id, "right").show()
df.join(df_address, df.id == df_address.id, "outer").show()

# Broadcast join
df.join(df_address.broadcast(), df.id == df_address.id).show()

# Window Functions Row Number
df.withColumn("row_number", row_number().over(Window.orderBy(df.id))).show()

# Window Functions Rank
df.withColumn("rank", rank().over(Window.orderBy(df.id))).show()

# Window Functions Dense Rank
df.withColumn("dense_rank", dense_rank().over(Window.orderBy(df.id))).show()

# Window Functions Percent Rank
df.withColumn("percent_rank", percent_rank().over(Window.orderBy(df.id))).show()

# Create Temp View
df.createOrReplaceTempView("temp_table")

# SQL Query
spark.sql("SELECT * FROM temp_table").show()

# Drop Temp View
spark.catalog.dropTempView("temp_table")

# Create def function
def lower_case(x):
    return x.lower()

# Register UDF
spark.udf.register("lower_case", lower_case)

# Create UDF
lower_case_udf = udf(lower_case)

# Using UDF
df.withColumn("lower_case", lower_case_udf(df.name)).show()

# Persist DataFrame
df.persist()

# Unpersist DataFrame
df.unpersist()

# Cache DataFrame
df.cache()

# Uncache DataFrame
df.uncache()

# Read CSV
df = spark.read.csv("table.csv", header=True, inferSchema=True)

# Read JSON
df = spark.read.json("table.json")

# Read Parquet
df = spark.read.parquet("table.parquet")

# Read file from HDFS
df = spark.read.format("csv").option("header", "true").load("hdfs:///user/hive/warehouse/test.db/table")

# Write DataFrame to CSV
df.write.csv("table.csv")

# Write DataFrame to JSON
df.write.json("table.json")

# Write DataFrame to Parquet
df.write.parquet("table.parquet")

# Write DataFrame to HDFS
df.write.format("csv").option("header", "true").save("hdfs:///user/hive/warehouse/test.db/table")
df.write.format("csv").option("header", "true").save("hdfs:///user/hive/warehouse/test.db/table", mode="append")
df.write.format("csv").option("header", "true").save("hdfs:///user/hive/warehouse/test.db/table", mode="overwrite")
df.write.format("csv").option("header", "true").save("hdfs:///user/hive/warehouse/test.db/table", mode="ignore")

# Write DataFrame to Hive
df.write.format("orc").saveAsTable("table")
df.write.format("orc").saveAsTable("table", mode="append")
df.write.format("orc").saveAsTable("table", mode="overwrite")
df.write.format("orc").saveAsTable("table", mode="ignore")

# Write DataFrame to mysql
df.write.\
   format("jdbc").\
   option("url", "jdbc:mysql://localhost:3306/test").\
   option("dbtable", "table").\
   option("user", "root").\
   option("password", "root").save()

# Write DataFrame to mysql
df.write.\
    format("jdbc").\
    option("url", "jdbc:mysql://localhost:3306/test").\
    option("dbtable", "table").\
    option("user", "root").\
    option("password", "root").\
    option("driver", "com.mysql.jdbc.Driver").save()

# Write DataFrame to Sftp
df.write.\
    format("com.jcraft.jsch.ChannelSftp").\
    option("host", "localhost").\
    option("port", "22").\
    option("username", "root").\
    option("password", "root").\
    option("fileType", "csv").\
    option("path", "/home/hadoop/test.csv").save()

# Read data from Sftp
df = spark.read.\
    format("com.jcraft.jsch.ChannelSftp").\
    option("host", "localhost").\
    option("port", "22").\
    option("username", "root").\
    option("password", "root").\
    option("fileType", "csv").\
    option("path", "/home/hadoop/test.csv").load()

# Read data from S3
df = spark.read.csv("s3://test/test.csv")

# Write data to S3
df.write.csv("s3://test/test.csv")

# Read data from Kafka
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()

# Write data to Kafka
df.write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "test").save()

# Stop Spark Session
spark.stop()

# Stop Spark Context
sc.stop()

# Stop Spark Session
spark.sparkContext.stop()

