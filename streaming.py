from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("SparkStreaming").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic1")\
  .option("failOnDataLoss", "false").load()
print(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.outputMode("append").format("csv") \
.option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "./pyspark_template/streaming")\
.option("path", "./pyspark_template/streaming_output_sink").start().awaitTermination()
