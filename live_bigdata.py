from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,split,to_date,from_unixtime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,LongType


spark = SparkSession.builder.master("spark://192.168.1.173:7077").appName("ETL LIVE_Pipeline")\
        .config("spark.scheduler.mode","FAIR")\
        .config("spark.cores.max",4)\
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")\
        .getOrCreate()
        # .config("spark.jars", "./jars/postgresql-42.5.1.jar")

driver = "org.postgresql.Driver"
url='jdbc:postgresql://bigdata-database.ce0q5dfktsor.us-east-1.rds.amazonaws.com/postgres'	  
user='postgres'
password='truong157359'

df=spark.readStream.format("kafka")\
        .option("mode", "PERMISSIVE")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "stats")\
        .option("failOnDataLoss", "false")\
        .load()

schema = StructType([
        StructField("type", StringType()),
        StructField("countTrades", LongType()),
        StructField("sumPrice", DoubleType()),
        StructField("minPrice", DoubleType()),
        StructField("avgPrice", DoubleType()),
        StructField("maxPrice", DoubleType()),])


value_df=df.select(from_json(col("value").cast("string"),schema=schema).alias("data"),col("key").cast("string").alias("key"))\
        .selectExpr("data.type as type","data.countTrades as countTrades","data.sumPrice as sumPrice",
                    "data.minPrice as minPrice","data.avgPrice as avgPrice","data.maxPrice as maxPrice","key")

value_df=value_df.withColumn("start",from_unixtime(split(col("key"),"-").getItem(0).cast('double')/1000,"yyyy-MM-dd HH:mm:ss"))\
                .withColumn("end",from_unixtime(split(col("key"),"-").getItem(1).cast('double')/1000,"yyyy-MM-dd HH:mm:ss"))
value_df=value_df.drop("key","type")

def foreach_batch_function(df:DataFrame, epoch_id:int):
        if df.isEmpty():
                return
        df.write.format('jdbc').options(
        url=url,	  
        driver=driver,
        dbtable="public.EURUSD",
        user=user,
        password=password).mode('append').save() 
        


value_df.writeStream .outputMode("update").trigger(processingTime="1 minutes") \
        .foreachBatch(lambda df,epoch_id:foreach_batch_function(df,epoch_id)) \
        .start().awaitTermination()
        