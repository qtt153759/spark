import io
import string
from time import sleep
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column 
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType
import pyspark.sql.functions as psf
import fastavro
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('date', LongType(), True),
  StructField('value', DoubleType(), True),
  StructField('interval', StringType(), True)
  ])




# Build SparkSession
spark = SparkSession.builder.master("local[*]").appName("ETL Pipeline")\
        .config("spark.jars", "./jars/redshift-jdbc42-2.1.0.9.jar").config("spark.scheduler.mode","FAIR").master("local[*]").getOrCreate()

# Set Logging Level to WARN
# spark.sparkContext.setLogLevel("WARN")

# df.writeStream.outputMode("append").format("console").start().awaitTermination()
EconomicIndicatorIndexList=["REAL_GDP",
            "REAL_GDP_PER_CAPITA",
            "DURABLES",
            "CPI",
            "INFLATION",
            "RETAIL_SALES",
            "UNEMPLOYMENT",
        #     "NONFARM_PAYROLL"
            ]
EconomicIndicatorIndex={}
for i in EconomicIndicatorIndexList:
        EconomicIndicatorIndex[i]=spark.readStream.format("kafka")\
        .option("mode", "PERMISSIVE")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", i)\
        .option("failOnDataLoss", "false")\
        .load()

driver = "com.amazon.redshift.jdbc42.Driver"
url='jdbc:redshift://redshift-cluster-1.cfaj06ovlgm3.us-east-1.redshift.amazonaws.com:5439/dev'	  
user='awsuser'
password='Truong157359'



avroSchema = '''{
                        "type": "record",
                        "namespace": "bigdata_project",
                        "name": "economicIndicatorRecord",
                        "fields": [
                        { "name": "date", "type": "long", "logicalType": "date"},
                        { "name": "value", "type": ["double","null"] },
                        { "name": "interval", "type": "string"}
                        ]
                }
'''

# df_schema = StructType([
#               StructField("date", StringType(), True),
#               StructField("value", StringType(), True)
#           ])

# avro_deserialize_udf = psf.udf(deserialize_avro, returnType=df_schema)
# parsed_df = df.withColumn("avro", avro_deserialize_udf(psf.col("value"))).select("avro.*")


def foreach_batch_function(df:DataFrame, epoch_id:int):
        if df.isEmpty():
                return
        table_name=df.collect()[0][0]
        df.drop("topic")\
        .write.format('jdbc').options(
        url=url,	  
        driver=driver,
        dbtable="public."+table_name,
        user=user,
        password=password).mode('overwrite').save() 
        df.printSchema()


for topic_name,df in EconomicIndicatorIndex.items():
        df.printSchema()
        EconomicIndicatorIndex[topic_name]=df.selectExpr("substring(value, 6) as avro_value","topic")\
        .select(from_avro(col("avro_value"), avroSchema).alias("data"),"topic")\
        .selectExpr("topic","data.date as date","data.value as value","data.interval as interval")
        df.printSchema()
        # df.writeStream .outputMode("append").trigger(processingTime="1 minutes") \
        # .foreachBatch(lambda df,epoch_id:foreach_batch_function(df,epoch_id,topic_name)) \
        # .start().awaitTermination()
for topic_name,df in EconomicIndicatorIndex.items():
        # topic_name=df.writeStream.format("csv")\
        # .option("mode", "PERMISSIVE")\
        # .trigger(processingTime="10 seconds")\
        # .option("checkpointLocation", "./pyspark_template/checkpoint")\
        # .option("startingOffsets", "earliest")\
        # .option("path", "./pyspark_template/csv_folder")\
        # .outputMode("append")\
        # .start()\
        print("after")
        df.printSchema()
        df.writeStream .outputMode("append").trigger(processingTime="1 minutes") \
        .foreachBatch(lambda df,epoch_id:foreach_batch_function(df,epoch_id)) \
        .start()

spark.streams.awaitAnyTermination()
        

  
# value_df = df.select((from_avro(col("value"), avroSchema)).alias("value")).select("value.*")
# print("check schema",output.schema)
# output\
# .writeStream\
# .format("kafka")\
# .option("kafka.bootstrap.servers", "localhost:9092")\
# .option("checkpointLocation", "./streaming_checkpoint")\
# .option("failOnDataLoss","false")\
# .option("topic", "news_1")\
# .start().awaitTermination()


# query = value_df\
#     .writeStream \
#     .format("console") \
#     .option("checkpointLocation", "../streaming_checkpoin") \
#         .option("failOnDataLoss","false")\
#     .start()

# query.awaitTermination()

# output.writeStream.format("console").option("truncate", "true").option("startingOffsets", "earliest").start().awaitTermination()

# output.writeStream.format("csv")\
#   .option("mode", "PERMISSIVE")\
#   .trigger(processingTime="10 seconds")\
#   .option("checkpointLocation", "./pyspark_template/streaming_checkpoint")\
#   .option("path", "/home/qtt/spark/pyspark_template/test_csv")\
#   .outputMode("append")\
#   .start()\
#   .awaitTermination()