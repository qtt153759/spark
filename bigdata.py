import string
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType,StructField, StringType

from pyspark.sql.functions import from_unixtime,to_date,col,year,month,dayofmonth,first,desc,last,coalesce,regexp_replace

from pyspark.sql.window import Window


# Build SparkSession
spark = SparkSession.builder.master("spark://192.168.1.173:7077").appName("ETL Pipeline")\
        .config("spark.scheduler.mode","FAIR")\
        .config("spark.cores.max",4)\
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")\
        .getOrCreate()

# Set Logging Level to WARN
# spark.sparkContext.setLogLevel("WARN")

# df.writeStream.outputMode("append").format("console").start().awaitTermination()
EconomicIndicatorIndexList=[
        "REAL_GDP",
            "REAL_GDP_PER_CAPITA",
            "DURABLES",
            "CPI",
            "INFLATION",
            "RETAIL_SALES",
            "UNEMPLOYMENT",
            "NONFARM_PAYROLL",
            "Treasury_yield"
            ]
EconomicIndicatorIndex={}


def read_from_kafka_topic(topic_name):
        return spark.readStream.format("kafka")\
        .option("mode", "PERMISSIVE")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", topic_name)\
        .option("failOnDataLoss", "false")\
        .load()

for topic_name in EconomicIndicatorIndexList:
        EconomicIndicatorIndex[topic_name]=read_from_kafka_topic(topic_name)


avroSchema = '''{
                        "type": "record",
                        "namespace": "bigdata_project",
                        "name": "economicIndicatorRecord",
                        "fields": [
                        { "name": "id", "type": "long"},
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
def avro_deserializer(df:DataFrame):
        df=df.selectExpr("substring(value, 6) as avro_value","topic")\
        .select(from_avro(col("avro_value"), avroSchema).alias("data"),"topic")\
        .selectExpr("data.id as id","topic","data.date as date","data.value as value","data.interval as interval")
        result=df.replace({0:None},'value')
        return result

def get_year_value(df:DataFrame):
        windowSpec = Window().partitionBy("year")\
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        result=df.withColumn("year_value",first("value").over(windowSpec))
        return result

def get_year_month_day(df:DataFrame):
        # convert unixtimestamp -> datetime
        df=df.select("id",to_date(from_unixtime(col("date")/1000)).alias("date"),"value","interval","topic") 
        # convert datetime -> year,month,day
        result=df.select("id","date",year(df.date).alias("year"),month(df.date).alias("month")\
                                ,dayofmonth(df.date).alias("day"),"interval","value","topic")
        return result

def fillNA_with_mean(df:DataFrame):
        w1 = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        w2 = Window.orderBy("id").rowsBetween(Window.currentRow, Window.unboundedFollowing)
        result=df.withColumn("previous", last("value", ignorenulls = True).over(w1))\
        .withColumn("next", first("value", ignorenulls = True).over(w2))\
        .withColumn("value", (coalesce("previous", "next") + coalesce("next", "previous")) / 2)
        return result

driver = "org.postgresql.Driver"
url='jdbc:postgresql://bigdata-database.ce0q5dfktsor.us-east-1.rds.amazonaws.com/postgres'	  
user='postgres'
password='truong157359'

def write_to_redshift(df:DataFrame,table_name:string):
        df.write.format('jdbc').options(
        url=url,	  
        driver=driver,
        dbtable="public."+table_name,
        user=user,
        password=password).mode('overwrite').save() 


def write_to_csv(df:DataFrame):
        df.write.mode('append')\
        .csv("./pyspark_template/spark_output/datacsv")


def foreach_batch_function(df:DataFrame, epoch_id:int):
        if df.isEmpty():
                return
        
        interval,table_name=df.first().interval,df.first().topic
        df=fillNA_with_mean(df)
        # get year
        if interval=="monthly" or interval=="quarterly":
                df=get_year_value(df) 

        # df=df.drop("topic").drop("date")
               
        write_to_redshift(df,table_name)
        # write_to_csv(df)
        


for topic_name,df in EconomicIndicatorIndex.items():
        df=avro_deserializer(df)        
        EconomicIndicatorIndex[topic_name]=get_year_month_day(df)
        
def write_stream_to_sinks(topic_name:string,df:DataFrame):
        df.writeStream .outputMode("update").trigger(processingTime="1 minutes") \
        .foreachBatch(lambda df,epoch_id:foreach_batch_function(df,epoch_id)) \
        .start()
        
for topic_name,df in EconomicIndicatorIndex.items():
        # topic_name=df.writeStream.format("csv")\
        # .option("mode", "PERMISSIVE")\
        # .trigger(processingTime="10 seconds")\
        # .option("checkpointLocation", "./pyspark_template/checkpoint")\
        # .option("startingOffsets", "earliest")\
        # .option("path", "./pyspark_template/csv_folder")\
        # .outputMode("append")\
        # .start()\
        write_stream_to_sinks(topic_name,df)
        

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