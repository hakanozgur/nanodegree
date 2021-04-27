# %%
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell"

#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

#%%
spark = SparkSession.builder.appName("p2_hkn_kafka_console").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#%%
df_stedi_events = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()


#%%
df_stedi_events = df_stedi_events.selectExpr("cast(value as string) value")

#%%
schema_stedi = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType())
])

#%%
# parse json and create temp view 
df_stedi_events.withColumn("value", from_json("value", schema_stedi)) \
    .select("value.*") \
    .createOrReplaceTempView("CustomerRisk")

#%%
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

#%%
customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
