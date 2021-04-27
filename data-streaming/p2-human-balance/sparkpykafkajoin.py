# %%
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell"

#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

#%%
# spark application
spark = SparkSession.builder.appName("p2_hkn_kafka_join").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# read df from redis-server kafka stream
# local address -> "localhost:9092"
# docker bootstrap address -> "172.22.0.6:9092"
# kafka:19092
#%%
df_redis_server = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

#%%
df_redis_server = df_redis_server.selectExpr("cast(value as string) value")

#%%
# {
#   "key": "Q3VzdG9tZXI=",
#   "existType": "NONE",
#   "Ch": false,
#   "Incr": false,
#   "zSetEntries": [
#     {
#       "element": "eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
#       "Score": 0.0
#     }
#   ],
#   "zsetEntries": [
#     {
#       "element": "eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
#       "score": 0.0
#     }
#   ]
# }
#%%
schema_redis = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType()),
            ])
        )),
    ]
)

# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+

#%%
# parse json, ignore duplicate field and create temp view 
df_redis_server.withColumn("value", from_json("value", schema_redis)) \
    .select("value.*") \
    .createOrReplaceTempView("RedisSortedSet")


#%%
encoded_customer = spark.sql("select zSetEntries[0].element as customer from RedisSortedSet")


#%%
decoded_customer = encoded_customer.withColumn("customer", unbase64(encoded_customer.customer).cast("string"))

#%%
# {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
#%%
schema_customer = StructType(
    [
        StructField("customerName", StringType()),
        StructField("birthDay", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
    ]
)

#%%
decoded_customer.withColumn("customer", from_json("customer", schema_customer)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

#%%
emailAndBirthDayStreamingDF = spark.sql("select email, birthDay from CustomerRecords where email is not null")

#%%
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select("email",
split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

#%%
emailAndBirthYearStreamingDF.printSchema()

#################################
#################################

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
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+

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
df_join = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr("email = customer"))

#%%
df_join.printSchema()

#%%
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+

# {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 

#%%
df_join.selectExpr("cast(customer as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "stedi-risk") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()


# %%
