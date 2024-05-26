# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC #### Geo dataframe setup + clean

# COMMAND ----------

geo_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e5f67235f6b-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
geo_df = geo_df.selectExpr("CAST(data as STRING)")

# COMMAND ----------

geo_schema = StructType([\
    StructField("ind", IntegerType(), True),\
    StructField("country", StringType(), True),\
    StructField("latitude", StringType(), True),\
    StructField("longitude", StringType(), True),\
    StructField("timestamp", StringType(), True),\
])

# COMMAND ----------

geo_df = geo_df.withColumn("data", from_json(col("data"), geo_schema)).select("data.*")

# COMMAND ----------

# Create coordinates column & remove latitude and longitude columns
from pyspark.sql.functions import array
geo_df = geo_df.withColumn('coordinates' , array("latitude", "longitude"))
geo_df = geo_df.drop("latitude", "longitude")

# COMMAND ----------

# Convert timestamp into timestamp data type
from pyspark.sql.functions import to_timestamp

geo_df = geo_df.withColumn("timestamp", to_timestamp("timestamp"))

# COMMAND ----------

# Reorder the columns
geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")


# COMMAND ----------

geo_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e5f67235f6b_geo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pin Dataframe Setup

# COMMAND ----------

pin_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e5f67235f6b-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
pin_df = pin_df.selectExpr("CAST(data as STRING)")

# COMMAND ----------

pin_schema = StructType([\
    StructField("index", IntegerType(), True),\
    StructField("unique_id", StringType(), True),\
    StructField("title", StringType(), True),\
    StructField("description", StringType(), True),\
    StructField("follower_count", StringType(), True),\
    StructField("poster_name", StringType(), True),\
    StructField("tag_list", StringType(), True),\
    StructField("is_image_or_video", StringType(), True),\
    StructField("image_src", StringType(), True),\
    StructField("save_location", StringType(), True),\
    StructField("category", StringType(), True),\
])

# COMMAND ----------

pin_df = pin_df.withColumn("data", from_json(col("data"), pin_schema)).select("data.*")

# COMMAND ----------

#Replace null values in various forms with none
pin_df = pin_df.replace({'User Info Error': None}, subset=['follower_count', 'poster_name'])
pin_df = pin_df.replace({'Image src error.': None}, subset=['image_src'])
pin_df = pin_df.replace({'No Title Data Available': None}, subset=['title'])

# COMMAND ----------

# Clean the follower count table
from pyspark.sql.functions import regexp_replace

pin_df = pin_df.withColumn("follower_count", 
    regexp_replace("follower_count", "k", "000") )
pin_df = pin_df.withColumn("follower_count", 
    regexp_replace("follower_count", "M", "000000"))


pin_df = pin_df.withColumn("follower_count", 
    pin_df["follower_count"].cast("int"))

# COMMAND ----------

# Clean the save_location 
pin_df = pin_df.withColumn("save_location", 
    regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

# Rename the index column
pin_df = pin_df.withColumnRenamed("index", "ind")

# COMMAND ----------

# Reorder the columns
pin_df = pin_df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

# COMMAND ----------

pin_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e5f67235f6b_pin_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #### User Dataframe Setup

# COMMAND ----------

user_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e5f67235f6b-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

user_df = user_df.selectExpr("CAST(data as STRING)")

# COMMAND ----------

user_schema = StructType([\
    StructField("ind", IntegerType(), True),\
    StructField("age", StringType(), True),\
    StructField("date_joined", StringType(), True),\
    StructField("first_name", StringType(), True),\
    StructField("last_name", StringType(), True),\
])

# COMMAND ----------

user_df = user_df.withColumn("data", from_json(col("data"), user_schema)).select("data.*")

# COMMAND ----------

# Create user_name column from first and last names
from pyspark.sql.functions import concat, lit

user_df = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
user_df = user_df.drop("first_name", "last_name")

# COMMAND ----------

# Convert date_joined into timestamp
from pyspark.sql.functions import to_timestamp

user_df = user_df.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

# Reorder the columns
user_df = user_df.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

user_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e5f67235f6b_user_table")
