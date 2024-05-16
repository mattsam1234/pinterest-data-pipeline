# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning the Geographical data table
# MAGIC ### Load the data into notebook

# COMMAND ----------

geo_file_location = "/mnt/pinterest-mattie/topics/0e5f67235f6b.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(geo_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create coordinates column & remove latitude and longitude columns

# COMMAND ----------

from pyspark.sql.functions import array

geo_df = geo_df.withColumn('coordinates' , array("latitude", "longitude"))
geo_df = geo_df.drop("latitude", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert timestamp into timestamp data type

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

geo_df = geo_df.withColumn("timestamp", to_timestamp("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reorder the columns

# COMMAND ----------

geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")

