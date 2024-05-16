# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning the Pinterest data table
# MAGIC ### Load the data into notebook

# COMMAND ----------

pin_file_location = "/mnt/pinterest-mattie/topics/0e5f67235f6b.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
pin_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(pin_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace the null values in various forms with none

# COMMAND ----------

cleaned_pin_df = pin_df.replace({'User Info Error': None}, subset=['follower_count', 'poster_name'])
cleaned_pin_df = cleaned_pin_df.replace({'Image src error.': None}, subset=['image_src'])
cleaned_pin_df = cleaned_pin_df.replace({'No Title Data Available': None}, subset=['title'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean the follower count table

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

cleaned_pin_df = cleaned_pin_df.withColumn("follower_count", 
    regexp_replace("follower_count", "k", "000") )
cleaned_pin_df = cleaned_pin_df.withColumn("follower_count", 
    regexp_replace("follower_count", "M", "000000"))


cleaned_pin_df = cleaned_pin_df.withColumn("follower_count", 
    cleaned_pin_df["follower_count"].cast("int"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean the save_location 

# COMMAND ----------

cleaned_pin_df = cleaned_pin_df.withColumn("save_location", 
    regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the index column
# MAGIC

# COMMAND ----------

cleaned_pin_df = cleaned_pin_df.withColumnRenamed("index", "ind")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reorder the columns

# COMMAND ----------

cleaned_pin_df = cleaned_pin_df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

