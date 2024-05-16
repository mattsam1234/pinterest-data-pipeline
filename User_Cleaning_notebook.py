# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning the User data table
# MAGIC ### Load the data into notebook

# COMMAND ----------

user_file_location = "/mnt/pinterest-mattie/topics/0e5f67235f6b.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
user_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(user_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create user_name column from first and last names

# COMMAND ----------

from pyspark.sql.functions import concat, lit

user_df = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
user_df = user_df.drop("first_name", "last_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert date_joined into timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

user_df = user_df.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reorder the columns

# COMMAND ----------

user_df = user_df.select("ind", "user_name", "age", "date_joined")
