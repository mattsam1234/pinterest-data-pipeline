# Databricks notebook source
# MAGIC %md
# MAGIC ### Import all the cleaned dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Geographical Data Tables by running the Geographical cleaning notebook

# COMMAND ----------

# MAGIC %run "./Geo_Cleaning_notebook"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Pinterest Data Tables by running the Pinterest cleaning notebook

# COMMAND ----------

# MAGIC %run "./Pin_Cleaning_notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC Import the User Data Tables by running the User cleaning notebook

# COMMAND ----------

# MAGIC %run "./User_Cleaning_notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dataframe that contains all the joined data 

# COMMAND ----------

total_grouped_df = cleaned_pin_df.join(user_df, on="ind").join(geo_df, on="ind")

display(total_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Find the most popular category in each country

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count

geo_pin_df = cleaned_pin_df.join(geo_df, cleaned_pin_df["ind"] == geo_df["ind"], how="inner")
grouped_df = geo_pin_df.groupBy("country", "category").count()

window = Window.partitionBy("country").orderBy(col("count").desc())

top_category_per_country = grouped_df.withColumn("row_num", row_number().over(window)) \
  .filter(col("row_num") == 1) \
  .select("country", "category",col("count").alias("category_count"))


display(top_category_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC Find which was the most popular category each year. Find how many posts each category had between 2018 - 2022

# COMMAND ----------

from pyspark.sql.functions import year , col

q2_grouped_df = geo_pin_df.groupBy(year("timestamp").alias("post_year"), "category").count()

q2_window = Window.partitionBy("post_year").orderBy(col("count").desc())

top_category_per_year = q2_grouped_df.withColumn("row_num", row_number().over(q2_window)) \
  .filter(col("row_num") == 1) \
  .select("post_year", "category",col("count").alias("category_count")) \
  .orderBy("post_year") 

top_category_per_year = top_category_per_year.filter(col("post_year").between(2018, 2022))

display(top_category_per_year)


# COMMAND ----------

# MAGIC %md
# MAGIC Find the user with the most followers in each country

# COMMAND ----------


q3_grouped_df = total_grouped_df.select("country", "user_name" , "follower_count")
from pyspark.sql.functions import desc

q3_window = Window.partitionBy("country").orderBy(desc("follower_count"))

top_user_per_country = q3_grouped_df.withColumn("row_num", row_number().over(q3_window)) \
  .filter(col("row_num") == 1) \
  .select("country", "user_name", "follower_count") \
  .orderBy(desc("follower_count"))

display(top_user_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC Country with the user with top follower count

# COMMAND ----------

top_country_by_follower_count = top_user_per_country.limit(1)
display(top_country_by_follower_count)

# COMMAND ----------

# MAGIC %md
# MAGIC Top category per age group

# COMMAND ----------

from pyspark.sql.functions import when 
q4_df = total_grouped_df.select("age", "category", "category")

q4_df = q4_df.withColumn("age_group", 
                   when((q4_df.age >= 18) & (q4_df.age <= 24), "18-24")
                   .when((q4_df.age >= 25) & (q4_df.age <= 35), "25-35")
                   .when((q4_df.age >= 36) & (q4_df.age <= 50), "36-50")
                   .otherwise("50+"))

q4_grouped_df = q4_df.groupBy("age_group", "category").count()

q4_window = Window.partitionBy("age_group").orderBy(desc("count"))

top_category_per_age_group = q4_grouped_df.withColumn("row_num", row_number().over(q4_window)) \
  .filter(col("row_num") == 1) \
  .select("age_group", "category", col("count").alias("category_count")) \
  .orderBy(desc("category_count"))

display(top_category_per_age_group)

# COMMAND ----------

# MAGIC %md
# MAGIC Median follower count per age group

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F

q5_df = total_grouped_df.select("age", "follower_count")
q5_df = q5_df.withColumn("age_group", 
                   when((q5_df.age >= 18) & (q5_df.age <= 24), "18-24")
                   .when((q5_df.age >= 25) & (q5_df.age <= 35), "25-35")
                   .when((q5_df.age >= 36) & (q5_df.age <= 50), "36-50")
                   .otherwise("50+"))

q5_window = Window.partitionBy('age_group')
magic_percentile = F.expr('percentile_approx(follower_count, 0.5)')

q5_df = q5_df.withColumn('median_follower_count', magic_percentile.over(q5_window))

q5_grouped_df = q5_df.groupBy('age_group').agg(magic_percentile.alias('median_follower_count')).orderBy(desc("median_follower_count"))

display(q5_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Find how many users have joined each year

# COMMAND ----------

q6_df = user_df.groupBy(year("date_joined").alias("year_joined")).count()

users_per_year = q6_df.filter(col("year_joined").between(2015, 2020))
users_per_year = users_per_year.withColumnRenamed("count", "number_users_joined")


display(users_per_year)

# COMMAND ----------

# MAGIC %md
# MAGIC Find the median follower count of users based on their joining year

# COMMAND ----------

q7_df = total_grouped_df.select(year("date_joined").alias("year_joined"), "follower_count")

magic_percentile = F.expr('percentile_approx(follower_count, 0.5)')

q7_window = window.partitionBy("year_joined")

q7_grouped_df = q7_df.groupBy('year_joined').agg(magic_percentile.alias('median_follower_count')).orderBy(desc("median_follower_count"))

display(q7_grouped_df)



# COMMAND ----------

# MAGIC %md
# MAGIC Find the median follower count of users based on their joining and age group

# COMMAND ----------

q8_df = total_grouped_df.select(year("date_joined").alias("year_joined"), "follower_count", "age")
q8_df = q8_df.withColumn("age_group", 
                   when((q8_df.age >= 18) & (q8_df.age <= 24), "18-24")
                   .when((q8_df.age >= 25) & (q8_df.age <= 35), "25-35")
                   .when((q8_df.age >= 36) & (q8_df.age <= 50), "36-50")
                   .otherwise("50+"))

magic_percentile = F.expr('percentile_approx(follower_count, 0.5)')

q8_window = Window.partitionBy("age_group", "year_joined")

median_follower_count_by_age_group_and_year_joined = q8_df.groupBy('age_group', "year_joined").agg(magic_percentile.alias('median_follower_count')).orderBy("age_group", "year_joined")

display(median_follower_count_by_age_group_and_year_joined)

