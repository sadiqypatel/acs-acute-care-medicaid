# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df_episode_level = spark.table("dua_058828_spa240.paper2_acute_care_visit_episodes")
df_episode_level = df_episode_level.drop("CLM_ID","ednnp","alcohol","drug","injury","psych","unclassified")
df_episode_level.show(5)

# COMMAND ----------

df_county_level = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_all")

# Count the total number of rows
total_rows = df_county_level.count()

# Count the number of rows where "total_members" is 100 or greater
rows_with_100_or_more_members = df_county_level.filter(col("total_members") >= 100).count()

# Calculate the percentage
percentage = (rows_with_100_or_more_members / total_rows) * 100

# Print the result
print(f"The percentage of rows with 100 or more members is: {percentage:.1f}%")

# Filter rows where "total_members" is under 100
filtered_df = df_county_level.filter(col("total_members") < 100)

# Calculate the total number of members in counties with under 100 members
total_members_under_100 = filtered_df.agg({"total_members": "sum"}).collect()[0]["sum(total_members)"]

# Print the result
print(f"The total number of members in counties with under 100 members is: {total_members_under_100}")

df_county_level.show(5)

# COMMAND ----------

# Filter rows where "total_members" is 100 or greater
df_county_level_100_or_more = df_county_level.filter(col("total_members") >= 100)

# Count the total number of rows
total_rows = df_county_level_100_or_more.count()

# Count the number of rows where "total_members" is 100 or greater
rows_with_100_or_more_members = df_county_level_100_or_more.filter(col("total_members") >= 100).count()

# Calculate the percentage
percentage = (rows_with_100_or_more_members / total_rows) * 100

# Print the result
print(f"The percentage of rows with 100 or more members is: {percentage:.1f}%")

df_county_level_100_or_more.show(5)

# COMMAND ----------

#create a member data frame for final patient sample

fips_column_name = "fips_code"

# Get unique values from the fips_code column
unique_values = df_county_level_100_or_more.select(fips_column_name).distinct()

# Count the number of unique values
num_unique_values = unique_values.count()

# Count the total number of rows in the DataFrame
num_rows = df_county_level_100_or_more.count()

# Display the results
print(f"Number of unique values in {fips_column_name}: {num_unique_values}")
print(f"Number of rows in df_county_level_100_or_more: {num_rows}")

# COMMAND ----------

df_county_level_100_or_more.show()

# COMMAND ----------

#create patient sample in counties with 100 or more members

patient_sample = spark.table("dua_058828_spa240.paper2_final_sample")
print(patient_sample.count())
#patient_sample.show(10)

fips_column_name = "fips_code"
fips_column = df_county_level_100_or_more.select(fips_column_name)
#fips_column.show()

patient_sample_100_or_more = patient_sample.join(fips_column, on="fips_code", how="inner")
print(patient_sample_100_or_more.count())

patient_sample_100_or_more.write.saveAsTable("dua_058828_spa240.paper2_final_sample_100_or_more", mode='overwrite')

# COMMAND ----------

#create acute care episodes in counties with 100 or more members

acute_care_episodes = spark.table("dua_058828_spa240.paper2_acute_care_visit_episodes")
print(acute_care_episodes.count())
#patient_sample.show(10)

fips_column_name = "fips_code"
fips_column = df_county_level_100_or_more.select(fips_column_name)
#fips_column.show()

acute_care_episodes_100_or_more = acute_care_episodes.join(fips_column, on="fips_code", how="inner")
print(acute_care_episodes_100_or_more.count())

acute_care_episodes_100_or_more.write.saveAsTable("dua_058828_spa240.paper2_acute_care_visit_episodes_100_or_more", mode='overwrite')

# COMMAND ----------

#create county-level file for acute care for counties with 100 or more members

print(df_county_level_100_or_more.count())
df_county_level_100_or_more.write.saveAsTable("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more", mode='overwrite')

# COMMAND ----------

