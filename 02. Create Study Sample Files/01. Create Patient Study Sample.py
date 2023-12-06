# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np

# COMMAND ----------

df = spark.table("dua_058828_spa240.demo2019")
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

# Group by 'beneID' and count the number of occurrences
df_count = df.groupBy("beneID").count()

# Filter out the 'beneID' values that occur more than once
df_count = df_count.filter(df_count["count"] == 1)

# Drop the 'count' column
df_count = df_count.drop("count")

# Join the original DataFrame with the filtered DataFrame to remove the duplicate rows
df = df.join(df_count, on="beneID", how="inner")

print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

df = df.dropna(subset=['beneID','state'])
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

df.registerTempTable("connections")
df = spark.sql('''
SELECT distinct beneID, state, max(age) as age, max(disabled) as disabled
FROM connections
GROUP BY beneID, state;
''')

print((df.count(), len(df.columns)))

# COMMAND ----------

df = df.filter(~col("state").isin(['RI', 'UT', 'AL', 'KY', 'OK', 'NY', 'MA', 'NH', 'CT', 'TN', 'MD', 'ID', 'MN', 'NJ', 'TX', 'NE', 'PR', 'VI']))
print((df.count(), len(df.columns)))
df.show()

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

member = spark.table("dua_058828_spa240.elig2019")
member = member.select("beneID", "state", "dual","medEnroll")
member = member.withColumn("dualInd", when((col("dual").isin(['yes'])), lit(1)).otherwise(lit(0))).withColumn("medicaidMonths", when((col("medEnroll").isin(['yes'])), lit(1)).otherwise(lit(0)))
member.show(10)
print((member.count(), len(member.columns)))

# Count the number of distinct values in a column
distinct_count = member.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

member.registerTempTable("connections")
memberDf = spark.sql('''
SELECT beneID, state, sum(dualInd) as medicareMonths, sum(medicaidMonths) as medicaidMonths
FROM connections
GROUP BY beneID, state;
''')

print((memberDf.count(), len(memberDf.columns)))

# Count the number of distinct values in a column
distinct_count = memberDf.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

print((df.count(), len(df.columns)))
print((memberDf.count(), len(memberDf.columns)))

dfNew = df.join(memberDf, on=['beneID','state'], how='left')
print((dfNew.count(), len(dfNew.columns)))
dfNew.show(25)  

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

#remove all duals
#1 -- remove members with any enrollment in medicare
#2 -- remove members who are 65 and older

dfNew = dfNew.filter(dfNew.medicareMonths ==0)

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

dfNew = dfNew.filter(dfNew.age < 65)

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

dfNew = dfNew.dropna(subset=["medicaidMonths"])

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

print((dfNew.count(), len(dfNew.columns)))
dfNew.show(10)

# COMMAND ----------

county_data = spark.table("dua_058828_spa240.paper2_member_state_county_2019")
county_data = county_data.drop("BENE_CNTY_CD")
county_data = county_data.withColumn('dummy', lit(1))
county_data = county_data.withColumnRenamed('STATE_CD', 'state').withColumnRenamed('BENE_ID', 'beneID')
#print(county_data.count())
print(dfNew.count())
sample = dfNew.join(county_data, how="inner", on=["beneID", "state"])
print(sample.count())
sample.show()

# COMMAND ----------

# Drop rows with null values in the "age" column
column_with_nulls = "county_code"
df = sample.na.drop(subset=[column_with_nulls])
df = df.drop(df.dummy)
# Show the resulting DataFrame
print(df.count())
df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# Define the crosswalk between state codes and FIPS codes
state_fips_crosswalk = [
    ("AL", "01"), ("AK", "02"), ("AZ", "04"), ("AR", "05"), ("CA", "06"), ("CO", "08"), ("CT", "09"),
    ("DE", "10"), ("FL", "12"), ("GA", "13"), ("HI", "15"), ("ID", "16"), ("IL", "17"), ("IN", "18"),
    ("IA", "19"), ("KS", "20"), ("KY", "21"), ("LA", "22"), ("ME", "23"), ("MD", "24"), ("MA", "25"),
    ("MI", "26"), ("MN", "27"), ("MS", "28"), ("MO", "29"), ("MT", "30"), ("NE", "31"), ("NV", "32"),
    ("NH", "33"), ("NJ", "34"), ("NM", "35"), ("NY", "36"), ("NC", "37"), ("ND", "38"), ("OH", "39"),
    ("OK", "40"), ("OR", "41"), ("PA", "42"), ("RI", "44"), ("SC", "45"), ("SD", "46"), ("TN", "47"),
    ("TX", "48"), ("UT", "49"), ("VT", "50"), ("VA", "51"), ("WA", "53"), ("WV", "54"), ("WI", "55"),
    ("WY", "56"), ("DC", "11"), ("AS", "60"), ("GU", "66"), ("MP", "69"), ("PR", "72"), ("VI", "78")
]

# Create a DataFrame from the crosswalk data
crosswalk_df = spark.createDataFrame(state_fips_crosswalk, ["state", "fips_state"])

# Join the original DataFrame with the crosswalk DataFrame
df_with_fips = df.join(crosswalk_df, on="state", how="left")

# Check for missing fips_state values
missing_fips_states = df_with_fips.filter(col("fips_state").isNull())
missing_fips_states.show()

# Show the resulting DataFrame
df_with_fips.show()

# COMMAND ----------

# Combine fips_state and county_code into a single column
df_with_fips = df_with_fips.withColumn("fips_code", concat(col("fips_state"), col("county_code")))

# Show the resulting DataFrame
df_with_fips.show(250)

# COMMAND ----------

# Count the number of unique fips_code values
unique_fips_count1 = df_with_fips.select("fips_code").distinct().count()
unique_fips_count2 = df_with_fips.select(["fips_state","county_code"]).distinct().count()


# Print the count of unique fips_code values
print("Number of unique fips_code values:", unique_fips_count1)
print("Number of unique fips_code values:", unique_fips_count2)

# COMMAND ----------

df_with_fips.write.saveAsTable("dua_058828_spa240.paper2_final_sample", mode='overwrite')

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_final_sample")
#df.show()

# Get unique states and their counts
state_counts = df.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show()

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_visit_episodes_100_or_more")
#df.show()

# Get unique states and their counts
state_counts = df.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show()

# COMMAND ----------

