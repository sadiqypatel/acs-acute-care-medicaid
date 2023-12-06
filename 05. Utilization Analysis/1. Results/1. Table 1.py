# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

sample = spark.table("dua_058828_spa240.paper2_final_sample_100_or_more")
#sample.show(5)

# COMMAND ----------

demo = spark.read.table("dua_058828_spa240.demo2019")
demo = demo.withColumn("demo", lit(1))
demo = demo.drop("county")
demo = demo.drop("disabled")
#demo.show(10)

# COMMAND ----------

df = sample.join(demo, on=["beneID","state"], how="left")
df = df.fillna({"demo": 0})
df.groupBy("demo").count().show()
#df.show(15)

# COMMAND ----------

df = df.withColumn("censusRegion", 
                        when((col("state").isin(['AZ','HI','WA','OR','CA','MT','ID','NV','WY','UT','AZ','CO','NM'])), 'West')
                       .when((col("state").isin(['ND','SD','NE','KS','MN','AI','MO','WI','IL','MI','IN','OH'])), 'Midwest')
                       .when((col("state").isin(['PA','NY','NJ','VT','NH','MA','CT','RI','ME'])), 'Northeast')        
                       .otherwise('South')) 

# COMMAND ----------

# Import PySpark and create a SparkSession

spark = SparkSession.builder \
        .appName("ColumnPercentages") \
        .getOrCreate()

# Define a function to calculate column percentages for a single categorical column
def calculate_percentages(df, column_name):
    category_counts = df.groupBy(column_name).agg(count("*").alias("Count"))
    total_rows = df.count()
    category_percentages = category_counts.withColumn("Percentage", round((col("Count") / total_rows) * 100, 1))
    return category_percentages
  
# Read the table into a PySpark DataFrame
#df = spark.table("dua_058828_spa240.stage1_final_analysis")
print((df.count(), len(df.columns)))

# List of categorical columns
categorical_columns = ["ageCat", "sex", "race","censusRegion","houseSize","fedPovLine","speakEnglish","married","UsCitizen","ssi","ssdi","tanf","disabled"]

# Calculate and display column percentages for each categorical column
for column_name in categorical_columns:
    print(f"Column Percentages for {column_name}:")
    calculate_percentages(df, column_name).show()
    
# Stop the Spark session
#spark.stop()