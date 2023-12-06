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
#df.groupBy("demo").count().show()
#df.show(15)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import sum

df = df.select("state","ageCat")

# Get unique categories
unique_categories = df.select("ageCat").distinct().rdd.flatMap(lambda x: x).collect()

# Create columns with 0/1 indicators for each category
for category in unique_categories:
    df = df.withColumn(category, when(df["ageCat"] == category, 1).otherwise(0))

# Show the result
#df.show(truncate=False)

# COMMAND ----------

state_df = df.groupBy("state").agg(
    sum("50To64").alias("sum_50To64"),
    sum("10To17").alias("sum_10To17"),
    sum("40To49").alias("sum_40To49"),
    sum("30To39").alias("sum_30To39"),
    sum("under10").alias("sum_under10"),
    sum("missing").alias("sum_missing"),
    sum("18To29").alias("sum_18To29")
)

#state_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum, col
state_df = state_df.withColumn("total", expr("sum_50To64 + sum_10To17 + sum_40To49 + sum_30To39 + sum_under10 + sum_missing + sum_18To29"))
#state_df.show()

# COMMAND ----------

percentage_columns = ["sum_50To64", "sum_10To17", "sum_40To49", "sum_30To39", "sum_under10", "sum_missing", "sum_18To29"]

# Calculate the percentage of total patients for each age group
for column in percentage_columns:
    state_df = state_df.withColumn(f"{column}_percentage", round(expr(f"{column} / total * 100"), 1))

# Show the result
state_df = state_df.select("state", "total", *[f"{column}_percentage" for column in percentage_columns])

# COMMAND ----------

state_df.show()

# COMMAND ----------

df_percent = state_df.withColumn("total_percentage", expr("sum_50To64_percentage + sum_10To17_percentage + sum_40To49_percentage + sum_30To39_percentage + sum_under10_percentage + sum_missing_percentage + sum_18To29_percentage"))

# Show the result
df_percent.show(truncate=False)

# COMMAND ----------

visits = spark.table("dua_058828_spa240.paper2_acute_care_percent")
visits = visits.select("state_name","non_emergent_acute")
visits = visits.withColumnRenamed("state_name", "state")
visits = visits.withColumnRenamed("non_emergent_acute", "avoid_acute")
visits.show()

# COMMAND ----------

visits = visits.join(df_percent, on="state", how="left")
visits.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# Assemble features into a vector column
feature_columns = ["sum_50To64_percentage", "sum_10To17_percentage", "sum_40To49_percentage", "sum_30To39_percentage", "sum_under10_percentage", "sum_missing_percentage", "sum_18To29_percentage"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_assembled = assembler.transform(visits)

# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="avoid_acute")

# Fit the model
model = lr.fit(df_assembled)

# Make predictions
predictions = model.transform(df_assembled)

# Show the results
predictions.show()

# COMMAND ----------

prediction_show = predictions.select("state","prediction")

# COMMAND ----------

prediction_show.show(n=prediction_show.count(), truncate=False)

# COMMAND ----------

