# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more")
df.show()

# COMMAND ----------

df = df.select("fips_code","total_members","member_month")
df.show()

# COMMAND ----------

# Calculate basic statistics using summary
basic_stats = df.select("total_members").summary("min", "25%", "50%", "75%", "max")

# Calculate additional percentiles using approxQuantile
additional_percentiles = df.approxQuantile("total_members", [0.9, 0.95, 0.99], 0)

# Combine the results
result = basic_stats.union(
    spark.createDataFrame([("90%", additional_percentiles[0]), 
                           ("95%", additional_percentiles[1]),
                           ("99%", additional_percentiles[2])],
                          ["summary", "total_members"])
)

# Order the result DataFrame
result = result.orderBy("summary")

# Show the result
result.show()

# COMMAND ----------

df.write.saveAsTable("dua_058828_spa240.paper_2_output_for_member_months_new", mode='overwrite')
df.show(n=df.count(), truncate=False)

# COMMAND ----------

df.write.format('csv').option('header', 'true').mode('overwrite').save("dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_output_member_months.csv")

# COMMAND ----------

#dbutils.fs.ls(f"dbfs:/mnt/dua/dua_058828/SPA240/files/")

# COMMAND ----------

dbutils.fs.cp(f"dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_output_member_months.csv", "s3://apcws301-transfer/dua/dua_058828/toVDI/paper2_output_member_months.csv", recurse=True)

# COMMAND ----------

