# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

sample = spark.table("dua_058828_spa240.paper2_final_sample_100_or_more")
sample = sample.select("beneID","state")
sample.show(5)

# COMMAND ----------

inpatient = spark.table("dua_058828_spa240.paper2_inpatient_file")
print(inpatient.count())
inpatient = inpatient.join(sample, on=["beneID","state"], how="inner")
print(inpatient.count())

# COMMAND ----------

outpatient = spark.table("dua_058828_spa240.paper2_other_services_file")
#print(outpatient.count())
outpatient = outpatient.join(sample, on=["beneID","state"], how="inner")
#print(outpatient.count())

# COMMAND ----------

# Use the filter transformation
inpatient_filtered = inpatient.filter(col("state").isin(["AK","FL","ME","MT","NM","WY"]))
print(inpatient_filtered.count())

# COMMAND ----------

# Use the filter transformation
outpatient_filtered = outpatient.filter(col("state").isin(["AK","FL","ME","MT","NM","WY"]))
print(outpatient_filtered.count())

# COMMAND ----------

inpatient_filtered.write.saveAsTable("dua_058828_spa240.paper2_ed_and_ip_cost_ip_file", mode='overwrite')
outpatient_filtered.write.saveAsTable("dua_058828_spa240.paper2_ed_and_ip_cost_other_file", mode='overwrite')

# COMMAND ----------

