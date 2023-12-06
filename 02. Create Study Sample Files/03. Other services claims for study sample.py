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

#inpatient claims
sample = spark.table("dua_058828_spa240.paper2_final_sample")
inpatient = spark.table("dua_058828_spa240.otherServices2019")
inpatient = inpatient.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
print((inpatient.count(), len(inpatient.columns)))
inpatient = inpatient.join(sample, on=['beneID','state'], how='inner')
print((inpatient.count(), len(inpatient.columns)))

inpatient.write.saveAsTable("dua_058828_spa240.paper2_other_services_file", mode='overwrite')

# COMMAND ----------

