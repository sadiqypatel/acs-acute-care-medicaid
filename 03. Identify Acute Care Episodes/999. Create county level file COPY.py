# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df = spark.table("dua_058828_spa240.new_paper2_acute_care_visit_episodes")
df = df.drop("CLM_ID","ednnp","alcohol","drug","injury","psych","unclassified")
df.show()

# COMMAND ----------

df.registerTempTable("connections")

final_county = spark.sql('''
SELECT fips_code, count(episode) as numberOfEpisodes, sum(non_avoid_ip) as non_avoid_ip, sum(avoid_ip) as avoid_ip, sum(non_avoid_ed) as non_avoid_ed, sum(avoid_ed) as avoid_ed
FROM connections
GROUP BY fips_code;
''')

final_county.show()

# COMMAND ----------

df_with_fips = spark.table("dua_058828_spa240.new_paper2_final_sample")

#aggregate to total 
df_with_fips.registerTempTable("total")
county_file = spark.sql('''
select fips_code, count(beneID) as total_members, sum(medicaidMonths) as member_month
FROM total 
GROUP BY fips_code; 
''')

county_file.show()

# COMMAND ----------

print(county_file.count())
final_file_county = county_file.join(final_county, on="fips_code", how="left").fillna(0)
print(final_file_county.count())
final_file_county.show()

# COMMAND ----------

final_file_county = final_file_county.withColumn('non_emergent_acute', col('avoid_ip') + col('avoid_ed'))
final_file_county = final_file_county.withColumn('all_cause_acute', col('avoid_ip') + col('avoid_ed') + col('non_avoid_ed') + col('non_avoid_ip'))
final_file_county = final_file_county.withColumn('total_ip', col('avoid_ip') + col('non_avoid_ip'))
final_file_county = final_file_county.withColumn('total_ed', col('avoid_ed') + col('non_avoid_ed'))
final_file_county = final_file_county.drop("numberOfEpisodes")
final_file_county.show()

# COMMAND ----------

final_file_county.write.saveAsTable("dua_058828_spa240.new_paper2_acute_care_visits_county", mode='overwrite')

# COMMAND ----------

final_file_county = spark.table("dua_058828_spa240.new_paper2_acute_care_visits_county")
final_file_county.show()

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0, 0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.00]
deciles = final_file_county.approxQuantile("total_members", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------



# COMMAND ----------

# #get the top dx for avoidable ED and IP visits

# #merge df final into the df with diagnosis that has not yet been aggregated to keep visibility over dx [df]

# #create df for avoidable IP only
# ip = final1.select("beneID","state","episode","avoid_ip")
# ip = ip.filter(ip.avoid_ip==1)
# ip.show()

# #create df for avoidable ED only 
# ed = final1.select("beneID","state","episode","avoid_ed")
# ed = ed.filter(ed.avoid_ed==1)
# ed.show()

# COMMAND ----------

# all = df.select("beneID","state","DGNS_CD_1","episode","avoidEdVisit","nonAvoidEdVisit","avoidIpVisit","nonAvoidIpVisit","StartDate","EndDate")
# print((all.count(), len(all.columns)))
# all.show()

# COMMAND ----------

# all_ip = all.join(ip, on=['beneID','state','episode'], how='inner')
# print((all_ip.count(), len(all_ip.columns)))
# all_ip.groupBy('avoid_ip','avoidIpVisit', 'nonAvoidIpVisit','avoidEdVisit', 'nonAvoidEdVisit').count().show()

# COMMAND ----------

# all_ip_keep = all_ip.filter(col("avoidIpVisit") == 1)
# all_ip_keep = all_ip_keep.filter(col("avoidEdVisit") == 0)
# all_ip_keep = all_ip_keep.filter(col("nonAvoidEdVisit") == 0)

# all_ip_keep.groupBy('avoid_ip','avoidIpVisit', 'nonAvoidIpVisit','avoidEdVisit', 'nonAvoidEdVisit').count().show()

# COMMAND ----------

# all_ed = all.join(ed, on=['beneID','state','episode'], how='inner')
# print((all_ed.count(), len(all_ed.columns)))
# all_ed.groupBy('avoid_ed','avoidEdVisit','nonAvoidEdVisit','avoidIpVisit','nonAvoidIpVisit').count().show()

# COMMAND ----------

# all_ed_keep = all_ed.filter(col("avoidEdVisit") == 1)
# all_ed_keep = all_ed_keep.filter(col("avoidIpVisit") == 0)
# all_ed_keep = all_ed_keep.filter(col("nonAvoidIpVisit") == 0)

# all_ed_keep.groupBy('avoid_ed','avoidEdVisit', 'nonAvoidEdVisit', 'avoidIpVisit', 'nonAvoidIpVisit').count().show()

# COMMAND ----------

# print(all_ed_keep.printSchema())

# COMMAND ----------

# print(all_ip_keep.printSchema())

# COMMAND ----------

# all_ed_keep.registerTempTable("connections")

# ed_diag = spark.sql('''
# SELECT beneID, state, episode, max(DGNS_CD_1) as diag, max(avoid_ed) as avoid_ed, max(avoidIpVisit) as avoidIpVisit, max(nonAvoidIpVisit) as nonAvoidIpVisit, max(avoidEdVisit) as avoidEdVisit, max(nonAvoidEdVisit) as nonAvoidEdVisit
# FROM connections
# GROUP BY beneID, state, episode;
# ''')

# COMMAND ----------

# all_ip_keep.registerTempTable("connections")

# ip_diag = spark.sql('''
# SELECT beneID, state, episode, max(DGNS_CD_1) as diag, max(avoid_ip) as avoid_ip, max(avoidEdVisit) as avoidEdVisit, max(nonAvoidEdVisit) as nonAvoidEdVisit, max(avoidIpVisit) as avoidIpVisit, max(nonAvoidIpVisit) as nonAvoidIpVisit
# FROM connections
# GROUP BY beneID, state, episode;
# ''')

# COMMAND ----------

# ed_diag.groupBy('avoid_ed','avoidEdVisit', 'nonAvoidEdVisit', 'avoidIpVisit', 'nonAvoidIpVisit').count().show()
# ip_diag.groupBy('avoid_ip','avoidIpVisit', 'nonAvoidIpVisit','avoidEdVisit', 'nonAvoidEdVisit').count().show()

# COMMAND ----------

# dbutils.fs.ls("s3://apcws301-transfer/dua/dua_058828/inbound/")

# COMMAND ----------

# import pandas as pd
# ccsr_df = pd.read_csv("/dbfs/mnt/dua/dua_058828/SPA240/files/ccsrCategory.csv")
# ccsr_df["diagnosisOne"] = ccsr_df["diagnosisOne"].str.strip("'")
# ccsr_df["ccsrCatOne"] = ccsr_df["ccsrCatOne"].str.strip("'")
# ccsr_df.head()

# COMMAND ----------

# spark_ccsr = spark.createDataFrame(ccsr_df)
# spark_ccsr = spark_ccsr.withColumnRenamed("diagnosisOne", "diag")
# spark_ccsr.show()

# COMMAND ----------

# ed_ccsr_diag = ed_diag.join(spark_ccsr, on=['diag'], how='left')
# missing_count = ed_ccsr_diag.select("ccsrCatOne").where(col("ccsrCatOne").isNull()).count()
# print(missing_count)
# ed_ccsr_diag.show()

# #35,858,175

# COMMAND ----------

# ip_ccsr_diag = ip_diag.join(spark_ccsr, on=['diag'], how='left')
# missing_count = ip_ccsr_diag.select("ccsrCatOne").where(col("ccsrCatOne").isNull()).count()
# print(missing_count)
# ip_ccsr_diag.show()

# COMMAND ----------

# print((ed_ccsr_diag.count(), len(ed_ccsr_diag.columns)))
# print((ed_diag.count(), len(all_ed_keep.columns)))

# print((ip_ccsr_diag.count(), len(ip_ccsr_diag.columns)))
# print((ip_diag.count(), len(all_ip_keep.columns)))

# COMMAND ----------

# ed_ccsr_diag.registerTempTable("connections")

# ccsr_ed = spark.sql('''
# SELECT ccsrCatOne, count(avoid_ed) as episode
# FROM connections
# GROUP BY ccsrCatOne;
# ''')

# ccsr_ed.show(n=df.count(), truncate=False)

# COMMAND ----------

# ip_ccsr_diag.registerTempTable("connections")

# ccsr_ip = spark.sql('''
# SELECT ccsrCatOne, count(avoid_ip) as episode
# FROM connections
# GROUP BY ccsrCatOne;
# ''')

# ccsr_ip.show(n=df.count(), truncate=False)