# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_visit_episodes")
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

df_with_fips = spark.table("dua_058828_spa240.paper2_final_sample")

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
final_file_county = final_file_county.withColumn('emergent_acute', col('non_avoid_ip') + col('non_avoid_ed'))
final_file_county = final_file_county.withColumn('all_cause_acute', col('avoid_ip') + col('avoid_ed') + col('non_avoid_ed') + col('non_avoid_ip'))
final_file_county = final_file_county.withColumn('total_ip', col('avoid_ip') + col('non_avoid_ip'))
final_file_county = final_file_county.withColumn('total_ed', col('avoid_ed') + col('non_avoid_ed'))
final_file_county = final_file_county.drop("numberOfEpisodes")
final_file_county.show()

# COMMAND ----------

final_file_county.write.saveAsTable("dua_058828_spa240.paper2_acute_care_visits_county_all", mode='overwrite')

# COMMAND ----------

