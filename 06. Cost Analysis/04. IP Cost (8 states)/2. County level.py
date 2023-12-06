# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

ed2 = spark.table("dua_058828_spa240.paper2_inpatient_cost_ip_file")

# COMMAND ----------

inpatient_for_ed_state = ed2.select("beneID","state","fips_code","medicaidMonths","CLM_ID","LINE_NUM","CLM_TYPE_CD","LINE_MDCD_PD_AMT","LINE_MDCD_FFS_EQUIV_AMT","MDCD_PD_AMT")
print(inpatient_for_ed_state.count())
inpatient_for_ed_state.show()

# COMMAND ----------

df_ip = inpatient_for_ed_state.withColumn("total_amt1", when(inpatient_for_ed_state['CLM_TYPE_CD'] == '1', inpatient_for_ed_state['LINE_MDCD_PD_AMT']).otherwise(lit(0))).withColumn("total_amt2", when(inpatient_for_ed_state['CLM_TYPE_CD'] == '3', inpatient_for_ed_state['LINE_MDCD_FFS_EQUIV_AMT']).otherwise(lit(0)))


df_ip = df_ip.withColumn("total_amt3", when(df_ip['CLM_TYPE_CD'] == '1', df_ip['MDCD_PD_AMT']).otherwise(lit(0)))

df_ip = df_ip.fillna(0.0, subset=["total_amt1", "total_amt2","total_amt3"])

# df_new = df_new.withColumn("ffs_claim", when(outpat_final['CLM_TYPE_CD'] == '1', lit(1)).otherwise(lit(0))) \
#            .withColumn("mc_claim", when(outpat_final['CLM_TYPE_CD'] == '3', lit(1)).otherwise(lit(0)))

selected_columns = ["beneID","state","fips_code","medicaidMonths","CLM_ID","LINE_NUM","CLM_TYPE_CD","LINE_MDCD_PD_AMT","LINE_MDCD_FFS_EQUIV_AMT","MDCD_PD_AMT", "total_amt1", "total_amt2","total_amt3"]
df_ip = df_ip.select(*selected_columns)
print(df_ip.count())
df_ip.show(500)

# COMMAND ----------

df_ip.registerTempTable("spend")
spend_county = spark.sql('''
select state, fips_code, beneID, CLM_ID, sum(total_amt1) as total_amt1, mean(total_amt2) as total_amt2, mean(total_amt3) as total_amt3
FROM spend 
GROUP BY state, fips_code,  beneID, CLM_ID; 
''')

print(df_ip.count())
print(spend_county.count())
spend_county.show(1000)

# COMMAND ----------

#aggregate to total 

spend_county.registerTempTable("spend_fips")
spend_state = spark.sql('''
select fips_code, state, sum(total_amt1) as total_amt1, sum(total_amt2) as ffs_amount_ip, sum(total_amt3) as mc_amount_ip
FROM spend_fips 
GROUP BY fips_code, state; 
''')

total_state = spend_state.withColumn(
    "inpatient_cost",
    spend_state["ffs_amount_ip"] + spend_state["mc_amount_ip"])

total_state = total_state.drop("total_amt1")
total_state.show(n=spend_state.count(), truncate=False)

# COMMAND ----------

ip_spend = total_state
ip_spend.show()

# COMMAND ----------

percent = spark.table("dua_058828_spa240.paper2_acute_care_percent_county")
percent = percent.filter(col("state_name").isin(["FL","NM","VA","AK","ME","MT","SD","WY"]))
print(percent.count())
percent = percent.select("fips_code","member_month","ip_percent")
#percent = percent.withColumnRenamed("state_name", "state")
combined = ip_spend.join(percent, how="left", on="fips_code")
print(combined.count())
combined.show()

# COMMAND ----------

#combined = combined.withColumn("avoid_ed_cost", (col("total_cost_ed") * col("ed_percent")) / 100)
combined = combined.withColumn("avoid_ip_cost", (col("inpatient_cost") * col("ip_percent")) / 100)
#combined = combined.withColumn("avoid_acute_cost", (col("avoid_ed_cost") + col("avoid_ip_cost")))

# COMMAND ----------

combined.show()

# COMMAND ----------

combined = combined.withColumn("pmpy", (col("avoid_ip_cost") / col("member_month")) * 12)
combined.show()

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0,  0.25, 0.5, 0.75, 1.00]
deciles = combined.approxQuantile("pmpy", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------

df_select = combined.select("fips_code","state","avoid_ip_cost","pmpy")
df_select = df_select.withColumnRenamed("pmpy", "ip_pmpy")
df_select.show()

# COMMAND ----------

df_select.write.format('csv').option('header', 'true').mode('overwrite').save("dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_ip_spend.csv")

# COMMAND ----------

dbutils.fs.cp(f"dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_ip_spend.csv", "s3://apcws301-transfer/dua/dua_058828/toVDI/paper2_ip_spend.csv", recurse=True)

# COMMAND ----------

