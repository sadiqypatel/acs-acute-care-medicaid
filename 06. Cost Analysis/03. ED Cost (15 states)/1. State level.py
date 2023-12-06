# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

ed2 = spark.table("dua_058828_spa240.paper2_ed_cost_other_file")

edvisit_conditions = (
    ed2["REV_CNTR_CD"].isin(['0450', '0451', '0452', '0453', '0454', '0456', '0457', '0458', '0459', '0981']) |
    ed2["POS_CD"].isin([23]) |
    ed2["LINE_PRCDR_CD"].isin(['99281', '99282', '99283', '99284', '99285'])
)

# Create the "EDvisit" binary indicator based on the conditions
ed_visits = ed2.withColumn("EDvisit", when(edvisit_conditions, 1).otherwise(0))

# Filter out rows where "EDvisit" is not equal to 1
ed_visits = ed_visits.filter(ed_visits["EDvisit"] == 1)
ed_visits.show()

# COMMAND ----------

inpatient_for_ed_state = ed_visits.select("beneID", "state", "fips_code", "medicaidMonths", "CLM_ID", "LINE_NUM", "CLM_TYPE_CD", "LINE_MDCD_PD_AMT", "LINE_MDCD_FFS_EQUIV_AMT")
print(inpatient_for_ed_state.count())
inpatient_for_ed_state.show()

# COMMAND ----------

df_ip = inpatient_for_ed_state.withColumn("ffs_amount", when(inpatient_for_ed_state['CLM_TYPE_CD'] == '1', inpatient_for_ed_state['LINE_MDCD_PD_AMT']).otherwise(lit(0))) \
           .withColumn("mc_amount", when(inpatient_for_ed_state['CLM_TYPE_CD'] == '3', inpatient_for_ed_state['LINE_MDCD_FFS_EQUIV_AMT']).otherwise(lit(0)))

df_ip = df_ip.fillna(0.0, subset=["ffs_amount", "mc_amount"])

df_ip = df_ip.withColumn("ffs_claim", when(df_ip['CLM_TYPE_CD'] == '1', lit(1)).otherwise(lit(0))) \
           .withColumn("mc_claim", when(df_ip['CLM_TYPE_CD'] == '3', lit(1)).otherwise(lit(0)))

selected_columns = ["beneID","state","fips_code","medicaidMonths","CLM_ID","LINE_NUM","CLM_TYPE_CD","LINE_MDCD_PD_AMT","LINE_MDCD_FFS_EQUIV_AMT", "ffs_amount", "mc_amount", "ffs_claim", "mc_claim"]
df_ip = df_ip.select(*selected_columns)
df_ip.show(500)

# COMMAND ----------

df_ip.registerTempTable("spend")
spend_county = spark.sql('''
select state, fips_code, beneID, CLM_ID, sum(ffs_amount) as ffs_amount, sum(mc_amount) as mc_amount
FROM spend 
GROUP BY state, fips_code, beneID, CLM_ID; 
''')

print(df_ip.count())
print(spend_county.count())
spend_county.show(1000)

# COMMAND ----------

#aggregate to total 

spend_county.registerTempTable("spend_fips")
spend_state = spark.sql('''
select state, sum(ffs_amount) as ffs_amount_ed, sum(mc_amount) as mc_amount_ed
FROM spend_fips 
GROUP BY state; 
''')

total_state = spend_state.withColumn(
    "total_cost_ed",
    spend_state["ffs_amount_ed"] + spend_state["mc_amount_ed"])

total_state.show(n=spend_state.count(), truncate=False)

# COMMAND ----------

ed_spend = total_state
ed_spend.show()

# COMMAND ----------

percent = spark.table("dua_058828_spa240.paper2_acute_care_percent")
percent = percent.select("state_name","member_month","ed_percent")
percent = percent.withColumnRenamed("state_name", "state")
combined = ed_spend.join(percent, how="left", on="state")
combined.show()

# COMMAND ----------

combined = combined.withColumn("avoid_ed_cost", (col("total_cost_ed") * col("ed_percent")) / 100)
#combined = combined.withColumn("avoid_ip_cost", (col("inpatient_cost") * col("ip_percent")) / 100)
#combined = combined.withColumn("avoid_acute_cost", (col("avoid_ed_cost") + col("avoid_ip_cost")))

# COMMAND ----------

combined.show()

# COMMAND ----------

combined = combined.withColumn("pmpy", (col("avoid_ed_cost") / col("member_month")) * 12)
combined.show()

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0,  0.25, 0.5, 0.75, 1.00]
deciles = combined.approxQuantile("pmpy", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------

#aggregate to total 

combined.registerTempTable("combined")
spend_total = spark.sql('''
select sum(avoid_ed_cost) as avoid_ed_cost, sum(member_month) as member_month
FROM combined ; 
''')

spend_total = spend_total.withColumn("pmpy", (col("avoid_ed_cost") / col("member_month")) * 12)
spend_total.show()

# COMMAND ----------

