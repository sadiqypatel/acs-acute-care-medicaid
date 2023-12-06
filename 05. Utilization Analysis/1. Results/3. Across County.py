# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more")
df.show(25)

# COMMAND ----------

# Example crosswalk DataFrame with FIPS codes for all 50 states
data = [
    ("01", "AL"), ("02", "AK"), ("04", "AZ"), ("05", "AR"), ("06", "CA"),
    ("08", "CO"), ("09", "CT"), ("10", "DE"), ("11", "DC"), ("12", "FL"),
    ("13", "GA"), ("15", "HI"), ("16", "ID"), ("17", "IL"), ("18", "IN"),
    ("19", "IA"), ("20", "KS"), ("21", "KY"), ("22", "LA"), ("23", "ME"),
    ("24", "MD"), ("25", "MA"), ("26", "MI"), ("27", "MN"), ("28", "MS"),
    ("29", "MO"), ("30", "MT"), ("31", "NE"), ("32", "NV"), ("33", "NH"),
    ("34", "NJ"), ("35", "NM"), ("36", "NY"), ("37", "NC"), ("38", "ND"),
    ("39", "OH"), ("40", "OK"), ("41", "OR"), ("42", "PA"), ("44", "RI"),
    ("45", "SC"), ("46", "SD"), ("47", "TN"), ("48", "TX"), ("49", "UT"),
    ("50", "VT"), ("51", "VA"), ("53", "WA"), ("54", "WV"), ("55", "WI"),
    ("56", "WY")
]

columns = ["state", "state_name"]

fips_crosswalk = spark.createDataFrame(data, columns)

#df = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more")
df = df.withColumn("state", col("fips_code").substr(1, 2))
df = df.join(fips_crosswalk, on="state", how="left")

# Get unique states and their counts
state_counts = df.groupBy("state_name").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show(n=state_counts.count(), truncate=False)

# COMMAND ----------

df.show(10)

# COMMAND ----------

#aggregate to total 
df.registerTempTable("total")
total = spark.sql('''
select state_name, fips_code, sum(total_members) as total_members, sum(member_month) as member_month, sum(non_avoid_ip) as non_avoid_ip, sum(avoid_ip) as avoid_ip, sum(total_ip) as total_ip, sum(non_avoid_ed) as non_avoid_ed, sum(avoid_ed) as avoid_ed, sum(total_ed) as total_ed, sum(non_emergent_acute) as non_emergent_acute, sum(emergent_acute) as emergent_acute, sum(all_cause_acute) as total_acute
FROM total
GROUP BY state_name, fips_code;
''')

total.show()

# COMMAND ----------

# Define a list of columns to be updated
from pyspark.sql.functions import col, round

columns_to_calculate = total.columns[4:]

# Define a function to perform the calculation
def calculate_and_round_ratio(row):
    return [(round(col(column) / col('member_month') * 1000, 1).alias(column)) for column in columns_to_calculate]

# Apply the calculation function to the specified columns
test = total.select("state_name", "fips_code", "member_month", *calculate_and_round_ratio(df))

# Show the updated DataFrame
test.show()

# COMMAND ----------

test = test.withColumn("ip_percent", round((col("avoid_ip") / col("total_ip") * 100), 1))
test = test.withColumn("ed_percent", round((col("avoid_ed") / col("total_ed") * 100), 1))
test = test.withColumn("acute_percent", round((col("non_emergent_acute") / col("total_acute") * 100), 1))
test = test.drop("non_avoid_ip","non_avoid_ed","emergent_acute")
test.show()

# COMMAND ----------

test.write.saveAsTable("dua_058828_spa240.paper2_acute_care_percent_county", mode='overwrite')

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("non_emergent_acute", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")
    
# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("acute_percent", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("avoid_ed", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("ed_percent", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------

# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("avoid_ip", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# Calculate the deciles
quantile_values = [0.0, 0.25, 0.5, 0.75, 1.00]
deciles = test.approxQuantile("ip_percent", quantile_values, 0.01)

# Display the decile distribution
for q, value in zip(quantile_values, deciles):
    print(f"Decile {int(q * 100)}%: {value}")

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_percent_county")
df.show()

# COMMAND ----------

df_select = df.select("state_name", "fips_code", "avoid_ed","avoid_ip","non_emergent_acute","ip_percent","ed_percent","acute_percent")
df_select = df_select.withColumnRenamed("non_emergent_acute", "avoid_acute")
df_select.show()

# COMMAND ----------

df_select.write.format('csv').option('header', 'true').mode('overwrite').save("dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_utilization_county.csv")

# COMMAND ----------

dbutils.fs.cp(f"dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_utilization_county.csv", "s3://apcws301-transfer/dua/dua_058828/toVDI/paper2_utilization_county.csv", recurse=True)