# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more")
df.show(1000)

# COMMAND ----------



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

df = spark.table("dua_058828_spa240.paper2_acute_care_visits_county_100_or_more")
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

# Specify the columns to check
columns_to_check = ["non_avoid_ip", "avoid_ip", "non_avoid_ed", "avoid_ed", "non_emergent_acute", "emergent_acute", "all_cause_acute", "total_ip", "total_ed"]

# Filter out rows where at least one of the specified columns has a value of 11 or less
filtered_df = df.filter(~(col(columns_to_check[0]) <= 11) &
                       ~(col(columns_to_check[1]) <= 11) &
                       ~(col(columns_to_check[2]) <= 11) &
                       ~(col(columns_to_check[3]) <= 11) &
                       ~(col(columns_to_check[4]) <= 11) &
                       ~(col(columns_to_check[5]) <= 11) &
                       ~(col(columns_to_check[6]) <= 11) &
                       ~(col(columns_to_check[7]) <= 11) &
                       ~(col(columns_to_check[8]) <= 11))

# Show the filtered DataFrame
filtered_df.show()

# COMMAND ----------

print(filtered_df.count())

# COMMAND ----------

filtered_df.write.format('csv').option('header', 'true').mode('overwrite').save("dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_county_output_file.csv")

# COMMAND ----------

dbutils.fs.cp(f"dbfs:/mnt/dua/dua_058828/SPA240/files/paper2_county_output_file.csv", "s3://apcws301-transfer/dua/dua_058828/toVDI/paper2_county_output_file.csv", recurse=True)

# COMMAND ----------

