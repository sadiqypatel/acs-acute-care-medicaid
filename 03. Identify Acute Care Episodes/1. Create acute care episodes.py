# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql.functions import lpad

# COMMAND ----------

sample = spark.table("dua_058828_spa240.paper2_final_sample")
print(sample.count())
sample.show(10)

# COMMAND ----------

sample = sample.select("beneID","state","fips_code")
sample.show()

# COMMAND ----------

outpat = spark.table("dua_058828_spa240.paper2_other_services_file")
outpat_selected = outpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"LINE_PRCDR_CD" ,"SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "POS_CD")
print(outpat.count())
outpat_selected = outpat_selected.join(sample, how="inner", on=["beneID","state"])
print(outpat_selected.count())
outpat_selected = outpat_selected.withColumn("inpatientVisit", lit(0))
outpat_selected.show(10)

# COMMAND ----------

inpat = spark.table("dua_058828_spa240.paper2_inpatient_file")
inpat_selected = inpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"SRVC_BGN_DT", "SRVC_END_DT", "DGNS_CD_1")
print(inpat.count())
inpat_selected = inpat_selected.join(sample, how="inner", on=["beneID","state"])
print(inpat_selected.count())
inpat_selected = inpat_selected.withColumn("inpatientVisit", lit(1))
inpat_selected = inpat_selected.withColumn("EDvisit", lit(0))
#print(inpat_selected.printSchema())
#inpat_selected.show()

# COMMAND ----------

# Get unique states and their counts
state_counts = inpat_selected.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show(n=state_counts.count(), truncate=False)

# COMMAND ----------

# Get unique states and their counts
state_counts = outpat_selected.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show(n=state_counts.count(), truncate=False)

# COMMAND ----------

# Define the conditions for the "EDvisit" binary indicator
edvisit_conditions = (
    outpat_selected["REV_CNTR_CD"].isin(['0450', '0451', '0452', '0453', '0454', '0456', '0457', '0458', '0459', '0981']) |
    outpat_selected["POS_CD"].isin([23]) |
    outpat_selected["LINE_PRCDR_CD"].isin(['99281', '99282', '99283', '99284', '99285'])
)

# Create the "EDvisit" binary indicator based on the conditions
outpat_selected = outpat_selected.withColumn("EDvisit", when(edvisit_conditions, 1).otherwise(0))

# Create the "inpatientVisit" binary indicator and set it equal to 0
outpat_selected = outpat_selected.withColumn("inpatientVisit", lit(0))

# Filter out rows where "EDvisit" is not equal to 1
outpat_selected = outpat_selected.filter(outpat_selected["EDvisit"] == 1)

# Show the result
print(outpat_selected.count())
outpat_selected.show(1000)

# COMMAND ----------

inpatFinal = inpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")
outpatFinal = outpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")

# Show the result
inpatFinal.show(1)
outpatFinal.show(1)

# COMMAND ----------

df =  inpatFinal.union(outpatFinal)
df = df.withColumnRenamed("SRVC_BGN_DT", "StartDate").withColumnRenamed("SRVC_END_DT", "EndDate")
print(df.printSchema())

# COMMAND ----------

df = df.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as cumsum, when, row_number
from pyspark.sql.window import Window

def episodesOfCare(df):
    # Define window specifications for calculating lag values, cumulative sum, and row number
    beneID_state_window = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate")
    beneID_state_window_cumsum = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate").rowsBetween(Window.unboundedPreceding, 0)

    # Calculate lag values for StartDate and EndDate columns
    df = df.withColumn("prev_StartDate", lag("StartDate").over(beneID_state_window))
    df = df.withColumn("prev_EndDate", lag("EndDate").over(beneID_state_window))

    # Calculate row number within each group
    df = df.withColumn("row_num", row_number().over(beneID_state_window))

    # Define conditions for new episode and overlap types
    new_episode_condition = (col("StartDate") > col("prev_EndDate") + 1) | col("prev_EndDate").isNull()
    regular_overlap_condition = (col("StartDate") <= col("prev_EndDate") + 1) & (col("EndDate") > col("prev_EndDate"))
    same_start_date_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    embedded_condition = (col("StartDate") > col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    perfect_overlap_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") == col("prev_EndDate"))

    # Assign new episode flag based on condition
    df = df.withColumn("new_episode_flag", new_episode_condition.cast("int"))

    # Calculate episode numbers using cumulative sum
    df = df.withColumn("episode", cumsum("new_episode_flag").over(beneID_state_window_cumsum))

    df = df.withColumn("ovlp", 
                   when(col("row_num") == 1, "1.First")
                   .when(new_episode_condition, "2.New Episode")
                   .when(regular_overlap_condition, "3.Regular Overlap")
                   .when(same_start_date_condition, "5.Same Start Date (SRO)")
                   .when(embedded_condition, "6.Embedded")
                   .when(perfect_overlap_condition, "7.Perfect Overlap"))

    # Drop unnecessary columns
    df = df.drop("prev_StartDate", "prev_EndDate", "new_episode_flag", "row_num")

    return df
  
# Convert 'StartDate' and 'EndDate' columns to date type
df = df.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# Apply the episodesOfCare function
result_df = episodesOfCare(df)

# Sort the DataFrame by beneID, state, StartDate, and EndDate
#result_df = result_df.orderBy("beneID", "state", "StartDate", "EndDate")

# Show the result
result_df.show(1000)

# COMMAND ----------

ed = spark.read.csv("dbfs:/mnt/dua/dua_058828/SPA240/files/AvoidableEdVisit.csv", header=True, inferSchema=True)
ed.show()

# COMMAND ----------

avoidFile = ed
avoidFile = avoidFile.withColumn('sum', col('ednp') + col('pct') + col('nonemergent'))
# Select the column to review
col_name = "sum"
col_values = avoidFile.select(col(col_name)).rdd.flatMap(lambda x: x).collect()

# Compute basic summary statistics
summary = avoidFile.select(col(col_name)).describe().toPandas()

# Print the summary statistics
print(summary)

# Create the 'AvoidableEdVisit' column based on the value of the 'sum' column
avoidFile = avoidFile.withColumn('avoid1', when(col('sum') > 0, 1).otherwise(0))
avoidFile = avoidFile.withColumn('avoid2', when(col('sum') > 0.5, 1).otherwise(0))
avoidFile = avoidFile.withColumn('avoid3', when(col('sum') > 0.75, 1).otherwise(0))
avoidFile = avoidFile.withColumn('avoid4', when(col('sum') > 0.9, 1).otherwise(0))
avoidFile = avoidFile.withColumn('avoid5', when(col('sum') ==1, 1).otherwise(0))

avoidFile.groupBy('avoid1').count().show()
avoidFile.groupBy('avoid2').count().show()
avoidFile.groupBy('avoid3').count().show()
avoidFile.groupBy('avoid4').count().show()
avoidFile.groupBy('avoid5').count().show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as pyspark_sum

def cleanAvoidEdFile(df):

    # Read the CSV file into a PySpark DataFrame
    avoidFile = ed

    # Calculate the 'sum' column as the sum of 'ednp', 'pct', and 'nonemergent'
    avoidFile = avoidFile.withColumn('sum', col('ednp') + col('pct') + col('nonemergent'))

    # Create the 'AvoidableEdVisit' column based on the value of the 'sum' column
    avoidFile = avoidFile.withColumn('AvoidableEdVisit', when(col('sum') > 0.5, 1).otherwise(0))

    # Select the 'dx' and 'AvoidableEdVisit' columns to create the 'avoidEdVisitFile' DataFrame
    #avoidEdVisitFile = avoidFile.select('dx', 'AvoidableEdVisit')
    avoidEdVisitFile = avoidFile.drop('sum', '_c0')
    
    #rename diagnosis column
    avoidEdVisitFile = avoidEdVisitFile.withColumnRenamed("dx", "DGNS_CD_1")

    # Print the value counts for the 'AvoidableEdVisit' column
    avoidFile.groupBy('AvoidableEdVisit').count().show()
    avoidEdVisitFile.groupBy('AvoidableEdVisit').count().show()
    
    return avoidEdVisitFile

    # Write the 'avoidEdVisitFile' DataFrame to a CSV file
    #avoidEdVisitFile.write.csv('avoidEdVisitFinal.csv', header=True)

# Call the cleanAvoidEdFile function (pass the file path as an argument if needed)
avoidEdVisitFile = cleanAvoidEdFile(ed)
avoidEdVisitFile.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as pyspark_max, greatest, when

avoidEdVisitFile1 = avoidEdVisitFile.withColumnRenamed('AvoidableEdVisit', 'avoid1') \
       .withColumn('avoid1', col('avoid1').cast('string'))

# Drop duplicates and reset index (PySpark DataFrames do not have an index)
avoidEdVisitFile1  = avoidEdVisitFile1.dropDuplicates()
avoidEdVisitFile1.show()

# Read the input DataFrame 'df' (replace this with your actual DataFrame)
df = result_df
df.show(5)

# # Left join 'df' with 'ed' based on the 'DGNS_CD_1' column
df = df.join(avoidEdVisitFile1, on='DGNS_CD_1', how='left')
df.show(5)

# # Clean the 'avoid1' column
df = df.withColumn('avoid1', when(col('avoid1').isNull(), 0)
                           .when(col('avoid1') == 'N/A', 0)
                           .when(col('avoid1') == '', 0)
                           .otherwise(col('avoid1')).cast('int'))
df.show()

# # Extract the first three characters of the 'DGNS_CD_1' column into a new column 'dx'
# df = df.withColumn('dx', col('DGNS_CD_1').substr(1, 3))
# df.show()

# # # Rename columns
# avoidEdVisitFile2 = avoidEdVisitFile.withColumnRenamed('DGNS_CD_1', 'dx').withColumnRenamed('AvoidableEdVisit', 'avoid2')

# # Convert 'avoid2' to string type
# avoidEdVisitFile2 = avoidEdVisitFile2.withColumn('avoid2', col('avoid2').cast('string'))

# # # Drop duplicates
# avoidEdVisitFile2 = avoidEdVisitFile2.dropDuplicates()
# avoidEdVisitFile2.show() 

# # # Join 'df' with 'ed' based on the 'dx' column (left join)
# df = df.join(avoidEdVisitFile2, on='dx', how='left')

# # # Clean the 'avoid2' column
# df = df.withColumn('avoid2', when(col('avoid2').isNull(), 0)
#                            .when(col('avoid2') == 'N/A', 0)
#                            .when(col('avoid2') == '', 0)
#                            .otherwise(col('avoid2')).cast('int'))

# # # Create the 'avoidableEdVisit' column as the maximum of 'avoid1' and 'avoid2'
# df = df.withColumn('avoidableEdVisit', greatest(col('avoid1'), col('avoid2')))

df = df.withColumn('avoidableEdVisit', (col('avoid1')))

# # Drop temporary columns 'dx', 'avoid1', and 'avoid2'
#df = df.drop('dx', 'avoid1', 'avoid2')
#df = df.drop('dx', 'avoid1')

# # Show the result (optional)
df.show()

# COMMAND ----------

ip = spark.read.csv("dbfs:/mnt/dua/dua_058828/SPA240/files/avoidableIpVisits.csv", header=True, inferSchema=True)
ip.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Rename the column 'diagnosisOne' to 'DGNS_CD_1' in the 'pqi' DataFrame
ip = ip.withColumnRenamed('diagnosisOne', 'DGNS_CD_1')

# Drop the columns 'pqiLabel', 'icd10Label', and 'pqiNum'
ip = ip.drop('pqiLabel', 'icd10Label', 'pqiNum')

# Drop duplicates
ip = ip.dropDuplicates()

# Add a new column 'avoidableIpVisit' and set it to 1 for all rows
ip = ip.withColumn('avoidableIpVisit', lit(1))

# Left join 'ip' with 'pqi' based on the 'DGNS_CD_1' column
df = df.join(ip, on='DGNS_CD_1', how='left')

# Clean the 'avoidableIpVisit' column
df = df.withColumn('avoidableIpVisit', when(col('avoidableIpVisit').isNull(), 0)
                                      .when(col('avoidableIpVisit') == 'N/A', 0)
                                      .when(col('avoidableIpVisit') == '', 0)
                                      .otherwise(col('avoidableIpVisit')).cast('int'))

# Show the result (optional)
df.show()

# COMMAND ----------

# Define the conditions and create new columns 'allCauseEdVisit' and 'avoidEdVisit'
df = df.withColumn('allCauseEdVisit', when(col('EDvisit') == 1, 1).otherwise(0)) 
df = df.withColumn('avoidEdVisit', when((col('EDvisit') == 1) & (col('avoidableEdVisit') == 1), 1).otherwise(0))
df = df.withColumn('nonAvoidEdVisit', when((col('EDvisit') == 1) & (col('avoidableEdVisit') == 0), 1).otherwise(0))

# Define the conditions and create new columns 'allCauseIpVisit' and 'avoidIpVisit'
df = df.withColumn('allCauseIpVisit', when(col('inpatientVisit') == 1, 1).otherwise(0)) 
df = df.withColumn('avoidIpVisit', when((col('inpatientVisit') == 1) & (col('avoidableIpVisit') == 1), 1).otherwise(0))
df = df.withColumn('nonAvoidIpVisit', when((col('inpatientVisit') == 1) & (col('avoidableIpVisit') == 0), 1).otherwise(0))

# Show the result (optional)
df.show(250)

# COMMAND ----------

df.write.saveAsTable("dua_058828_spa240.paper2_final_visits", mode='overwrite')

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper2_final_visits")
df = df.drop("CLM_ID","ednnp","alcohol","drug","injury","psych","unclassified")
df.show()

# COMMAND ----------

df.registerTempTable("connections")

dfAgg = spark.sql('''
SELECT beneID, state, episode, max(allCauseEdVisit) as allCauseEdVisit, max(avoidEdVisit) as avoidEdVisit, max(nonAvoidEdVisit) as nonAvoidEdVisit, max(allCauseIpVisit) as allCauseIpVisit, max(avoidIpVisit) as avoidIpVisit, max(nonAvoidIpVisit) as nonAvoidIpVisit, min(StartDate) as StartDate, max(EndDate) as EndDate, max(DGNS_CD_1) as DGNS_CD_1
FROM connections
GROUP BY beneID, state, episode;
''')

dfAgg.show(250)

# COMMAND ----------

final = dfAgg.withColumn('allCauseIp', when(col('allCauseIpVisit') == 1, 1).otherwise(0))
final = final.withColumn('avoidIp', when(col('avoidIpVisit') == 1, 1).otherwise(0))
final = final.withColumn('nonAvoidIp', when(col('nonAvoidIpVisit') == 1, 1).otherwise(0))

# Define the conditions and create new columns 'allCauseEd' and 'avoidableEd' only for non-IP visits
final = final.withColumn('allCauseEd', when((col('allCauseIp') == 0) & (col('allCauseEdVisit') == 1), 1).otherwise(0)) 
final = final.withColumn('avoidableEd', when((col('allCauseIp') == 0) & (col('avoidEdVisit') == 1), 1).otherwise(0))
final = final.withColumn('nonAvoidableEd', when((col('allCauseIp') == 0) & (col('nonAvoidEdVisit') == 1), 1).otherwise(0))

# Show the result (optional)
final.show(250)

# COMMAND ----------

final.groupBy('avoidIp','nonAvoidIp', 'avoidableEd','nonAvoidableEd').count().show()

# COMMAND ----------

final1 = final.withColumn('non_avoid_ip', when(col('nonAvoidIp') == 1, 1).otherwise(0))
final1 = final1.withColumn('avoid_ip', when((col('avoidIp') == 1) & (col('nonAvoidIp') == 0), 1).otherwise(0))

# # Define the conditions and create new columns 'allCauseEd' and 'avoidableEd' only for non-IP visits
final1 = final1.withColumn('non_avoid_ed', when((col('allCauseIp') == 0) & (col('nonAvoidableEd') == 1), 1).otherwise(0)) 
final1 = final1.withColumn('avoid_ed', when((col('allCauseIp') == 0) & (col('nonAvoidableEd') == 0) & (col('avoidableEd') == 1), 1).otherwise(0)) 

# Show the result (optional)
final1.show(250)

final1.write.saveAsTable("dua_058828_spa240.paper2_acute_care_visits", mode='overwrite')

# COMMAND ----------

final1.groupBy('non_avoid_ip','avoid_ip', 'non_avoid_ed','avoid_ed').count().show()

# COMMAND ----------

sample = sample.select("beneID","state","fips_code")
#sample.show()

final_file = final1.join(sample, on=["beneID","state"], how="inner")
print(final_file.count())
print(final1.count())

# COMMAND ----------

final_file.show()

# COMMAND ----------

final_file.write.saveAsTable("dua_058828_spa240.paper2_acute_care_visit_episodes", mode='overwrite')