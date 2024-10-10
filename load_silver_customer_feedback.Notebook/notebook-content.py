# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19b7b582-c117-458b-b16f-47ee9bdefe4c",
# META       "default_lakehouse_name": "Lakehouse_SILVER",
# META       "default_lakehouse_workspace_id": "5b0eaee0-12a4-4652-93b8-0f08395fb823",
# META       "known_lakehouses": [
# META         {
# META           "id": "19b7b582-c117-458b-b16f-47ee9bdefe4c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark.sql.functions import col

# CELL ********************

folder_path = 'Files/raw/REST/customerFeedback'

sink_table = 'silv_customer_feedback'

# CELL ********************

schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("happiness", LongType(), True),
    StructField("_file_modification_time", TimestampType(), True)
])

if not spark.catalog.tableExists(sink_table):
    print(f"Creating empty table {sink_table}")    
    df = spark.createDataFrame([], schema)
    df.write.saveAsTable(sink_table)

# CELL ********************

query = f"""
select 
    max(_file_modification_time) as maxval
from {sink_table}
"""

max_file_modification_ts = spark.sql(query).collect()[0]['maxval']

print(f"max _file_modification_time in {sink_table}: {max_file_modification_ts}")

# CELL ********************

if max_file_modification_ts:
    df = spark.read.format("parquet").load(folder_path, schema=schema, modifiedAfter=max_file_modification_ts)
else: 
    df = spark.read.format("parquet").load(folder_path, schema=schema)

print(f"appending {df.count()} rows to {sink_table}")
df.select("customer_id", "happiness") \
    .withColumn("_file_modification_time", col("_metadata.file_modification_time")) \
    .write.mode("append").saveAsTable(sink_table)

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silv_customer_feedback

# CELL ********************

