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
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# CELL ********************

folder_path = 'Files/raw/REST/customerFeedback'

sink_table = 'silv_customers'

# CELL ********************

schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("_file_modification_time", TimestampType(), True)
])

if not spark.catalog.tableExists(sink_table):
    print(f"Creating empty table {sink_table}")    
    df = spark.createDataFrame([], schema)
    df.write.saveAsTable(sink_table)

# CELL ********************

df = spark.read.format("parquet").load(folder_path, schema=schema).withColumn("_file_modification_time", col("_metadata.file_modification_time"))
display(df)

# CELL ********************

# Here we perform SCD-1

# Define the window spec to partition by 'customer_id' and order by '_file_modification_time' descending
windowSpec = Window.partitionBy("customer_id").orderBy(col("_file_modification_time").desc())

# Use the row_number function to add a 'row_number' column
# which assigns a unique sequential number starting from 1 for each 'id' group
df = df.withColumn("row_number", row_number().over(windowSpec))

# Filter the rows where 'row_number' is 1, this keeps the latest entry for each 'id'
deduplicated_df = df.filter(col("row_number") == 1).drop("row_number")
display(deduplicated_df)

# CELL ********************

print(f"writing {deduplicated_df.count()} rows to {sink_table}")
deduplicated_df.write.mode("overwrite").saveAsTable(sink_table)

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from silv_customers

# CELL ********************

