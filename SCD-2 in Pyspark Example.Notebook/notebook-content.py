# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "523fc7c7-2e87-4485-93fe-8b43d47916eb",
# META       "default_lakehouse_name": "Spark_Demo_Lakehouse",
# META       "default_lakehouse_workspace_id": "5b0eaee0-12a4-4652-93b8-0f08395fb823"
# META     },
# META     "environment": {
# META       "environmentId": "1588d72a-2ba2-4276-bbd8-9d55cad9f0e0",
# META       "workspaceId": "5b0eaee0-12a4-4652-93b8-0f08395fb823"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType
from pyspark.sql.functions import lit, col, coalesce

demo_data = [
    (1, "Stefan"),
    (2, "Markus")
]

schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), False)
])

staging_df = spark.createDataFrame(demo_data, schema=schema)

# CELL ********************

staging_df.show()

# CELL ********************

scd_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('_version', IntegerType(), False),
    StructField('_is_active', BooleanType(), False),
])

scd_df = spark.createDataFrame([], schema=scd_schema)

# CELL ********************

scd_df.write.mode("overwrite").saveAsTable('scd_table')

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from scd_table

# CELL ********************

staging_df\
.withColumn('_version', lit(1))\
.withColumn('_is_active', lit(True))\
.write.mode("append").saveAsTable('scd_table')

# CELL ********************

from delta.tables import DeltaTable

scd_table = DeltaTable.forName(spark, tableOrViewName='scd_table')

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from scd_table

# CELL ********************

demo_data = [
    (1, "Stefan"),
    (2, "Hans"),
    (3, "Birgit")
]

schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), False)
])

staging_df = spark.createDataFrame(demo_data, schema=schema)

# CELL ********************

staging_df.show()

# CELL ********************

update_df = staging_df.select(col('id'), col('name').alias('new_name'))\
    .join(scd_table.toDF().where(col('_is_active')), on="id", how='left')\
    .where((col('new_name') != col('name')) | col('name').isNull())\
    .withColumn('new_version', coalesce(col('_version') + 1, lit(1)))\
    .withColumn('new_is_active', lit(True))
    
update_df.show()

# CELL ********************


# CELL ********************

from pyspark.sql.functions import col, lit

# insert new and updated records

update_df.select('id', col('new_name').alias('name'), col('new_version').alias('_version'), col('new_is_active').alias('_is_active')).write.mode('append').saveAsTable('scd_table')


# deactivate old entries

scd_table.alias('scd_table').merge(
    source=update_df.alias('update'),
    condition='scd_table.id = update.id and scd_table.name != update.new_name and scd_table._is_active'
).whenMatchedUpdate(set = {
    '_is_active': "False",
}).execute()


# CELL ********************

# MAGIC %%sql
# MAGIC select * from scd_table
