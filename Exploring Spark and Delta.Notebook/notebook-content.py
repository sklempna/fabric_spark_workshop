# Fabric notebook source


# MARKDOWN ********************

# # Looking under the hood
# 
# Spark splits up the work you give it into Jobs, Stages and Tasks.
# 
# As a rule of thumb: 
# 
# - Jobs = Operations on Dataframes
# - Stages = Phases of a Job that have to run sequentially 
# - Tasks = Constituents of a Stage that can be run in parallel
# 
# Spark follows the **lazy execution** paradigm, i.e. only compute what is necessary to perform the job.
# 
# ![Spark Execution](https://www.mdpi.com/BDCC/BDCC-05-00046/article_deploy/html/images/BDCC-05-00046-g001.png)
# 
# We can use the `explain()` function of Spark to look at the execution plan of dataframe operations.

# CELL ********************

# Here is a nice little helper-library written by Gerhard Brueckl that helps visualize the Spark execution plan
# Let's import it

sc.addPyFile("https://raw.githubusercontent.com/gbrueckl/Fabric.Toolbox/main/DataEngineering/Library/VisualizeExecutionPlan.py")
from VisualizeExecutionPlan import show_plan


# MARKDOWN ********************

# # Lazy evaluation and execution plan
# 
# **Task**: run `df = spark.read.csv(csv_path, header=True)` and observe how much data is read and how many rows are processed
# 
# **Task**: run `df.explain()` to see the execution plan of the query
# 
# **Task**: run `show_plan(df)` to see the graph visualization of the execution plan
# 
# **Task**: run `spark.read.csv(csv_path, header=True).limit(10)` and see what is meant by lazy evaluation
# 
# **Task**: run `spark.read.csv(csv_path, header=True).limit(10).collect()` and see that something happens, i.e. the rows are transferred to the driver
# 
# **Task**: Observe the execution plan of a join `df.join(df, on='Store_Key', how='inner').explain()`
# 
# **Task**: See the graphical representation of the execution plan `show_plan(df.join(df, on='Store_Key', how='inner'))`


# CELL ********************

# Execute code here

# MARKDOWN ********************

# # Exploring Delta
# 
# Delta is a format built around Apache Spark that brings transactions and CRUD operations to parquet files. Let's look under the hood of delta tables to gain a better understanding.

# CELL ********************

# Let's create some dummy data

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

data = [
    (1, "Stefan")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)

# MARKDOWN ********************

# **Task**: Write the dataframe to a parquet table using `df.write.format("parquet").saveAsTable("test_parquet")`
# 
# **Task**: View the data in the table by executing (in a sql cell) `select * from test_parquet`
# 
# **Task**: Let's have a look, what files spark created. Execute `!file /lakehouse/default/Tables/test_parquet/*`
# 
# **Task**: Let's inspect the contents of the created files with `cat`
# 
# **Task**: Let's add some data to the table. In a sql-cell execute `insert into test_parquet values (2, "Guido")`
# 
# **Task**: Get the contents of the table again via `select * from test_parquet`
# 
# **Task**: Look at the directory contents again `!ls -l /lakehouse/default/Tables/test_parquet`
# 
# **Task**: Let's try to update the table via `update test_parquet set name='Anna' where id=2`
# 
# **Task**: Let's create a delta table from the dataframe `df.write.saveAsTable("test_delta")`
# 
# **Task**: Check the contents of the table again `select * from test_delta`
# 
# **Task**: Let's look at the created folder `!ls -l /lakehouse/default/Tables/test_delta`
# 
# **Task**: Let's look at the delta log `!cat /lakehouse/default/Tables/test_delta/_delta_log/00000000000000000000.json`
# 
# **Task**: Insert data into the table `insert into test_delta values (2, 'Guido')`
# 
# **Task**: Let's look at the delta log again `!cat /lakehouse/default/Tables/test_delta/_delta_log/00000000000000000001.json`
# 
# **Task**: Update the table `update test_delta set name='Anna' where id=2`
# 
# **Task**: Check the delta log again `!cat /lakehouse/default/Tables/test_delta/_delta_log/00000000000000000002.json`
# 
# **Task**: Let's look at the history of the table `describe history test_delta`
# 
# **Task**: Time-travel to a previous version `select * from test_delta version as of 1`
# 
# **Task**: Disable retention safeguard `set spark.databricks.delta.retentionDurationCheck.enabled=false`
# 
# **Task**: Vacuum the table `vacuum test_delta retain 0 hours`
# 
# **Task**: Try time-travelling again to version 1 `select * from test_delta version as of 1`
# 
# 
# 
# 
# 


# CELL ********************

# Execute code here
