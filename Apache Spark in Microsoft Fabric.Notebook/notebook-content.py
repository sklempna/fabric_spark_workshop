# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "523fc7c7-2e87-4485-93fe-8b43d47916eb",
# META       "default_lakehouse_name": "Spark_Demo_Lakehouse",
# META       "default_lakehouse_workspace_id": "5b0eaee0-12a4-4652-93b8-0f08395fb823"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Spark Architecture
# 
# ![Spark Architecture](https://i0.wp.com/0x0fff.com/wp-content/uploads/2015/03/Spark-Architecture-Official.png)
# 
# - In-memory parallel processing engine
# - Fabric manages all the necessary infrastructure for us
# - In Fabric you interact with a spark session (unified entrypoint to spark since v2.0)
# 
# ### Spark components
# - Driver: Responsible for breaking a spark program up into tasks and handing them over to the executors
# - Cluster manager: Responsible for management of Cluster Resources
# - Worker Nodes: Responsible for executing tasks. Store data in-memory (re-usability)


# MARKDOWN ********************

# # Creating dataframes
# 
# - most of the data interaction we will do is based on the Spark dataframe API

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    ("Randy", "Minder", "1", "M", 10000),
    ("Lauren", "Minder", "2", "F", 20000),
    ("Sarah", "Minder", "3", "F", 50000),
    ("Siegfried", "Minder", "4", "M", 30000),
]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True),
])

df = spark.createDataFrame(data=data, schema=schema)

# MARKDOWN ********************

# **Task**: Execute `df.printSchema()` to see the schema of the dataframe
# 
# **Task**: execute `df.show(truncate=False)` and `display(df)` to peek at the dataframe
# 
# **Task**: Look at the help for the two functions with `help(df.show)` and `help(display)`
# 
# **Task**: Try setting the salary of Randy Minder to `None` and setting nullability of the salary column to `False`
# 
# **Task**: Execute `type(df)` to see the type of every object 
# 
# **Task**: Execute `df.columns` to see the columns in the dataframe 
# 
# # Exploring the local filesystem
# 
# We want to read example data I previously uploaded to the Lakehouse 
# 
# **Task**: Connect the Notebook to the Lakehouse Workshop_LH and find the file located under Files and look at the different filepath options
# 
# **Task**: Let's peek at the file. With python we would usually do something like `f = open(filepath, "r")`. Let's try this.
# 
# **Task**: This doesn't work. Let's use the Linux CLI. We can use command-line magic `!` to execute shell commands on the driver node
# 
# **Task**: Try `!uname` to see what os we're running
# 
# **Task**: Try `!hostenamectl` to get more information
# 
# **Task**: Explore the local filesystem with `!ls` and `!ls /`
# 
# **Task**: After we've found the file, try peeking into the file with `!head /lakehouse/default/Files/OnlineSales.csv`
# 
# **Task**: Let's determine if we're actually dealing with a local file with `!df /lakehouse/default/Files/OnlineSales.csv`
# 
# # Reading a file into a dataframe
# 
# We want to read example data from an Azure storage via Shortcut
# 
# **Task**: Create a storage account in the Azure Portal with LRS, public access and hierarchical namespace enabled.
# 
# **Task**: Create a Lakehouse `Spark_Demo_Lakehouse` and there create a shortcut to the storage container.
# 
# **Task**: Now try again with the `Storage Blob Data Contributor` Rights.
# 


# CELL ********************

csv_path = "Files/example-data/OnlineSales.csv"

df = spark.read.csv(csv_path, header=True)

# MARKDOWN ********************

# **Task**: display the dataframe `display(df)`
# 
# **Task**: Run `df.dtypes` to see the column types
# 
# **Task**: Run the read command again with the paramenter `inferSchema=True`. Check the dtypes again
# 
# **Task**: Manually change the dtype of the column Customer_Key back to string with `df.withColumn('Customer_Key', col('Customer_Key').astype('string'))`

# CELL ********************

# Execute the tasks here

# MARKDOWN ********************

# 
# 
# # Manipulating dataframes in Pyspark
# 
# **Task**: Create a StoreID by concatenating the Store_Key column with the prefix 'ID_' via `df = df.withColumn("StoreID", concat(lit('ID_'), col("Store_Key").astype("string")))`
# 
# **Task**: Compute the Earnings per Row via `df = df.withColumn('Earnings',  col('Unit_Price') - col('Unit_Cost'))`
# 
# **Task**: Find out how many unique stores there are in the dataset by inspecting `display(df, summary=True)`
# 
# **Task**: Compute the average earnings per store via `df.groupBy(col('StoreID')).avg('Earnings').sort('avg(Earnings)', ascending=False).show()`
# 
# 
# 
# 


# CELL ********************

# Execute the tasks here

# MARKDOWN ********************

# # Manipulating dataframes in Spark SQL
# 
# We can also query tables and dataframes in Fabric with sql
# 
# **Task**: Create a temporary view from the dataframe via `df.createOrReplaceTempView("df_view")`
# 
# **Task**: We want to calculate the number of unique customers that visited every store. Define a sql query string via
# 
#     `sql_query = "select StoreID, count( distinct Customer_Key) as distinct_customers from df_view group by StoreID"`
# 
# Then create a new dataframe df2 via `df2 = spark.sql(sql_query)` and run `display(df2)`
# 
# **Task**: For the ultimate sql comfort, do this again in a sql cell :)
# 
# - You can find the spark sql function documentation [here](https://spark.apache.org/docs/latest/api/sql/index.html)
# - You can finde the Pyspark function documentation [here](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

# CELL ********************

# Execute the tasks here

# MARKDOWN ********************

# # Writing to files and tables
# 
# Usually we will persist the transformed dataframes as Files or Delta tables
# 
# **Task**: Write df2 as json File to the Lakehouse Files via `df2.write.json('Files/df2', mode='overwrite')`
# 
# **Task**: Write df2 as json File to the Azure Storage via `df2.write.json('Files/example-data/df2', mode='overwrite')`. Look at the Azure storage and preview the file.
# 
# **Task**: Write df2 as managed Spark table to the Lakehouse via `df2.write.saveAsTable('df2', mode="overwrite")`
# 
# **Task**: Write df2 as Spark Delta Files to the Azure Storage via `df2.write.save('Files/example-data/deltadf2', mode="overwrite")`
# 
# **Task**: Write df2 as Spark unmanaged Delta Table to the Azure Storage via `df2.write.mode("overwrite").option("path", "Files/example-data/deltadf2").saveAsTable("unmanaged_table")`
# 
# 
# 


# CELL ********************

# Execute the tasks here

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
# 
# 
# 


# MARKDOWN ********************

# # Exploring Delta
# 
# TODO:
# 
# - create a dataframe with a small amount of demo data
# - write the dataframe as a parquet table into azure storage
# - select some rows from it
# - try to update -> see that it fails
# - look at the written data in azure storage
# - write the same dataframe as format delta as into azure storage
# - look at the written data
# - try to update -> see that it works
# - select from the data again
# - look at the written data (log and parquet files)
# - look at the history of the table
# - time-travel back in history
# - select the data again
# - look at the files in azure storage
# - vacuum the table
# - select again 
# - look at the files again

# CELL ********************

