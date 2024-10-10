# Fabric notebook source


# MARKDOWN ********************

# # Task: Historization in Fabric
# 
# In this task we are dealing with mock data about employees. Every employee is characterized by two fields: 
# - id (integer)
# - name (string)
# 
# The non-historized source data will look like this:
# 
# | id         | name     | 
# |--------------|-----------|
# | 1 | Eloise |
# | 2 | Penelope | 
# 
# **Task**: Use Pyspark to define a dataframe `staging_df`, that contains the above fields and records

# CELL ********************

# Execute code here


# MARKDOWN ********************

# Next, we want to create a table of historized employee records with additional metadata fields:
# - id (integer)
# - name (string)
# - _version (integer)
# - _is_active (boolean)
# 
# **Task**: Create an empty delta table (save it as a table in a Lakehouse) with the above column structure. Call it `employee_hist_{your_name}`.

# CELL ********************

# Execute code here

# MARKDOWN ********************

# Now we want to insert the records in `staging_df` into the created table, such that it looks like this:
# 
# | id | name | _version | _is_active |
# |--|--|--|--|
# |1|Eloise|1|True|
# |2|Penelope|1|True|
# 
# **Task**: Insert records into the historized table, such that it looks like this

# CELL ********************

# Execute code here

# MARKDOWN ********************

# We will now simulate a new batch of source data. Our aim is for `staging_df` to look like this: 
# 
# | id         | name     | 
# |--------------|-----------|
# | 1 | Eloise |
# | 2 | Colin | 
# |3 |Cressida|
# 
# **Task**: Redefine `staging_df` to have the mentioned content. 

# CELL ********************

# Execute code here

# MARKDOWN ********************

# We now want to update the contents of `employee_hist` with the content from `staging_df`, such that the old values are historized. The resulting contents of `employee_hist` should look like this:
# 
# | id | name | _version | _is_active |
# |--|--|--|--|
# |1|Eloise|1|True|
# |2|Penelope|1|False|
# |2|Colin|2|True|
# |3|Cressida|1|True|
# 
# **Task**: Perform the required updates and inserts to `employee_hist`. *Hint*: To perform a merge on tables it can be beneficial to look at the `delta.tables` library, i.e. `from delta.tables import DeltaTable`

# CELL ********************

# Execute code here
