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
# META         },
# META         {
# META           "id": "e423e6d1-ea68-49a2-911c-38fdf3b4e3db"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table silv_customers;
# MAGIC 
# MAGIC drop table silv_customer_feedback;

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table Lakehouse_GOLD.happy_first_names
