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
# META           "id": "e423e6d1-ea68-49a2-911c-38fdf3b4e3db"
# META         },
# META         {
# META           "id": "19b7b582-c117-458b-b16f-47ee9bdefe4c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# select top 10 average happiest first names

query = """
select
    c.first_name, 
    avg(f.happiness) avg_happiness 
from 
    Lakehouse_SILVER.silv_customers c inner join Lakehouse_SILVER.silv_customer_feedback f 
    on c.customer_id = f.customer_id
group by 1
order by 2 desc
limit 10
"""

# CELL ********************

df = spark.sql(query)
df.write.mode("overwrite").saveAsTable("Lakehouse_GOLD.happy_first_names")

# CELL ********************

