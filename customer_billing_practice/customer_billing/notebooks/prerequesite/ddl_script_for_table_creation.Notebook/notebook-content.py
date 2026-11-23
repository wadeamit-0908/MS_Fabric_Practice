# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03e256ce-fa0b-4d58-8366-7c93f46ed4a6",
# META       "default_lakehouse_name": "lk_customer_billing",
# META       "default_lakehouse_workspace_id": "d08afa54-65d9-4c57-8ae5-f93fc706e510",
# META       "known_lakehouses": [
# META         {
# META           "id": "03e256ce-fa0b-4d58-8366-7c93f46ed4a6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Table creation notebook and it will be used as commonly.**

# CELL ********************

import yaml

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# yaml_path = "/lakehouse/default/Files/config/yaml/control/etl_run_log.yaml"
yaml_path = yaml_base_path+yaml_file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with open(yaml_path, 'r') as yaml_files:
    yaml_config = yaml.safe_load(yaml_files)
    # print(yaml_config)
    table_name = yaml_config["table"]["name"]
    table_schema = yaml_config["table"]["schema_name"]
    table_columns = yaml_config["table"]["columns"]
    table_column_name = ",\n".join([f'{table_column["name"]} {table_column["data_type"]}' for table_column in table_columns])
    # print(table_column_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")

#Prepare CREATE TABLE statement
create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} ({table_column_name}) USING DELTA"""

#Prepare complete table name
full_table_name = f"{table_schema}.{table_name}"

# Check whether table already exists
if spark.catalog.tableExists(full_table_name):
    print(f"Table already exists: {full_table_name}")
else:
    # Create table only when it does not exist
    spark.sql(create_table_sql)
    print(f"Table created successfully: {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
