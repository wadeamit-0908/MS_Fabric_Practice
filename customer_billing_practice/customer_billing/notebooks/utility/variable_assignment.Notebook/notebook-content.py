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

# **This notebook we use to create a variable from dev.config**

# CELL ********************

#import libraries
import json
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Declaring varibales
file_location = "/lakehouse/default/Files/config/dev_config.json"
folder_name = datetime.now().strftime("%Y%m%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read file and assign data to variables
with open(file_location) as var_file:
    config_data = json.load(var_file)
env = config_data["env"]
input_folder = config_data["input_folder"]
unzip_folder = config_data["unzip_folder"]
excepted_files = config_data["excepted_files"]
bronze_yaml_path = config_data["bronze_yaml_path"]
silver_yaml_path = config_data["silver_yaml_path"]
gold_yaml_path = config_data["gold_yaml_path"]
bronze_all_files = config_data["bronze_all_files"]
silver_all_files = config_data["silver_all_files"]
gold_all_files = config_data["gold_all_files"]
dummy_date = config_data["dummy_date"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
