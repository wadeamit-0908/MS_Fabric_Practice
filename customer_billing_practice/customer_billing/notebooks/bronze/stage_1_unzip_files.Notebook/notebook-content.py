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

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folder_name = datetime.now().strftime("%Y%m%d")
run_start_time = datetime.now()
run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime("%Y%m%d_%H%m%s")}"
file_name = "client_input_20261106_073512.zip"
# print("run_id => ", run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_input_folder = input_folder + file_name
# print("final_input_folder => ", final_input_folder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calling unzip function
try:
    process_log("Start",file_name=file_name)
    validation_message = unzip_files(final_input_folder, unzip_folder)
    if "Error:" in validation_message:
        raise Exception(validation_message)
    else:
        print("Validation message after execution => ", validation_message)
    process_log("Success",file_name=file_name) 
except Exception as e:
    process_log("Failed",file_name=file_name, error_message = f"{validation_message}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
