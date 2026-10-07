# Databricks notebook source
# DBTITLE 1,import required for library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *



# COMMAND ----------

# DBTITLE 1,Catalog Schema & Vol
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path='/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,Calling APi
API_KEY = '1d6486d2-9565-4e5e-a66b-d2b2a6d75277'

api_url = f'https://api.cricapi.com/v1/countries?apikey=1d6486d2-9565-4e5e-a66b-d2b2a6d75277&offset=0'

response = requests.get(api_url)

response.raise_for_status()

api_data=response.json()
print(api_data.keys())
print(json.dumps(api_data,indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,Save Raw API response in Volume
raw_file_path = f'{base_path}/current_matches.json'

with open(raw_file_path,'w') as file:
    json.dump(api_data,file)

    print("Raw API saved:",raw_file_path)

# COMMAND ----------

# DBTITLE 1,Create Bronze layer table DF
bronze_data = [{
             "source_api":api_url,
             "raw_json":json.dumps(api_data),
             "ingestion_time":None

}]

bronze_schema=StructType([
    StructField("source-api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("ingestion_time",TimestampType(),True),
])

bronze_df = spark.createDataFrame(bronze_data,bronze_schema)\
            .withColumn("IngestionTime",current_timestamp())

display(bronze_df)


# COMMAND ----------

bronze_data = [{
             "source_api":api_url,
             "raw_json":json.dumps(api_data),
           
             }]

# COMMAND ----------

bronze_df = spark.createDataFrame(bronze_data)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

bronze_df = bronze_df.withColumn(
    "ingestion_time",
    current_timestamp()
)

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save Bronze Table
bronze_df.write\
     .format('delta')\
     .mode("overwrite")\
     .saveAsTable("workspace_default_current_matches") 

print("Bronze Table Created Successfully")   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  workspace_default_current_matches

# COMMAND ----------

# DBTITLE 1,issue thats y recreating
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.default.current_matches;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable(" workspace.default.current_matches")

print("Bronze Successfully Created")    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.current_matches

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;