# Databricks notebook source
# MAGIC %md
# MAGIC Notebook to take the raw CSV data and move to a Delta table
# MAGIC
# MAGIC Destination table: `colibri_bronze.turbine_data_csv_import`

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import input_file_name, current_timestamp


# COMMAND ----------

# widget to accept the date for the folder path
dbutils.widgets.text('import_date', '', '1 - Import Date')


# COMMAND ----------

import_date = dbutils.widgets.get('import_date')


# COMMAND ----------

try:
    formatted_date = datetime.strptime(import_date, '%Y-%m-%d').strftime('%Y%m%d')
except ValueError:
    raise ValueError("The import_date cannot be cast to a date. Please provide a valid date in the format YYYY-MM-DD.")


# COMMAND ----------

import_path = f'abfss://landing@colibritechtestraw.dfs.core.windows.net/{formatted_date}'


# COMMAND ----------

import_schema = '''
  timestamp timestamp,
  turbine_id integer,
  wind_speed double,
  wind_direction integer,
  power_output double
'''


# COMMAND ----------

import_df = (
  spark.read
  .format('csv')
  .option('header', 'true')
  .schema(import_schema)
  .load(import_path)
)


# COMMAND ----------

enriched_df = (
  import_df
  .withColumnRenamed('timestamp', 'reading_timestamp')
  .withColumn('_import_filename', input_file_name())
  .withColumn('_import_timestamp', current_timestamp())
)


# COMMAND ----------

(
  enriched_df.write
  .mode('append')
  .saveAsTable('colibri_bronze.turbine_data_csv_import')
)
