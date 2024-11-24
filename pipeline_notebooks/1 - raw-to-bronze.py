# Databricks notebook source
# MAGIC %md
# MAGIC Notebook to take the raw CSV data and move to a Delta table
# MAGIC
# MAGIC Destination table: `colibri_bronze.turbine_data_csv_import`

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import input_file_name, current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text('import_date', '', '1 - Import Date')
dbutils.widgets.text('import_container', '', '2 - Import Path')

# COMMAND ----------

import_date = dbutils.widgets.get('import_date')
import_container = dbutils.widgets.get('import_container')

# COMMAND ----------

try:
    formatted_date = datetime.strptime(import_date, '%Y-%m-%d').strftime('%Y%m%d')
except ValueError:
    raise ValueError("The import_date cannot be cast to a date. Please provide a valid date in the format YYYY-MM-DD.")


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
  .load(f'{import_container}/{formatted_date}')
)


# COMMAND ----------

enriched_df = (
  import_df
  .withColumnRenamed('timestamp', 'reading_timestamp')
  .withColumn('_import_filename', input_file_name())
  .withColumn('_import_timestamp', current_timestamp())
  .withColumn('_is_moved_to_silver', lit(False))
)


# COMMAND ----------

(
  enriched_df.write
  .mode('append')
  .saveAsTable('colibri_bronze.turbine_data_csv_import')
)
