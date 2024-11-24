# Databricks notebook source
# MAGIC %md
# MAGIC ## Turbine data validation
# MAGIC
# MAGIC Check that:
# MAGIC - wind_speed is between 0-100
# MAGIC - wind_direction is between 0-360
# MAGIC - power_output is > 0
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

silver_df = spark.read.table('colibri_silver.turbine_data_full')

# COMMAND ----------

valid_cols = silver_df.columns

# COMMAND ----------

validated_df = (
    silver_df
    .withColumn(
        '_is_invalid_wind_speed',
        (col('wind_speed') < 0)
        | (col('wind_speed') > 100)
    )
    .withColumn(
        '_is_invalid_wind_direction',
        (col('wind_direction') < 0)
        | (col('wind_direction') > 360)
    )
    .withColumn(
        '_is_invalid_power_output',
        col('power_output') < 0
    )
)

# COMMAND ----------

# TODO: identify any columns with values outside of the 2nd percentile

# COMMAND ----------

# MAGIC %md
# MAGIC Write rows that pass all tests to the _validated table.  
# MAGIC Write rows that don't pass to the _quarantine table for further investigation.

# COMMAND ----------

# passed
(
    validated_df
    .filter(
        (~col('_is_invalid_wind_speed'))
        & (~col('_is_invalid_wind_direction'))
        & (~col('_is_invalid_power_output'))
    )
    .select(*valid_cols)
)

# COMMAND ----------

silver_valid = DeltaTable.forName(spark, 'colibri_silver.turbine_data_validated')

(
    silver_valid.alias('target')
    .merge(
        validated_df.alias('source'),
        '''target.reading_date = source.reading_date
        AND target.reading_hour = source.reading_hour
        AND target.turbine_id = source.turbine_id '''
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# failed
(
    validated_df
    .filter(
        (col('_is_invalid_wind_speed'))
        | (col('_is_invalid_wind_direction'))
        | (col('_is_invalid_power_output'))
    )
    .withColumn('_validation_timestamp', current_timestamp())
    .write
    .mode('append')
    .saveAsTable('colibri_silver.turbine_data_quarantine')
)
