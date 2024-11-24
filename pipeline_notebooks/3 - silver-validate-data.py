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

from pyspark.sql.functions import col

# COMMAND ----------

silver_df = spark.read.table('colibri_silver.turbine_data_full')

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

# valid data
(
    validated_df
    .filter(
        (~col('_is_invalid_wind_speed'))
        & (~col('_is_invalid_wind_direction'))
        & (~col('_is_invalid_power_output'))
    )
    .write
    .mode('append')
    .saveAsTable('colibri_silver.turbine_data_validated')
)

# COMMAND ----------

# quarantine data
(
    validated_df
    .filter(
        (col('_is_invalid_wind_speed'))
        | (col('_is_invalid_wind_direction'))
        | (col('_is_invalid_power_output'))
    )
    .write
    .mode('append')
    .saveAsTable('colibri_silver.turbine_data_quarantine')
)
