# Databricks notebook source
from pyspark.sql.functions import (
  sequence,
  explode,
  col,
  lit,
  count,
  mean,
  stddev,
  abs
)


# COMMAND ----------

bronze_turbine_data = (
  spark.read
  .table('colibri_bronze.turbine_data_csv_import')
)


# COMMAND ----------

turbine_data_hours = (
  bronze_turbine_data
  .selectExpr(
     'CAST(reading_timestamp AS DATE) AS reading_date',
     'HOUR(reading_timestamp) AS reading_hour',
     'turbine_id',
     'wind_speed',
     'wind_direction',
     'power_output'
  )
)


# COMMAND ----------

# MAGIC %md
# MAGIC Create a complete df of all turbine_ids, for all hours, for all dates.  
# MAGIC Use that df as the master list to reconcile the existing data against and identify missing data.  
# MAGIC Because it's a left-join any missing data will be null.
# MAGIC

# COMMAND ----------

turbine_min_max_date = (
  turbine_data_hours
  .selectExpr(
     'MIN(reading_date) AS min_date',
     'MAX(reading_date) AS max_date'
  )
)

min_max_dates = turbine_min_max_date.collect()[0]
min_date = min_max_dates['min_date']
max_date = min_max_dates['max_date']


# COMMAND ----------

all_dates = (
  spark.createDataFrame(
    [(min_date, max_date)],
    ['min_date', 'max_date']
  )
  .select(
    explode(
      sequence(col('min_date'), col('max_date'))
    ).alias('reading_date')
  )
)


# COMMAND ----------

all_hours = (
  spark.createDataFrame(
    [(0,23)],
    ['min_hour', 'max_hour']
  )
  .select(
    explode(
      sequence(col('min_hour'), col('max_hour'))
    ).alias('reading_hour')
  )
)


# COMMAND ----------

all_turbines = (
  spark.createDataFrame(
    [(1,15)],
    ['min_turbine', 'max_turbine']
  )
  .select(
    explode(
      sequence(col('min_turbine'), col('max_turbine'))
    ).alias('turbine_id')
  )
)


# COMMAND ----------

master_dates = (
  all_dates
  .crossJoin(all_hours)
  .crossJoin(all_turbines)
)


# COMMAND ----------

all_data = (
  master_dates
  .join(
    other = turbine_data_hours,
    on = ['reading_date', 'reading_hour', 'turbine_id'],
    how = 'left'
  )
)


# COMMAND ----------

(
  all_data.write
  .mode('append')
  .saveAsTable('colibri_silver.turbine_data_full')
)
