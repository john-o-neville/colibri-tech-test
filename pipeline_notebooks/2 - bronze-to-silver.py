# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Bronze to Silver
# MAGIC
# MAGIC - denormalize timestamp to get date + hour
# MAGIC - de-duplicate based on date + hour (take the latest reading)
# MAGIC - ensure all dates + hours + turbines are present

# COMMAND ----------

from pyspark.sql.functions import sequence, explode, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

bronze_turbine_data = DeltaTable.forName(spark, 'colibri_bronze.turbine_data_csv_import')

# COMMAND ----------

turbine_data_hours = (
  bronze_turbine_data.toDF()
  .filter('_is_moved_to_silver = False')
  .selectExpr(
    'reading_timestamp',
    'CAST(reading_timestamp AS DATE) AS reading_date',
    'HOUR(reading_timestamp) AS reading_hour',
    'turbine_id',
    'wind_speed',
    'wind_direction',
    'power_output'
  )
)


# COMMAND ----------

turbine_data_hours.count()

# COMMAND ----------

# de-duplicate the Bronze data
window_spec = (
  Window
  .partitionBy('turbine_id', 'reading_date', 'reading_hour')
  .orderBy(col('reading_timestamp').desc())
)

turbine_dedup = (
  turbine_data_hours
  .withColumn('row_num', row_number().over(window_spec))
  .filter('row_num = 1')
  .drop('row_num')
)

# COMMAND ----------

turbine_dedup.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a complete df of all turbine_ids, for all hours, for all dates.  
# MAGIC Use that df as the master list to reconcile the existing data against and identify missing data.  
# MAGIC Because it's a left-join any missing data will be null.
# MAGIC

# COMMAND ----------

turbine_min_max_date = (
  turbine_dedup
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

# MAGIC %md
# MAGIC Merge the de-duplicate and complete data into the Silver table.  
# MAGIC Mark the Bronze records as 'done'

# COMMAND ----------

dest_table = DeltaTable.forName(spark, 'colibri_silver.turbine_data_full')

(
  dest_table.alias('target')
  .merge(
    all_data.alias('source'),
    '''target.reading_date = source.reading_date
    AND target.reading_hour = source.reading_hour
    AND target.turbine_id = source.turbine_id '''
  )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# COMMAND ----------

bronze_turbine_data.update(
  condition = '_is_moved_to_silver = False',
  set = {'_is_moved_to_silver': 'true'}
)
