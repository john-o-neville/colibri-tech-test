# Databricks notebook source
# 1. dates should be in the expected date range: not future dates, and not more than 30 days old
# 2. turbine_id: 1-15
# 3. wind_speed: 0-100
# 4. wind_direction: 0-360
# 5. power_output: 0-1000

# COMMAND ----------

# add a column to the dataframe containing a json object with the results of the above
# if the column is null then that row passed


# COMMAND ----------

# MAGIC %md
# MAGIC `wind_speed` and `power_output` should be no more than 2 standard deviations from the mean

# COMMAND ----------

# Calculate standard deviation for the whole dataset
stddev_values = (
  turbine_data_hours
  .select(
    stddev(col('wind_speed')).alias('wind_speed_stddev'),
    stddev(col('power_output')).alias('power_output_stddev')
  )
).collect()[0]

wind_speed_stddev = stddev_values['wind_speed_stddev']
power_output_stddev = stddev_values['power_output_stddev']


# COMMAND ----------

# Calculate mean for the whole dataset
mean_values = (
  turbine_data_hours
  .select(
    mean(col('wind_speed')).alias('wind_speed_mean'),
    mean(col('power_output')).alias('power_output_mean')
  )
).collect()[0]

wind_speed_mean = mean_values['wind_speed_mean']
power_output_mean = mean_values['power_output_mean']

# COMMAND ----------

# Add columns to the dataframe
turbine_data_with_stddev = (
  turbine_data_hours
  .withColumn('wind_speed_stddev', lit(wind_speed_stddev))
  .withColumn('power_output_stddev', lit(power_output_stddev))
  .withColumn('wind_speed_deviation', abs(col('wind_speed') - wind_speed_mean))
  .withColumn('power_output_deviation', abs(col('power_output') - power_output_mean))
  .withColumn('is_wind_speed_outlier', col('wind_speed_deviation') > (2 * wind_speed_stddev))
  .withColumn('is_power_output_outlier', col('power_output_deviation') > (2 * power_output_stddev))
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For reference: here is a SQL-only version for if this test is to be done by analysts in the Gold layer
# MAGIC
# MAGIC ```
# MAGIC WITH Stats AS (
# MAGIC     SELECT 
# MAGIC         AVG(column_name) AS mean,
# MAGIC         STDEV(column_name) AS stddev
# MAGIC     FROM 
# MAGIC         table_name
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC         WHEN column_name > mean + 2 * stddev 
# MAGIC           OR column_name < mean - 2 * stddev 
# MAGIC           THEN 'True'
# MAGIC         ELSE False'
# MAGIC     END AS is_outlier
# MAGIC FROM 
# MAGIC     table_name, Stats;
# MAGIC ```
# MAGIC
