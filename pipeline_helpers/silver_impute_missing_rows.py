# Databricks notebook source
# read the data in silver_2

# COMMAND ----------

# use ML imputer to add missing data

# COMMAND ----------

# use an imputer to fill in the blanks
# NB: both 'turbine_date_hours' and 'missing_dates' have row counts of 11,160.... so are there any blanks?

# mock-up a file with missing data in order to test using an imputer


# COMMAND ----------

# NB: there doesn't seem to be any missing data? :-?  Was this a trick question?
# Get a distinct count by date, hour, and turbine to double check

turbine_dup_check = (
  turbine_data_hours
  .select(
    'reading_date',
    'reading_hour',
    'turbine_id'
  )
  .groupBy(
    'reading_date',
    'reading_hour',
    'turbine_id'
  )
  .agg(
    count('*').alias('count_rows')
  )
  # .where('count_rows > 1')
)

