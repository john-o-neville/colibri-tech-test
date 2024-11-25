-- Databricks notebook source
-- MAGIC %md
-- MAGIC Highlight power_output rows that are more than 2 standard deviations from the mean.
-- MAGIC
-- MAGIC NB: this query can be added to a dashboard to allow users to identify potential outliers.

-- COMMAND ----------


WITH stats AS (
  SELECT
    AVG(t.power_output) AS mean_power_output,
    STDDEV(t.power_output) AS stddev_power_output
  FROM
    colibri_gold_views.turbine AS t
)
SELECT
  *,
  CASE 
    WHEN t2.power_output > stats.mean_power_output + (2 * stats.stddev_power_output)
        OR t2.power_output < stats.mean_power_output - (2 * stats.stddev_power_output)
      THEN 'True'
    ELSE 'False'
  END AS is_power_output_outlier
FROM 
     colibri_gold_views.turbine AS t2,
     stats;
