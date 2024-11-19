-- Databricks notebook source
-- MAGIC %md
-- MAGIC Schemas (aka Database)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS colibri_bronze
LOCATION 'abfss://bronze@colibritechtestraw.dfs.core.windows.net/tables/';

CREATE SCHEMA IF NOT EXISTS colibri_silver
LOCATION 'abfss://silver@colibritechtestcleansed.dfs.core.windows.net/tables/';

CREATE SCHEMA IF NOT EXISTS colibri_gold
LOCATION 'abfss://gold@colibritechtestcleansed.dfs.core.windows.net/tables/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bronze

-- COMMAND ----------

DROP TABLE colibri_bronze.turbine_data_csv_import;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_bronze.turbine_data_csv_import
(
  reading_timestamp TIMESTAMP,
  turbine_id INT,
  wind_speed DOUBLE,
  wind_direction INT,
  power_output DOUBLE,
  _import_filename STRING,
  _import_timestamp TIMESTAMP
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver
