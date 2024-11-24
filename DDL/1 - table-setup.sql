-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Schemas (aka Database)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS colibri_bronze
LOCATION 'abfss://bronze@colibritechtestraw.dfs.core.windows.net/tables/';

CREATE SCHEMA IF NOT EXISTS colibri_silver
LOCATION 'abfss://silver@colibritechtestcleansed.dfs.core.windows.net/tables/';

CREATE SCHEMA IF NOT EXISTS colibri_gold
LOCATION 'abfss://gold@colibritechtestcleansed.dfs.core.windows.net/tables/';

-- COMMAND ----------

-- schema for the Views
CREATE SCHEMA IF NOT EXISTS colibri_gold_views
LOCATION 'abfss://gold@colibritechtestcleansed.dfs.core.windows.net/views/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze

-- COMMAND ----------

--DROP TABLE colibri_bronze.turbine_data_csv_import;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_bronze.turbine_data_csv_import
(
  reading_timestamp   TIMESTAMP,
  turbine_id          INT,
  wind_speed          DOUBLE,
  wind_direction      INT,
  power_output        DOUBLE,
  _import_filename    STRING,
  _import_timestamp   TIMESTAMP
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_silver.turbine_data_full
(
  reading_date      DATE NOT NULL,
  reading_hour      INT NOT NULL,
  turbine_id        INT NOT NULL,
  wind_speed        DOUBLE,
  wind_direction    INT,
  power_output      DOUBLE
);

-- IMPROVE: consider adding liquid clustering once the querying patterns are known
-- When moved to UC, also add a constraint on the PK (reading_date, reading_hour, turbine_id)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_silver.turbine_data_validated
(
  reading_date    DATE NOT NULL,
  reading_hour    INT NOT NULL,
  turbine_id      INT NOT NULL,
  wind_speed      DOUBLE,
  wind_direction  INT,
  power_output    DOUBLE
);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_silver.turbine_data_quarantine
(
  reading_date                  DATE,
  reading_hour                  INT,
  turbine_id                    INT,
  wind_speed                    DOUBLE,
  wind_direction                INT,
  power_output                  DOUBLE,
  _is_invalid_wind_speed        BOOLEAN,
  _is_invalid_wind_direction    BOOLEAN,
  _is_invalid_power_output      BOOLEAN,
  _validation_timestamp         TIMESTAMP
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS colibri_gold.turbine
(
  reading_date                 DATE NOT NULL,
  reading_hour                 INT NOT NULL,
  turbine_id                   INT NOT NULL,
  wind_speed                   DOUBLE,
  wind_direction               INT,
  power_output                 DOUBLE,
  is_imputed_wind_speed        BOOLEAN,
  is_imputed_wind_direction    BOOLEAN,
  is_imputed_power_output      BOOLEAN
);

