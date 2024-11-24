-- Databricks notebook source
-- MAGIC %md
-- MAGIC Abstraction layer for the Gold tables.  End clients (e.g. PowerBI) will connect to these rather than to the tables directly.  This protects users from underlying schema changes.

-- COMMAND ----------

CREATE OR REPLACE VIEW colibri_gold_views.turbine
AS
SELECT
  t.reading_date
  ,t.reading_hour
  ,t.turbine_id
  ,t.wind_speed
  ,t.wind_direction
  ,t.power_output
FROM
  colibri_gold.turbine AS t;
