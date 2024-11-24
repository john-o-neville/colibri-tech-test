# Databricks notebook source
# MAGIC %md
# MAGIC ## Turbine data impute missing values
# MAGIC
# MAGIC There is not enough detail in the requirements to be precise here.  So this code is provided as a demonstration of what is possible, and to be the basis for future discussion with stakeholders to elicit more detailed requirements.
# MAGIC
# MAGIC The standard Imputer uses the mean of the existing values to replace missing values.  
# MAGIC (NB: for wind_direction this will nearly always result in 180 - so in a real situation it would be extra important to seek feedback from subject matter experts for a better way to impute missing values)

# COMMAND ----------

from pyspark.ml.feature import Imputer
from pyspark.sql.functions import col, floor
from delta.tables import DeltaTable

# COMMAND ----------

validated_df = spark.read.table('colibri_silver.turbine_data_validated')


# COMMAND ----------

# NB: the imputer only works with FloatType
floats_df = (
    validated_df
    .withColumn('wind_direction', validated_df.wind_direction.cast('float'))
)

# COMMAND ----------

pre_impute_df = (
    floats_df
    .withColumn('is_imputed_wind_speed', floats_df.wind_speed.isNull())
    .withColumn('is_imputed_wind_direction', floats_df.wind_direction.isNull())
    .withColumn('is_imputed_power_output', floats_df.power_output.isNull())
)

# COMMAND ----------

impute_cols = [
    'wind_speed',
    'wind_direction',
    'power_output'
]

imputer = Imputer(
    strategy='median',
    inputCols = impute_cols,
    outputCols = impute_cols
)

imputer_model = imputer.fit(pre_impute_df)

post_impute_df = imputer_model.transform(pre_impute_df)

# COMMAND ----------

casted_df = (
    post_impute_df
    .withColumn('wind_direction', floor(col('wind_direction')).cast('integer'))
)

# COMMAND ----------

gold_turbine = DeltaTable.forName(spark, 'colibri_gold.turbine')

(
    gold_turbine.alias('target')
    .merge(
        casted_df.alias('source'),
        '''target.reading_date = source.reading_date
        AND target.reading_hour = source.reading_hour
        AND target.turbine_id = source.turbine_id '''
    )
    .whenNotMatchedInsertAll()
    .execute()
)

