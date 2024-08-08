# Databricks notebook source
# Drop the existing table if it exists 
# spark.sql("DROP TABLE IF EXISTS f1_presentation.driver_standings")

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race year for which the data is to be processed

# COMMAND ----------

race_resluts_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
    .filter(f'file_date = "{v_file_date}"')

# COMMAND ----------

race_year_list = df_column_to_list(race_resluts_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_resluts_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

drivers_standings_df = race_resluts_df \
    .groupBy('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = drivers_standings_df.withColumn('driver_rank', rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')
