# Databricks notebook source

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
create table if not exists f1_presentation.calculated_race_results
(
  race_year INT,
  team_name STRING,
  driver_id INT,
  driver_name STRING,
  race_id INT,
  position INT,
  points INT,
  calculated_points INT,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)
USING DELTA """)

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_processed

# COMMAND ----------

spark.sql(f"""
create or replace TEMP VIEW race_results_updated
as
select races.race_year,
constructors.name AS team_name,
drivers.driver_id,
drivers.name As driver_name,
races.race_id,
results.position,
results.points,
11- results.position as calculated_points
from f1_processed.results
join f1_processed.drivers on (results.driver_id = drivers.driver_id)
join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
join f1_processed.races on (results.result_id = races.race_id)
where results.position <= 10
AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updated upd
# MAGIC ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.position = upd.position,
# MAGIC             tgt.points = upd.points,
# MAGIC             tgt.calculated_points = upd.calculated_points,
# MAGIC             tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points,created_date) VALUES (race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points, current_timestamp)

# COMMAND ----------

# %sql
# create table f1_presentation.calculated_race_results
# using parquet
# as
# select races.race_year,
# constructors.name AS team_name,
# drivers.name As driver_name,
# results.position,
# results.points,
# 11- results.position as calculated_points
# from results
# join f1_processed.drivers on (results.driver_id = drivers.driver_id)
# join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
# join f1_processed.races on (results.result_id = races.race_id)
# where results.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results where race_year=2021

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation.calculated_race_results