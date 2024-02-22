-- Databricks notebook source
select driver_name,
count(1) AS total_races,
 SUM(calculated_points) AS total_points,avg(calculated_points)  as avg_points
 from f1_presentation.calculated_race_results
 group by driver_name
 having count(1) >= 50
 order by avg_points desc

-- COMMAND ----------

