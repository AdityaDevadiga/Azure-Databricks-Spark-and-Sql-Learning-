-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select *,concat(driver_ref, '-' ,code ) AS new_driver_ref
 from drivers

-- COMMAND ----------

select *, SPLIT(name, ' ')[0] forename,SPLIT(name, ' ')[1] surname
from drivers

-- COMMAND ----------

select *,current_timestamp
from drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy')
from drivers

-- COMMAND ----------

select nationality,count(*)
from drivers
group by nationality

-- COMMAND ----------

select nationality,count(*)
from drivers
group by nationality
having count(*) > 100
order by nationality

-- COMMAND ----------

select nationality,name,dob,RANK() OVER(partition by nationality order by dob desc) as age_rank
from drivers
order by nationality,age_rank

-- COMMAND ----------

