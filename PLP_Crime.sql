-- Databricks notebook source
create table `Crime` using CSV options(path"/FileStore/tables/cleaned_crimeData.csv",header "true")

-- COMMAND ----------

create table `dimension_State` using CSV options(path"/FileStore/tables/dim_state.csv",header "true")

-- COMMAND ----------

create table `dimension_District` using CSV options(path"/FileStore/tables/dim_district.csv",header "true")

-- COMMAND ----------

create table `fact_table` using CSV options(path"/FileStore/tables/fact_district.csv",header "true")

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from crime

-- COMMAND ----------

describe dimension_State

-- COMMAND ----------

describe fact_table

-- COMMAND ----------

select d.state_name,sum(f.Count_Assault_on_Women) as Total_Assault_on_Women
from dimension_State d join fact_table f
on d.State_id=f.State_id
group by d.state_name
order by Total_Assault_on_Women desc
limit 5

-- COMMAND ----------

select count(distinct(state_name)) from dimension_State

-- COMMAND ----------

select sum(f.Count_Murders) as Total_Murders,f.Year
from dimension_State d join fact_table f
on d.State_id=f.State_id
where d.state_name="ANDHRA PRADESH"
group by f.Year
order by f.Year

-- COMMAND ----------

select sum(Count_Murders+Count_Kidnapping+Count_Dacoity+Count_Robbery) as sum,Year
from fact_table
group by Year
order by sum desc
limit 5
