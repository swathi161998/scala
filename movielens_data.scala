// Databricks notebook source
import org.apache.spark._

// COMMAND ----------

val rdd = sc.textFile("/FileStore/tables/u.data")
rdd.collect()

// COMMAND ----------

rdd.map( x => x.split("\t")).take(1)

// COMMAND ----------

//to list all the rating values from 2nd column
rdd.map( x => x.split("\t")(2)).collect()

// COMMAND ----------

val rating=rdd.map( x => x.split("\t")(2))
rating.take(1)

// COMMAND ----------

val keyRating=rating.map( x=> (x,1))
keyRating.countByValue()

// COMMAND ----------

keyRating.reduceByKey(_+_).collect()
