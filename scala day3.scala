// Databricks notebook source
val rdd=sc.parallelize(List(1,2,3,4))
rdd.collect()

// COMMAND ----------

rdd.unpersist()

// COMMAND ----------

rdd.count()

// COMMAND ----------

//gives dag information
rdd.toDebugString

// COMMAND ----------

import org.apache.spark.sql.Row

// COMMAND ----------

val row = Row(1,"hai")
row.get(1)
row.getAs[Int]{0}
Row.fromTuple{(0,"hello")}
Row.merge{Row(1);Row("hai")}
//Row.empty==Row()

// COMMAND ----------

val broad = sc.broadcast((Array(1,2,3)))
broad.value

// COMMAND ----------

val accum = sc.accumulator(0)

// COMMAND ----------

sc.parallelize(Array(1,2,3,4)).foreach(x=> accum+=x)
accum
