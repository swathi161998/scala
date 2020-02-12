// Databricks notebook source
import org.apache.spark._

// COMMAND ----------

val rdd = sc.textFile("/FileStore/tables/u.data")
rdd.collect()

// COMMAND ----------

def movielens(rdd:String)= {
  val fields = rdd.split("\t")
  val id = fields(0).toInt
  val rating = fields(2).toInt
(id,rating)
}

// COMMAND ----------

rdd.map(movielens).collect()

// COMMAND ----------

val evenid = rdd.map(movielens).filter(x => (x._1%2==0))
evenid.collect()

// COMMAND ----------

val oddid = rdd.map(movielens).filter(x => (x._1%2!=0))
oddid.collect()

// COMMAND ----------

val sum1=evenid.mapValues(x => (x,1))
sum1.collect()

// COMMAND ----------

var evensum = sum1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
evensum.collect()

// COMMAND ----------

val sum2=oddid.mapValues(x => (x,1))
sum2.collect()

// COMMAND ----------

var oddsum = sum2.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
oddsum.collect()

// COMMAND ----------

val even_avg=evensum.mapValues(x => (x._1/x._2))
even_avg.collect()

// COMMAND ----------

val odd_avg=oddsum.mapValues(x => (x._1/x._2))
odd_avg.collect()

// COMMAND ----------

val Outputrdd = even_avg.union(odd_avg)
Outputrdd.saveAsTextFile("/FileStore/tables/Avg_data")

// COMMAND ----------

even_avg.intersection(odd_avg).collect()
