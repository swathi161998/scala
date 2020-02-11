// Databricks notebook source
import org.apache.spark._
import scala.math.min
import scala.math.max

// COMMAND ----------

var data1=sc.textFile("/FileStore/tables/fakefriends.csv")

// COMMAND ----------

data1.collect()

// COMMAND ----------

def fakefriends(data1:String)= {
  val fields = data1.split(",")
  // this is to get station id
  val age = fields(2).toInt
  //this is get temparature type
  val nooffriends = fields(3).toInt
(age,nooffriends)
}

// COMMAND ----------

data1.map(fakefriends).collect()

// COMMAND ----------

var req=data1.map(fakefriends)
req.reduceByKey((x,y) => min(x,y)).collect()

// COMMAND ----------

req.reduceByKey((x,y) => max(x,y)).collect()

// COMMAND ----------

val average = req.sum / req.length

// COMMAND ----------

req.mapValues(x => (x,1)).collect()

// COMMAND ----------

req.map(x => (x,1)).collect()

// COMMAND ----------

val new1 = req.mapValues( x => (x,1))
new1.collect()

// COMMAND ----------

new1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2)).take(2)

// COMMAND ----------

var new2 = new1.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))

// COMMAND ----------

new2.mapValues(x => (x._1/x._2)).collect()

// COMMAND ----------

var rdd = sc.parallelize(Array((1,2),(3,4),(6,8),(6,78)))
rdd.reduceByKey((x,y) => (x+y)).collect()
