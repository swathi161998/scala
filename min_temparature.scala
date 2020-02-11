// Databricks notebook source
import org.apache.spark._
import scala.math.max
import scala.math.min

// COMMAND ----------

 var data = sc.textFile("/FileStore/tables/1800.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

def minTemperature(data:String)= {
  val fields = data.split(",")
  // this is to get station id
  val station = fields(0)
  //this is get temparature type
  val tempType = fields(2)
  //this is to get value of temparature
  val tempvalue=fields(3).toInt
(station,tempType,tempvalue)
}

// COMMAND ----------

val newrdd = data.map(minTemperature)
newrdd.take(5)

// COMMAND ----------

//creating new rdd with filtered temperature as maximum
val filtertemp = newrdd.filter( x=> x._2=="TMAX")
filtertemp.collect()
//filtertemp.take(2)

// COMMAND ----------

filtertemp.foreach(println).take()

// COMMAND ----------

val rdd1 = sc.parallelize(List(1,2,3,4,5))
rdd1.filter( x => x!=3).collect()

// COMMAND ----------

//I am getting only the station name and temp
val req = filtertemp.map( x=> (x._1,x._3))
//filtertemp.filter( x => x!="TMAX").collect()

// COMMAND ----------

req.take(20)

// COMMAND ----------

//filtertemp.reduceByKey((x,y) => max(x,y)).take(2)

// COMMAND ----------

req.reduceByKey((x,y) => max(x,y)).take(2)

// COMMAND ----------

req.reduceByKey((x,y) => min(x,y)).take(2)

// COMMAND ----------


