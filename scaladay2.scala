// Databricks notebook source
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// COMMAND ----------

//spc is sparkconf object and sc is sparkcontext object, setappname is used to provide a name to cluster
val spc = new SparkConf().setMaster("localhost[*]").setAppName("First RDD")
//val sc = new SparkContext(spc)

// COMMAND ----------

//first method to create RDD
var list1 = List(1,2,3,4)
var rdd = sc.parallelize(list1)

// COMMAND ----------

//do not use collect option when your data is larger than ram and also collect not used in real time
rdd.collect()

// COMMAND ----------

//second way to create RDD
val rdd1 = sc.parallelize(List(1,2,3))
rdd1.collect()

// COMMAND ----------

//another way to create RDD
sc.parallelize(List(1,2,3)).collect()

// COMMAND ----------

rdd1.partitions.size

// COMMAND ----------

//we can give partitions depending on data size or depending on codes 
val rdd1 = sc.parallelize(List(1,2,3),3)

// COMMAND ----------

rdd1.partitions.size

// COMMAND ----------

rdd1.map( tech => tech * tech).collect()

// COMMAND ----------

//here teach is nothing but a variable
//takes the data only upto particular partition
rdd1.map( tech => tech * tech).take(1)

// COMMAND ----------

rdd1.map( tech => tech * tech).take(3)

// COMMAND ----------

rdd1.map( tech => tech * tech).top(2)

// COMMAND ----------

//map takes whole string as single entity and performs operations
val rdd2 = sc.parallelize(List("hey","hello"))
rdd2.flatMap( tech => tech + "!!!").collect()

// COMMAND ----------

rdd2.filter( x => x=="hey").collect()

// COMMAND ----------

val rdd3 = sc.parallelize(List(1,2,3,4,6,8,0))
rdd3.filter(x => x % 2 !=0).collect()

// COMMAND ----------

rdd3.reduce( (x,y) => x+y )

// COMMAND ----------

val rdd3 = sc.parallelize(List(1,2,3,4,6,8,0,abc))
rdd3.reduce( (x,y) => x+y )

// COMMAND ----------

rdd3.count

// COMMAND ----------

val rdd4 = sc.parallelize(List("hey","hello","bye"))
rdd4.map( x => x.split(",")).collect()

// COMMAND ----------

rdd4.flatMap( x => x.split(",")).collect()

// COMMAND ----------

val rdd5 = sc.parallelize(Seq( (1,"swathi",2020),(2,"ram",2021)))
val rdd6 = sc.parallelize(Seq( (3,"swathiiii",2022),(4,"ram",2023)))
//in union duplicate values are present
val out = rdd5.union(rdd6)
out.collect()

// COMMAND ----------

//to print only distinct values
out.distinct().collect()

// COMMAND ----------

//only common data is displayed
val rdd5 = sc.parallelize(Seq( (1,"swathi",2020),(2,"ram",2021)))
val rdd6 = sc.parallelize(Seq( (1,"swathiiii",2022),(2,"ram",2021)))
rdd5.intersection(rdd6).collect()

// COMMAND ----------

//grouping data according to keys
val rdd12 = sc.parallelize(Array(("one",1),("two",2),("one",3)))
rdd12.groupByKey().collect()

// COMMAND ----------

var rdd13 = sc.parallelize(Array("one","two","three","two","one"))
rdd13.map( x => (x,1)).collect()

// COMMAND ----------

//reduce the list and gives number of times the list is repeated
rdd13.map( x => (x,1)).reduceByKey(_+_).collect()
