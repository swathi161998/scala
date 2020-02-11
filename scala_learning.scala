// Databricks notebook source
// val , var
val variable1: String = "hello world!"

// COMMAND ----------

var variable_var : String = "hello worlllllld 2"
variable_var

// COMMAND ----------

var value1 = "hello"

// COMMAND ----------

var variable1: String ="hey"
variable1 = "hey" + "world" 

// COMMAND ----------

val variable="hello"+"pinky" 

// COMMAND ----------

var a=2
if(a==2){
  println("value 2")
}
else{
  print("no value")
}

// COMMAND ----------

def table(a: Int): Unit={
  for(i<-1 to 11){
   val c:Int = a*i
  println(f"$a x $i = $c")
}
}
table(4)

// COMMAND ----------

def third(y: Int, func : Int => Int): Int =
{
  func(y)
}
third(4, squareInt)

// COMMAND ----------

def numberDivisor(a: Int,b: Int):Float ={
  a/b
}
numberDivisor(35,7)

// COMMAND ----------

def squareInt(x: Int):Int={
  x*x
}
squareInt(2)

// COMMAND ----------

var a=2
if(a==2){
  print("value 2")
}
else{
  print("no value")
}

// COMMAND ----------

for(i<-1 to 4){
  println(i)
}

// COMMAND ----------

var wh=4
while(wh>=4){
  print(f"wh,$wh")
  wh=wh-1
}

// COMMAND ----------

def fun(x:Double,y:Double) : Double={
  x/y
}
def addNumber(variable1: Double, variable2 : Double, fun :(Double,Double) =>Double):Double={
  fun(variable1,variable2)
}
addNumber(42.0,2.0,fun1)

// COMMAND ----------

def add(x:Int,y:Int):Int={
  x+y
}
add(2,3)

// COMMAND ----------

val tup=("hello","hai",4)
tup._2

// COMMAND ----------

var list1 = List("swathi","vandana")
var list2 = List("swathi","vandana")
list1++list2

// COMMAND ----------

var dict=1->"hai"
dict._2

// COMMAND ----------

var newlist =List(1,2,3,4)
newlist.filter((x:Int)=>x!=3)

// COMMAND ----------

var liststring = List("hey","hello")
liststring.map((x:String)=>x.reverse)

// COMMAND ----------

var liststring = List("hey","hello")
liststring.map((x:String)=>x.length)

// COMMAND ----------

var newlist = List(1,2,3,4,5)
newlist.reduce((x:Int,y:Int)=>x+y)

// COMMAND ----------

//here index starts with 0
newlist(2)

// COMMAND ----------

var list6=List("hey","hai","hello","swathi")
list6.map((x:String)=>(x,1))

// COMMAND ----------

var list6=List("hey","hai","hello","swathi")
list6.map((x:String)=>(x(0),1))

// COMMAND ----------

var ss : Byte =124
ss.toInt

// COMMAND ----------

var ss : Byte =124
ss.toString

// COMMAND ----------

var list6=List("hey","hai","hello","swathi")
list6(0).map((x:String)=>(x(0),1))

// COMMAND ----------


