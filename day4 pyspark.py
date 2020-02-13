# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("first pyark").getOrCreate()

# COMMAND ----------

#creating data frame object 
#by giving header=true it will consider 1st column as header
#inferSchema=true to know which type of data is present if no datatype is described then it will consider them as string
df = spark.read.csv("/FileStore/tables/fakefriends.csv", inferSchema=True, header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.schema

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.head(4)

# COMMAND ----------

df.select("name","age").show()

# COMMAND ----------

from pyspark.sql.functions import mean

# COMMAND ----------

df.select(mean("age")).show()

# COMMAND ----------

df_age = df.select(mean("age"))
df_age.show()

# COMMAND ----------

df_age = df.select(mean("age").alias("total_mean_age"))
df_age.show()

# COMMAND ----------

group = df.groupBy("age").max()
group.show()

# COMMAND ----------

max_age = df.groupBy("age").max()
max_age.select("age","max(totalfriends)").show()

# COMMAND ----------

max_age = df.groupBy("age").max()
max_age.withColumnRenamed("max(totalfriends)","max(friends)").show()

# COMMAND ----------

max_age = df.groupBy("age").max()
max_age.withColumnRenamed("max(totalfriends)","max(friends)").select("age","max(friends)").show()

# COMMAND ----------

#to add particular column to dataframe
df.withColumn("age/totalfriends",df["totalfriends"]/df["age"]).show()

# COMMAND ----------

df.columns

# COMMAND ----------

df2 = df.withColumn("age/totalfriends",df["totalfriends"]/df["age"])
df2.describe().show()

# COMMAND ----------

from pyspark.sql.functions import format_number
df2.select(format_number("age/totalfriends",2)).show()

# COMMAND ----------

#to get upto that decimal values
df2.select(format_number("age/totalfriends",2),"age","Name").show()

# COMMAND ----------

df2.withColumnRenamed("age/totalfriends","AgeByFriends").select("age",format_number("AgeByFriends",2),"Name").show()

# COMMAND ----------

df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends"),"age","Name").show()

# COMMAND ----------

df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends"),"age","Name").orderBy("AgeByFriends").show()

# COMMAND ----------

df3 = df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends"),"age","Name")
#df3.orderBy(df3["AgeByFriends"].desc()).show()
#df3.orderBy(df3["age"].desc()).show()
df3.describe().show()

# COMMAND ----------

df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends"),"age","Name").orderBy(df2["age"].desc()).show()

# COMMAND ----------

df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends"),"age","Name").orderBy(df2["AgeByFriends"].desc()).show()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

from pyspark.sql.types import DoubleType
df3 = df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends").cast(DoubleType()),"age","Name").show()

# COMMAND ----------

df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends").cast(DoubleType()),"age","Name").orderBy(df2["age"].desc()).show()

# COMMAND ----------

df3 = df2.select(format_number("age/totalfriends",2).alias ("AgeByFriends").cast(DoubleType()),"age","Name")
df3.orderBy(df3["AgeByFriends"].desc()).show()
#df3.orderBy(df3["age"].desc()).show()
