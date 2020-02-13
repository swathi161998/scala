# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("first pyark").getOrCreate()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/containsNulls.csv", inferSchema=True, header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.drop("name").show()

# COMMAND ----------

df.dropna().show()

# COMMAND ----------

df.dropna(thresh=2).show()

# COMMAND ----------

#remove data will all column values as null
df.dropna(how='all').show()

# COMMAND ----------

df.dropna(how='any').show()

# COMMAND ----------

df.dropna(subset=['sales']).show()

# COMMAND ----------

#to fill the null values
df.fillna('tushar').show()
#df.fillna(2).show()

# COMMAND ----------

df.printSchema

# COMMAND ----------

df.fillna('tushar',subset='Name').show()

# COMMAND ----------

#df.fillna('tushar',1,subset='Name','sales').show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.take(1)  #list
df.take(1)[0] #tuple
df.take(1)[0][0] #particular index value

# COMMAND ----------

from pyspark.sql.functions import mean
df2=df.select(mean("sales"))
df2

# COMMAND ----------

df3 = df2.take(1)[0][0]
df3

# COMMAND ----------

df4 = df.fillna(df3,subset=['sales'])
df4.show()
