# Databricks notebook source
dictionary={"name":"swathi","age":20,"name":"sweety"}
print(dictionary)

# COMMAND ----------

import numpy as np

# COMMAND ----------

arr = np.array([1,2,3,4])

# COMMAND ----------

arr

# COMMAND ----------

arr + 5

# COMMAND ----------

arr = arr + 5
arr

# COMMAND ----------

arr % 2

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.Series([1,2,3,4], index=["a","b","c","d"])

# COMMAND ----------

s = pd.Series({"name":"swathi","age":10},index=["name","age","gender"])
print("\n",s)

# COMMAND ----------

pd.Series(9,index=[1,2,3,4])

# COMMAND ----------

ser = pd.Series({"a":1,"b":2,"c":3},index=["b","d","a","c"])
ser

# COMMAND ----------

ser[2:]

# COMMAND ----------

ser[0:2]

# COMMAND ----------

ser[:3]

# COMMAND ----------

ser["a"]

# COMMAND ----------

ser[["a","b"]]

# COMMAND ----------

pd.Series([1,2,3,4],index=["z","y","x","w"])

# COMMAND ----------

list=[1,2,3,4]
pd.DataFrame(list)

# COMMAND ----------

list1 = [[1,2,3,4],[5,6,7,8]]
pd.DataFrame(list1)

# COMMAND ----------

list1 = [["swathi",20],["sweety",22]]
pd.DataFrame(list1,index=["a","b"],columns=["name","age"])

# COMMAND ----------

dict = {"name":"swathi","age":20}
pd.DataFrame(dict,index=["a"])

# COMMAND ----------

dict = {"name":["swathi","parrot"],"age":[20,22]}
pd.DataFrame(dict,index=["a","b"])

# COMMAND ----------

dict1 = {"name": pd.Series([1,2,3,4])}
pd.DataFrame(dict1)

# COMMAND ----------

dict2 = {"name":"swathi","age":20,"gmail":"abc@gmail.com","phone":546452457}
df2=pd.DataFrame(dict2,index=[1])

# COMMAND ----------

df3["new_col"]=df2["age"]+df2["phone"]
pd.DataFrame(df3["new_col"])

# COMMAND ----------

del df3["new_col"]
df3

# COMMAND ----------

# //loc
# //iloc
df3.loc[1]
df3.iloc[:1][0:3]
df3.iloc[:2][:4]

