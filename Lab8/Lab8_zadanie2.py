# Databricks notebook source
# MAGIC %md 
# MAGIC **Funkcje - POKA YOKE**

# COMMAND ----------

def isFormatCorrect(form, path) :
    if isinstance(form, str) and form in path:
        return True
    else :
        return False
    
isFormatCorrect('json','pp.parquet')

# COMMAND ----------

def isPathCorrect(path) :
    if isinstance(path, str) :
        return True


def returnData(path, form) :
    if isPathCorrect(path) and isFormatCorrect(form, path): 
        if isDataAvailable(path) :
            df=spark.read.format(form).load(path)
            return df
    else :
        print("Error")
    
returnData("dbfs:/wiki.parquet","parquet")

# COMMAND ----------

def isDFEmpty(df) :
    return len(df.head(1)) == 0
isDFEmpty(returnData("dbfs:/wiki.parquet","parquet"))

# COMMAND ----------

from pyspark.sql.functions import col,isnan,when,count

# COMMAND ----------

def isAnyNull(df):
    table=df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    for i in table.columns :
        if table.first()[i] != 0 :
            return True
    return False
        
isAnyNull(returnData("dbfs:/wiki.parquet","parquet"))

# COMMAND ----------

def isDataAvailable(path):
    try :
        dbutils.fs.ls(path)
        return True
    except :
        return False
isDataAvailable("dbfs:/wiki.paquet")
