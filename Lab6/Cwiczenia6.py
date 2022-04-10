# Databricks notebook source
# MAGIC %md
# MAGIC **ĆWICZENIA 6**

# COMMAND ----------

# MAGIC %md 
# MAGIC ZADANIE 1
# MAGIC * Wykorzystaj przykłady z notatnika Windowed Aggregate Functions i przepisz funkcje używając Spark API

# COMMAND ----------

# MAGIC %md 
# MAGIC ! Odpowiedź w pliku Windowed Aggregate Function

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/sample.db/"))

# COMMAND ----------

filePath = "dbfs:/user/hive/warehouse/sample.db/transactions"
df_trans = spark.read.format("delta") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

display(df_trans)

# COMMAND ----------

filePath = "dbfs:/user/hive/warehouse/salesorderheader"
salesorderheader = spark.read.format("delta") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

display(salesorderheader)

# COMMAND ----------

filePath = "dbfs:/user/hive/warehouse/salesorderdetail"
salesorderdetail = spark.read.format("delta") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

display(salesorderdetail)

# COMMAND ----------

# MAGIC %md 
# MAGIC ZADANIE 2
# MAGIC * Użyj ostatnich danych i użyj funkcji okienkowych LEAD, LAG, FIRST_VALUE, LAST_VALUE, ROW_NUMBER i DENS_RANK
# MAGIC * Każdą z funkcji wykonaj dla ROWS i RANGE i BETWEEN

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

#ROWSBETWEEN
windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
windowSpec2=windowSpec.rowsBetween(Window.unboundedPreceding, -1) 

DF=df_trans \
    .withColumn("Lead",lead("TranAmt").over(windowSpec)) \
    .withColumn("Lag",lag("TranAmt").over(windowSpec)) \
    .withColumn("FirstValue",first("TranAmt").over(windowSpec2)) \
    .withColumn("LastValue",last("TranAmt").over(windowSpec2)) \
    .withColumn("RowNumber",row_number().over(windowSpec)) \
    .withColumn("DenseRank",dense_rank().over(windowSpec)) \
    .orderBy("AccountId", "TranDate")
display(DF)

# COMMAND ----------

#RANGEBETWEEN
windowSpec = Window.partitionBy("AccountId").orderBy("TranAmt")
windowSpec2=windowSpec.rangeBetween(-90,Window.currentRow) 

DF=df_trans \
    .withColumn("Lead",lead("TranAmt").over(windowSpec)) \
    .withColumn("Lag",lag("TranAmt").over(windowSpec)) \
    .withColumn("FirstValue",first("TranAmt").over(windowSpec2)) \
    .withColumn("LastValue",last("TranAmt").over(windowSpec2)) \
    .withColumn("RowNumber",row_number().over(windowSpec)) \
    .withColumn("DenseRank",dense_rank().over(windowSpec)) \
    .orderBy("AccountId", "TranDate")
display(DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ZADANIE 3
# MAGIC * Użyj ostatnich danych i wykonaj połączenia Left Semi Join, Left Anti Join, za każdym razem sprawdź .explain i zobacz jak spark wykonuje połączenia. Jeśli nie będzie danych to trzeba je zmodyfikować żeby zobaczyć efekty.

# COMMAND ----------

joinExpression=[salesorderheader["SalesOrderID"] == salesorderdetail["SalesOrderID"]]
result =salesorderheader.join(salesorderdetail, joinExpression, "leftsemi")
display(result)
result.explain()

# COMMAND ----------

salesorderheaderBAD = salesorderheader.withColumn("SalesOrderID", when(col("SalesOrderID") == "71780", 99).otherwise(col("SalesOrderID")))
joinExpression=[salesorderheaderBAD["SalesOrderID"] == salesorderdetail["SalesOrderID"]]
result =salesorderheaderBAD.join(salesorderdetail, joinExpression, "leftanti")
display(result)
result.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC ZADANIE 4
# MAGIC * Połącz tabele po tych samych kolumnach i użyj dwóch metod na usunięcie duplikatów. 

# COMMAND ----------

result =salesorderheader.join(salesorderdetail, ["SalesOrderID"])
display(result)

# COMMAND ----------

joinExpression=[salesorderheader["SalesOrderID"] == salesorderdetail["SalesOrderID"]]
result =salesorderheader.join(salesorderdetail, joinExpression).drop(salesorderdetail["SalesOrderID"])
display(result)

# COMMAND ----------

# MAGIC %md 
# MAGIC ZADANIE 5
# MAGIC * W jednym z połączeń wykonaj broadcast join, i sprawdź plan wykonania.

# COMMAND ----------

from pyspark.sql.functions import broadcast
joinExpression=[salesorderheader["SalesOrderID"] == salesorderdetail["SalesOrderID"]]
result =salesorderheader.join(broadcast(salesorderdetail), joinExpression).drop(salesorderdetail["SalesOrderID"])
display(result)
result.explain()
