# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database if not exists Sample

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Sample")
#//fetch metadata data from the catalog. your database name will be listed here
dbs = spark.catalog.listDatabases()
display(dbs)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

# COMMAND ----------

sch=StructType([
  StructField("AccountId", IntegerType(), True),
  StructField("TranDate", DateType(), True),
  StructField("TranAmt", DecimalType(), True)])
#spark.catalog.createTable doesn't work for me

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));")
spark.sql("CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);")

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO Sample.Transactions VALUES 
# MAGIC ( 1, '2011-01-01', 500),
# MAGIC ( 1, '2011-01-15', 50),
# MAGIC ( 1, '2011-01-22', 250),
# MAGIC ( 1, '2011-01-24', 75),
# MAGIC ( 1, '2011-01-26', 125),
# MAGIC ( 1, '2011-01-28', 175),
# MAGIC ( 2, '2011-01-01', 500),
# MAGIC ( 2, '2011-01-15', 50),
# MAGIC ( 2, '2011-01-22', 25),
# MAGIC ( 2, '2011-01-23', 125),
# MAGIC ( 2, '2011-01-26', 200),
# MAGIC ( 2, '2011-01-29', 250),
# MAGIC ( 3, '2011-01-01', 500),
# MAGIC ( 3, '2011-01-15', 50 ),
# MAGIC ( 3, '2011-01-22', 5000),
# MAGIC ( 3, '2011-01-25', 550),
# MAGIC ( 3, '2011-01-27', 95 ),
# MAGIC ( 3, '2011-01-30', 2500)

# COMMAND ----------

data_trans = [( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)]
columns=["AccountId","TranDate", "TranAmt"]
df_trans = spark.createDataFrame(data_trans, columns)
df_trans.write.insertInto("Sample.Transactions",overwrite=True)
spark.sql('select * from Sample.Transactions').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Sample.Logical
# MAGIC VALUES (1,'George', 800),
# MAGIC (2,'Sam', 950),
# MAGIC (3,'Diane', 1100),
# MAGIC (4,'Nicholas', 1250),
# MAGIC (5,'Samuel', 1250),
# MAGIC (6,'Patricia', 1300),
# MAGIC (7,'Brian', 1500),
# MAGIC (8,'Thomas', 1600),
# MAGIC (9,'Fran', 2450),
# MAGIC (10,'Debbie', 2850),
# MAGIC (11,'Mark', 2975),
# MAGIC (12,'James', 3000),
# MAGIC (13,'Cynthia', 3000),
# MAGIC (14,'Christopher', 5000);

# COMMAND ----------

data_logical = [(1,'George', 800),
(2,'Sam', 950),
(3,'Diane', 1100),
(4,'Nicholas', 1250),
(5,'Samuel', 1250),
(6,'Patricia', 1300),
(7,'Brian', 1500),
(8,'Thomas', 1600),
(9,'Fran', 2450),
(10,'Debbie', 2850),
(11,'Mark', 2975),
(12,'James', 3000),
(13,'Cynthia', 3000),
(14,'Christopher', 5000)]
columns=["RowID","FName", "Salary"]
df_logic = spark.createDataFrame(data_logical, columns)
df_logic.write.insertInto("Sample.Logical",overwrite=True)
spark.sql('select * from Sample.Logical').show()

# COMMAND ----------

# MAGIC %md 
# MAGIC Totals based on previous row

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT AccountId,
# MAGIC TranDate,
# MAGIC TranAmt,
# MAGIC -- running total of all transactions
# MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
# MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

# COMMAND ----------

#A group by normally reduces the number of rows returned by rolling them up and calculating averages or sums for each row. partition by does not affect the number of rows returned, but it changes how a window function's result is calculated.

# COMMAND ----------

windowSpec  = Window.partitionBy("AccountId").orderBy("TranDate")
new=df_trans.withColumn("RunTotalAmt", sum(col("TranAmt")).over(windowSpec)).orderBy("AccountId", "TranDate")
display(new)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AccountId,
# MAGIC TranDate,
# MAGIC TranAmt,
# MAGIC -- running average of all transactions
# MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
# MAGIC -- running total # of transactions
# MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
# MAGIC -- smallest of the transactions so far
# MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
# MAGIC -- largest of the transactions so far
# MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
# MAGIC -- running total of all transactions
# MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
# MAGIC FROM Sample.Transactions 
# MAGIC ORDER BY AccountId,TranDate;

# COMMAND ----------

windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
new2 = df_trans.withColumn("RunAvg",avg("TranAmt").over(windowSpec)) \
  .withColumn("RunTranQty",count("*").over(windowSpec)) \
  .withColumn("RunSmallAmt",min("TranAmt").over(windowSpec)) \
  .withColumn("RunLargeAmt",max("TranAmt").over(windowSpec)) \
  .withColumn("RunTotalAmt",sum("TranAmt").over(windowSpec)) \
  .orderBy("AccountId", "TranDate")
display(new2)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Calculating Totals Based Upon a Subset of Rows

# COMMAND ----------

# MAGIC %md 
# MAGIC * CURRENT ROW, the current row
# MAGIC * UNBOUNDED PRECEDING, all rows before the current row 
# MAGIC * UNBOUNDED FOLLOWING, all rows after the current row 
# MAGIC * x PRECEDING, x rows before the current row -> relative
# MAGIC * y FOLLOWING, y rows after the current row -> relative

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AccountId,
# MAGIC TranDate,
# MAGIC TranAmt,
# MAGIC -- average of the current and previous 2 transactions
# MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
# MAGIC -- total # of the current and previous 2 transactions
# MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
# MAGIC -- smallest of the current and previous 2 transactions
# MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
# MAGIC -- largest of the current and previous 2 transactions
# MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
# MAGIC -- total of the current and previous 2 transactions
# MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
# MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
# MAGIC FROM Sample.Transactions 
# MAGIC ORDER BY AccountId, TranDate, RN

# COMMAND ----------

windowSpec = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
windowSpec2 = Window.partitionBy("AccountId").orderBy("TranDate")
new3 = df_trans.withColumn("SlideAvg",avg("TranAmt").over(windowSpec)) \
  .withColumn("SlideQty",count("*").over(windowSpec)) \
  .withColumn("SlideMin",min("TranAmt").over(windowSpec)) \
  .withColumn("SlideMax",max("TranAmt").over(windowSpec)) \
  .withColumn("SlideTotal",sum("TranAmt").over(windowSpec)) \
  .withColumn("RN", row_number().over(windowSpec2)) \
  .orderBy("AccountId", "TranDate")
display(new3)

# COMMAND ----------

# MAGIC %md
# MAGIC * Logical Window

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT RowID,
# MAGIC FName,
# MAGIC Salary,
# MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
# MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
# MAGIC 
# MAGIC FROM Sample.Logical
# MAGIC ORDER BY RowID;

# COMMAND ----------

windowSpec1 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
windowSpec2 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
new4 = df_logic.withColumn("SumByRows",sum("Salary").over(windowSpec1)) \
  .withColumn("SumByRange",sum("Salary").over(windowSpec2)) \
  .orderBy("RowID")
display(new4)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AccountNumber,
# MAGIC OrderDate,
# MAGIC TotalDue,
# MAGIC ROW_NUMBER() OVER (PARTITION BY AccountNumber ORDER BY OrderDate) AS RN
# MAGIC FROM Sales.SalesOrderHeader
# MAGIC ORDER BY AccountNumber
# MAGIC LIMIT 10; 

# COMMAND ----------

filePath = "dbfs:/user/hive/warehouse/salesorderheader"
salesorderheader = spark.read.format("delta") \
              .option("header","true") \
              .option("inferSchema","true") \
              .load(filePath)

display(salesorderheader)

# COMMAND ----------

windowSpec  = Window.partitionBy("AccountNumber").orderBy("OrderDate")
new5=salesorderheader.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_number().over(windowSpec).alias("RN") ).orderBy("AccountNumber").limit(10)
display(new5)
