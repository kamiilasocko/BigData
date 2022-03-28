// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")
//display(SalesLT)
val names=SalesLT.select("TABLE_NAME").as[String].collect.toList

// COMMAND ----------

for( i <- names){
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  table.write.format("delta").mode("overwrite").saveAsTable(i)
}

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse"))


// COMMAND ----------

//show tables
val namesLow=names.map(x => x.toLowerCase())

for( i<-namesLow ){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(filePath)
   df.show
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

// COMMAND ----------

//W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
def countCols2(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull || 
                 col(c).contains("NULL") || 
                 col(c).contains("null"),c)
           ).alias(c)
    })
}
//po kolumnach
for( i <- namesLow){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  println("Table name: "+i)
  df.select(countCols2(df.columns):_*).show()
}

// COMMAND ----------

//po wierszach
//i-kazda z tabel/nazwa tabeli
for( i <- namesLow){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  println("Table name: "+i)
  val counter=df.columns.map(x => when(col(x).isNull || col(x).contains("NULL") || 
                 col(x).contains("null"), 1).otherwise(0)).reduce(_+_)
  val df2 = df.withColumn("nullsInRow", counter).show()
}

// COMMAND ----------

//Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
for( i <- namesLow){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  val notNullTab=df.na.fill("").na.fill(0)
  println("Table name: "+i)
  notNullTab.select(countCols2(notNullTab.columns):_*).show()
}

// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle
for( i <- namesLow){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  val notNullTab=df.na.drop()
  println("Table name: "+i)
  notNullTab.select(countCols2(notNullTab.columns):_*).show()
}

// COMMAND ----------

//dla konkretnego przykładu
val filePath = s"dbfs:/user/hive/warehouse/customer"
val customer = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val customer_notnull=customer.na.drop()
customer_notnull.select(countCols2(customer_notnull.columns):_*).show()

// COMMAND ----------

val count=customer.count()
val count2=customer_notnull.count()

// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
//pobieranie danych
val filePath = s"dbfs:/user/hive/warehouse/salesorderheader"
val salesOrderH = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
salesOrderH.groupBy("Status").agg(max("TaxAmt")).show()
salesOrderH.groupBy("ShipMethod").agg(min("TaxAmt")).show()
salesOrderH.groupBy("ShipDate").agg(sum("Freight")).show()

// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
////Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)
//pobieranie danych
val filePath = s"dbfs:/user/hive/warehouse/product"
val product = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
product.printSchema

// COMMAND ----------

product.groupBy("ProductModelID","Color","ProductCategoryID").agg(Map("Size" -> "mean","Weight"->"min")).show()

// COMMAND ----------

product.groupBy("ProductModelID","Color").agg(avg("ListPrice")).show()

// COMMAND ----------

// MAGIC %md **zadanie 2**
// MAGIC 
// MAGIC Stwórz 3 funkcje UDF do wybranego zestawu danych, 
// MAGIC 
// MAGIC * Dwie funkcje działające na liczbach, int, double 
// MAGIC 
// MAGIC * Jedna funkcja na string 

// COMMAND ----------

product.printSchema

// COMMAND ----------

//na stringach
//val lower: String => String = _.toLowerCase
//val lowerUDF = udf(lower)
//val lowerUDF = udf[String, String](_.toLowerCase)

val lowerUDF=udf((s: String) => s.toUpperCase)
val new_df=product.withColumn("color_lower",lowerUDF($"Color").as("color_lower")).show()

//display(new_df2.select("color_lower")) doesn't work

// COMMAND ----------

//na intach
val multipUDF = udf((x: Int) => x*x)
val new_df=product.withColumn("Size^2",multipUDF($"Size").as("Size^2")).show()

// COMMAND ----------

//na doublach
val ton = udf((x: Double) => x/1000)
val new_df=product.withColumn("WeightInTon",ton($"Weight").as("WeightInTon"))
new_df.select("WeightInTon","Weight").show()

// COMMAND ----------

// MAGIC %md **zadanie 3** 
// MAGIC 
// MAGIC Flatten json, wybieranie atrybutów z pliku json. 
// MAGIC 
// MAGIC  

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/dane/brzydki.json"))
