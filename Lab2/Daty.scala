// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------

dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------

val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss") )

zmianaFormatu.printSchema()
zmianaFormatu.show

// COMMAND ----------

val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(xxxx, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")
tempE.show

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

dataFrame.show

// COMMAND ----------

//tworzenie nowych kolumn
val newDataFrame = dataFrame
  .withColumn("year",year($"current_date").as("year"))
  .withColumn("month",month($"current_date").as("month"))
  .withColumn("dayofyear",dayofyear($"current_date").as("dayaofyear"))
display(newDataFrame)


// COMMAND ----------

//1. unix_timestamp()
val zmianaFormatu1 = newDataFrame
  .select( $"*", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss") )
  .withColumnRenamed("unix_timestamp(timestamp, yyyy-MM-dd'T'HH:mm:ss)", "unix_timestamp from 'timestamp' column")
zmianaFormatu1.show


// COMMAND ----------

//2. date_format()
val zmianaFormatu2 = newDataFrame
  .select( $"*", date_format($"current_date","yy MM d").as("Different Format of current_date"))
zmianaFormatu2.show


// COMMAND ----------

//3. from_unixtime()
val zmianaFormatu3 = newDataFrame
   .select( $"*", 
   from_unixtime($"unix").as("from unix as yyy-MM-dd HH:mm:ss"),
   from_unixtime($"unix","MM-dd-yyyy").as("from unix as MM-dd-yyyy"))
zmianaFormatu3.show


// COMMAND ----------

//4. to_date)
val zmianaFormatu4 = newDataFrame
   .select( $"*", 
   to_date($"current_timestamp").as("date"))
zmianaFormatu4.show


// COMMAND ----------

//5. to_timestamp()
val zmianaFormatu5 = newDataFrame
   .select( $"*", 
   to_timestamp($"unix").as("to_timestamp as yyy-MM-dd HH:mm:ss"))
zmianaFormatu5.show

// COMMAND ----------

//6. from_utc_timestamp()
val zmianaFormatu6 = newDataFrame
    .select( $"*", 
   from_utc_timestamp($"current_timestamp","JST").as("to_timestamp as yyy-MM-dd HH:mm:ss"))
zmianaFormatu6.show

// COMMAND ----------

//7.to_utc_timestamp()
val zmianaFormatu7 = newDataFrame
    .select( $"*", 
   to_utc_timestamp($"current_timestamp","America/Denver").as("to_timestamp as yyy-MM-dd HH:mm:ss"))
zmianaFormatu7.show
