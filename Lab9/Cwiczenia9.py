# Databricks notebook source
# MAGIC %md
# MAGIC Użycie funkcji dropFields i danych Nested.json
# MAGIC 
# MAGIC 
# MAGIC Podpowiedź:  withColumn(„nowacol”, $”konlj”.dropfiels(

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Nested.json"
df = spark.read.format("json")\
    .option("multiLine", "true")\
    .option("inferSchema","true")\
    .load(filePath)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(df.withColumn("nowacol", col("pathLinkInfo").dropFields("alternateName", "matchStatus")))

# COMMAND ----------

# MAGIC %md
# MAGIC **FOLDLEFT**
# MAGIC Zaczynając od początkowej wartości 0, funkcja foldLeft stosuje funkcję (m, n) => m + n na każdym elemencie listy oraz poprzedniej zakumulowanej wartości.

# COMMAND ----------

# MAGIC %scala 
# MAGIC val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
# MAGIC val res = numbers.foldLeft(0)((m, n) => m + n)
# MAGIC println(res)

# COMMAND ----------

# MAGIC %scala
# MAGIC val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
# MAGIC val numberFunc = numbers.foldLeft(List[Int]())_
# MAGIC 
# MAGIC val squares = numberFunc((xs, x) => xs:+ x*x)
# MAGIC print(squares.toString()) // List(1, 4, 9, 16, 25, 36, 49, 64, 81, 100)
# MAGIC 
# MAGIC val cubes = numberFunc((xs, x) => xs:+ x*x*x)
# MAGIC print(cubes.toString())  // List(1, 8, 27, 64, 125, 216, 343, 512, 729, 1000)

# COMMAND ----------

# MAGIC %scala
# MAGIC case class Person(name: String, sex: String)
# MAGIC val persons = List(Person("Thomas", "male"), Person("Sowell", "male"), Person("Liz", "female"))
# MAGIC val foldedList = persons.foldLeft(List[String]()) { (accumulator, person) =>
# MAGIC   val title = person.sex match {
# MAGIC     case "male" => "Mr."
# MAGIC     case "female" => "Ms."
# MAGIC   }
# MAGIC   accumulator :+ s"$title ${person.name}"
# MAGIC }
# MAGIC assert(foldedList == List("Mr. Thomas", "Mr. Sowell", "Ms. Liz"))
