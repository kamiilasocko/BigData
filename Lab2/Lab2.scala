// Databricks notebook source
//Wybierz jeden z plików csv z poprzednich ćwiczeń i stwórz ręcznie schemat danych. Stwórz 
//DataFrame wczytując plik z użyciem schematu.

import org.apache.spark.sql.types._

val FileSchema = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", IntegerType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true))
)

val file = spark.read.format("csv")
  .option("header", "true")
  .schema(FileSchema)
  .load("dbfs:/FileStore/tables/Files/actors.csv/")
display(file)
file.printSchema

// COMMAND ----------

//sprawdzenie plikow w katalogu
display(dbutils.fs.ls("dbfs:/FileStore/tables/dane/"))

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
 
// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
}

// COMMAND ----------


//Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego 
//pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com
val schema = new StructType().add("imdb_title_id", StringType).add("ordering",IntegerType).add("imdb_name_id", StringType)
                          
val events = jsonToDataFrame("""
[
   {
      "imdb_title_id":"tt0000009",
      "ordering":1,
      "imdb_name_id":"nm0063086"
   },
   {
      "imdb_title_id":"tt0000009",
      "ordering":2,
      "imdb_name_id":"nm0183823"
   }
]
""", schema)
 
display(events.select("*"))

// COMMAND ----------

//events.write.json("dbfs:/FileStore/tables/Actors_json.json")
val actors_with_schema = spark.read.schema(schema).json("dbfs:/FileStore/tables/Actors_json.json")
display(actors_with_schema)

// COMMAND ----------

//Użycie Read Modes. 
//Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz 
//co się dzieje. Jeśli jedna z opcji nie da żadnych efektów, trzeba popsuć dane.

val record1 = "{d}"

val record2 = "{'imdb_title_id': 'tt0000009', 'ordering': 1, 'imdb_name_id': 'nm0063086'}"

val record3 = "{'imdb_title_id': 'tt0000009', 'ordering': 2, 'imdb_name_id': 'nm0183823'}"

Seq(record1, record2, record3).toDF().write.mode("overwrite").text("/FileStore/tables/actorss_bad.json")

val BadRecord = spark.read.format("json")
  .schema(schema)
  .option("badRecordsPath", "/FileStore/tables/badrecords")
  .load("/FileStore/tables/actorss_bad.json")


val DropMalFormed = spark.read.format("json")
  .schema(schema)
  .option("mode", "DROPMALFORMED")//wiersze są usuwane
  .load("/FileStore/tables/actorss_bad.json")
val Permissive = spark.read.format("json")
  .schema(schema)
  .option("mode", "PERMISSIVE") //Jeśli atrybuty nie mogą zostać wczytane Spark zamienia je na nule
  .load("/FileStore/tables/actorss_bad.json")

val FailFast = spark.read.format("json")
  .schema(schema)
  .option("mode", "FAILFAST")//proces odczytu zostaje całkowicie zatrzymany
  .load("/FileStore/tables/actorss_bad.json")


display(Permissive)


// COMMAND ----------

display(file)

// COMMAND ----------

//Użycie DataFrameWriter.
//Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane 
//poprawnie, użyj do tego DataFrameReader. Opisz co widzisz w docelowej ścieżce i otwórz używając 
//DataFramereader.
//PARQUET
file.write.format("parquet").mode("overwrite").save("/FileStore/tables/actors_p.parquet")
val actors_p = spark.read.format("parquet").load("/FileStore/tables/actors_p.parquet")
display(actors_p)


// COMMAND ----------

//JSON
file.write.format("json").mode("overwrite").save("/FileStore/tables/actors_j.json")
val actors_j = spark.read.format("json").load("/FileStore/tables/actors_j.json")
display(actors_j)

// COMMAND ----------

//sprawdzenie plikow w katalogu
display(dbutils.fs.ls("dbfs:/FileStore/tables/actors_j.json/"))

// COMMAND ----------

Pliki parquet mają mniejszy rozmiar niż json. 
