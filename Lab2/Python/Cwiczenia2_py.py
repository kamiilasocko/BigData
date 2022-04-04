# Databricks notebook source

#Wybierz jeden z plików csv z poprzednich ćwiczeń i stwórz ręcznie schemat danych. Stwórz 
#DataFrame wczytując plik z użyciem schematu.

from pyspark.sql.types import *

FileSchema = StructType([
  StructField("imdb_title_id", StringType(), True),
  StructField("ordering", IntegerType(), True),
  StructField("imdb_name_id", StringType(), True),
  StructField("category", StringType(), True),
  StructField("job", StringType(), True),
  StructField("characters", StringType(), True)])

filePath = "dbfs:/FileStore/tables/dane/actors.csv"
act = spark.read.format("csv") \
          .option("header","true") \
          .load(filePath, schema=FileSchema)
act.printSchema()

# COMMAND ----------

#sprawdzenie plikow w katalogu
display(dbutils.fs.ls("dbfs:/FileStore/tables/dane/"))

# COMMAND ----------


df = spark.createDataFrame([], schema)

json_file_1 = {"fruit": "Apple","size": "Large"}
json_df_1 = spark.read.json(sc.parallelize([json_file_1]))

df = df.unionByName(json_df_1, allowMissingColumns=True)

json_file_2 = {"fruit": "Banana","size": "Small","color": "Yellow"}

df = df.unionByName(json_file_2, allowMissingColumns=True)

display(df)

# COMMAND ----------

#Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego 
#pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com

js=[
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
#create empty dataframe with default schema
df = spark.createDataFrame([], FileSchema)
#read a json 
json_df = spark.read.json(sc.parallelize([js]))
#load json file to empty dataframe and allow to have missing columns
df = df.unionByName(json_df, allowMissingColumns=True)
 
display(df)

# COMMAND ----------

#save df to json
df.write.json("dbfs:/FileStore/tables/Actors_json_Py.json", mode="overwrite")
#read json from file
json_actors = spark.read.schema(FileSchema).json("dbfs:/FileStore/tables/Actors_json_Py.json")
display(json_actors)

# COMMAND ----------

#Użycie Read Modes. 
#Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz 
#co się dzieje. Jeśli jedna z opcji nie da żadnych efektów, trzeba popsuć dane.

record1 = {"ppp":"f"}

record2 = {'imdb_title_id': 'tt0000009', 'ordering': 1, 'imdb_name_id': 'nm0063086'}

record3 = {'imdb_title_id': 'tt0000009', 'ordering': 2, 'imdb_name_id': 'nm0183823'}

df1 = spark.createDataFrame([record1,record3,record2], FileSchema)
#df1.write.json("dbfs:/FileStore/tables/bad.json", mode="overwrite")
display(df1)

# COMMAND ----------

#check writing to json
path="dbfs:/FileStore/tables/bad.json"
df1_check = spark.read.format("csv").schema(FileSchema).load(path)
display(df1_check)

# COMMAND ----------

BadRecord = spark.read.format("csv").schema(FileSchema) \
            .option("badRecordsPath", "/FileStore/tables/badrecords") \
            .load(path)
    
display(BadRecord)

# COMMAND ----------

DropMalFormed = spark.read.format("csv").option("mode","DROPMALFORMED") \
        .schema(FileSchema) \
        .load(path)
#delete bad records
display(DropMalFormed)

# COMMAND ----------

Permissive =  spark.read.format("csv").option("mode","PERMISSIVE") \
        .schema(FileSchema) \
        .load(path)
display(Permissive)

# COMMAND ----------

FailFast =  spark.read.format("csv").option("mode","FAILFAST") \
        .schema(FileSchema) \
        .load(path)
display(FailFast)

# COMMAND ----------

display(df)

# COMMAND ----------

#//Użycie DataFrameWriter.
#//Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane 
#//poprawnie, użyj do tego DataFrameReader. Opisz co widzisz w docelowej ścieżce i otwórz używając 
#//DataFramereader.
#//PARQUET

df.write.format("parquet").mode("overwrite").save("/FileStore/tables/actors_p.parquet")
actors_p = spark.read.format("parquet").load("/FileStore/tables/actors_p.parquet")
display(actors_p)


# COMMAND ----------

#//JSON
df.write.format("json").mode("overwrite").save("/FileStore/tables/actors_j.json")
actors_j = spark.read.format("json").load("/FileStore/tables/actors_j.json")
display(actors_j)

# COMMAND ----------

#//sprawdzenie plikow w katalogu
display(dbutils.fs.ls("dbfs:/FileStore/tables/actors_j.json/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/actors_p.parquet/"))
