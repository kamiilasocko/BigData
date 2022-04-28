# Databricks notebook source
# MAGIC %md
# MAGIC **Zadanie 1**
# MAGIC 
# MAGIC Czy Hive wspiera indeksy? Jeśli staniesz przed problemem – czy da się szybko wyciągać dane?
# MAGIC Odpowiedz na pytanie i podaj przykłady jak rozwiążesz problem indeksów w Hive.
# MAGIC 
# MAGIC 
# MAGIC Na podstawie informacji ze ztrony https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Indexing indeksowanie po wersji 3.0 zostało usunięte. Istnieją alternatywne opcje, które mogą działać podobnie do indeksowania:
# MAGIC 
# MAGIC - Widoki zmaterializowane(obiekt bazy danych zawierający wyniki zapytania)z automatycznym przepisywaniem mogą dawać bardzo podobne wyniki. - Widok zmaterializowany to widok, którego wyniki są fizycznie przechowywane i muszą być okresowo odświeżane, aby pozostały aktualne. 
# MAGIC 
# MAGIC - Używając kolumnowych formatów plików(Parquet, OFC), mogą wykonywać selektywne skanowanie

# COMMAND ----------

# MAGIC %md
# MAGIC **Zadanie 2**
# MAGIC 
# MAGIC Stwórz diagram draw.io pokazujący jak powinien wyglądać pipeline.
# MAGIC 
# MAGIC Wymysl kilka transfomracji, np. usunięcie kolumny, wyliczenie współczynnika dla x gdzie wartość do formuły jest w pliku 
# MAGIC referencyjnym
# MAGIC 
# MAGIC Wymagania:
# MAGIC Ilość źródeł: 7; 5 rodzajów plików, 2 bazy danych, 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Zadanie 3**
# MAGIC 
# MAGIC Napisz funkcję, która usunie dane ze wszystkich tabel w konkretniej bazie danych. Informacje 
# MAGIC pobierz z obiektu Catalog.

# COMMAND ----------

spark.catalog.listDatabases()

# COMMAND ----------

spark.sql(f"CREATE DATABASE  IF NOT EXISTS Sample")
spark.catalog.listDatabases()

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/movies.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filePath)
df.write.mode("overwrite").saveAsTable("Sample.movies")
filePath = "dbfs:/FileStore/tables/Files/actors.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filePath)
df.write.mode("overwrite").saveAsTable("Sample.actors")

# COMMAND ----------

spark.catalog.listTables("Sample")

# COMMAND ----------

df1=spark.sql("select * from Sample.movies limit 5")
display(df1)

# COMMAND ----------

df1=spark.sql("select * from Sample.actors limit 5")
display(df1)

# COMMAND ----------

def dropData(db) :
    Tnames = [table.name for table in spark.catalog.listTables(db)]
    for n in Tnames:
        spark.sql(f"DELETE FROM {db}.{n}")
        print(f"Data from {n} deleted")

        
dropData("Sample")

# COMMAND ----------

df1=spark.sql("select * from Sample.actors limit 5")
display(df1)#Query returned no results

# COMMAND ----------

df1=spark.sql("select * from Sample.movies limit 5")
display(df1)#Query returned no results

# COMMAND ----------

# MAGIC %md 
# MAGIC **Zadanie 4**
# MAGIC 
# MAGIC Stwórz projekt w IntelliJ z spakuj do go jar. Uruchom go w IntelliJ.
# MAGIC 
# MAGIC Uruchom w Databricks.
# MAGIC 
# MAGIC Aplikacja powinna wczytać plik tekstowy i wykonać kilka transformacji (np. dodaj kolumnę, zmień 
# MAGIC wartości kolumny ect).

# COMMAND ----------

# MAGIC %scala
# MAGIC import Lab7._

# COMMAND ----------

# MAGIC %scala 
# MAGIC main(Array("pp","jkj"))
