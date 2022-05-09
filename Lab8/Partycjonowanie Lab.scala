// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomiędzy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy żeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw"

val data = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

data.write.mode("overwrite").parquet("dbfs:/wiki.parquet")


// COMMAND ----------

spark.catalog.clearCache()
val p = "dbfs:/wiki.parquet"
val df = spark.read
  .parquet(p)
    //.coalesce(2)

df.explain
val countDF=df.count()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.repartition(8).count()

// COMMAND ----------

df.repartition(1).count()

// COMMAND ----------

df.repartition(7).count()

// COMMAND ----------

df.repartition(9).count()

// COMMAND ----------

df.repartition(16).count()

// COMMAND ----------

df.repartition(24).count()

// COMMAND ----------

df.repartition(96).count()

// COMMAND ----------

df.repartition(200).count()

// COMMAND ----------

df.repartition(4000).count()

// COMMAND ----------

// MAGIC %md 
// MAGIC **Czas działania dla repartition**
// MAGIC 
// MAGIC 8 - 0.9s
// MAGIC 
// MAGIC 1 - 0.9s
// MAGIC 
// MAGIC 7 - 0.9s
// MAGIC 
// MAGIC 9 - 0.9s
// MAGIC 
// MAGIC 16 - 0.9s
// MAGIC 
// MAGIC 96 - 1s
// MAGIC 
// MAGIC 200 - 2s
// MAGIC 
// MAGIC 4000 - 2min 

// COMMAND ----------

df.coalesce(6).count()

// COMMAND ----------

df.coalesce(5).count()

// COMMAND ----------

df.coalesce(4).count()

// COMMAND ----------

df.coalesce(3).count()

// COMMAND ----------

df.coalesce(2).count()

// COMMAND ----------

df.coalesce(1).count()

// COMMAND ----------

df.coalesce(200).count()//robi 8 partycji - trwa 2 sek

// COMMAND ----------

// MAGIC %md 
// MAGIC **Czas działania dla coalesce**
// MAGIC 
// MAGIC 6 - 1s
// MAGIC 
// MAGIC 5 - 1s
// MAGIC 
// MAGIC 4 - 1s
// MAGIC 
// MAGIC 3 - 1s
// MAGIC 
// MAGIC 2 - 1s
// MAGIC 
// MAGIC 1 - 0s
