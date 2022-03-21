// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val start=System.currentTimeMillis();
val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val stop=System.currentTimeMillis();
val notebookTime=stop-start
val newDataFrame=namesDf.withColumn("exec_time",lit(notebookTime))
display(newDataFrame.select("exec_time"))

// COMMAND ----------

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
val newDataFrame = namesDf
  .withColumn("feet",($"height"*0.0328).as("feet"))
display(newDataFrame.select("feet"))
newDataFrame.printSchema

// COMMAND ----------

//Odpowiedz na pytanie jakie jest najpopularniesze imię?
val arrName=namesDf.select(split(col("name")," ").as("NameArray")).select(expr("NameArray[0]"))
val popularName = arrName.groupBy("NameArray[0]").count().orderBy(desc("count")).limit(1)
display(popularName)
//Most popular name is John!

// COMMAND ----------

//Dodaj kolumnę i policz wiek aktorów

val df=namesDf
   .select( $"name",$"date_of_birth",$"date_of_death",
   when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull,
      to_date(col("date_of_birth"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_birth"),"MM.dd.yyyy").isNotNull,
      to_date(col("date_of_birth"),"MM.dd.yyyy"))
    .when(to_date(col("date_of_birth"),"dd.MM.yyyy").isNotNull,
      to_date(col("date_of_birth"),"dd.MM.yyyy"))
    .otherwise("Unknown Format").as("Formated Birth Date"))
val newDf=df.withColumn("date_of_birth",$"Formated Birth Date")

val df2=newDf
   .select( $"name",$"date_of_birth",$"date_of_death",
   when(to_date(col("date_of_death"),"yyyy-MM-dd").isNotNull,
      to_date(col("date_of_death"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_death"),"MM.dd.yyyy").isNotNull,
      to_date(col("date_of_death"),"MM.dd.yyyy"))
    .when(to_date(col("date_of_death"),"dd.MM.yyyy").isNotNull,
      to_date(col("date_of_death"),"dd.MM.yyyy"))
    .otherwise("Unknown Format").as("Formated Death Date"))
val newDf2=df2.withColumn("date_of_death",$"Formated Death Date").drop($"Formated Death Date")
//display(newDf2.select("*"))

val res=newDf2.withColumn("Age",
months_between(
  col("date_of_death"),
  col("date_of_birth")
)/12)
display(res.select("*"))

//this solution doesn't include date format for example "1947 in Mexico" and age people who still alive - there is null..  

// COMMAND ----------

//Usuń kolumny (bio, death_details)
val newDataFrame=namesDf.drop("bio","death_details")
newDataFrame.printSchema()

// COMMAND ----------

//Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
val temp=namesDf.columns.map(c=>c.replace("_"," ")
                             .split(' ').map(_.capitalize)
                             .mkString(" ").replace(" ","") )
val colNames = namesDf.toDF(temp:_*)
colNames.printSchema

// COMMAND ----------

//Posortuj dataframe po imieniu rosnąco
val newDataFrame=namesDf.sort(asc("name"))
display(newDataFrame)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val start=System.currentTimeMillis();
val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val stop=System.currentTimeMillis();
val notebookTime=stop-start
val newDataFrame=moviesDf.withColumn("exec_time",lit(notebookTime))
display(newDataFrame.select("exec_time"))

// COMMAND ----------

display(moviesDf)

// COMMAND ----------

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
val years=moviesDf
  .withColumn("years after publishing",year(current_date())-$"year")
display(years.select("year","years after publishing"))

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
val cleanedDF = moviesDf.withColumn("budget",
  regexp_replace($"budget", "[^0-9]*", "").cast("int"))//without $ and NOK
cleanedDF.printSchema
display(cleanedDF.select("budget"))

// COMMAND ----------

//Usuń wiersze z dataframe gdzie wartości są null
val df= moviesDf.na.drop()
println("before: "+moviesDf.count)
println("after: "+df.count)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val start=System.currentTimeMillis();
val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val stop=System.currentTimeMillis();
val notebookTime=stop-start
val df=ratingsDf.withColumn("exec_time",lit(notebookTime))
display(df.select("exec_time"))

// COMMAND ----------

//Dla każdego z poniższych wyliczeń nie bierz pod uwagę nulls
val df_NotNull= ratingsDf.na.drop()
println("before: "+ratingsDf.count)
println("after: "+df_NotNull.count)

// COMMAND ----------

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
val votes = df_NotNull.columns.toList.slice(5,15)
val votes_column = df_NotNull.select(votes.map(c => col(c)): _*)
//display(votes_column)
val mean = df_NotNull.withColumn("mean 1 to 10", votes_column.columns.map(x => col(x)).reduce((x1, x2) => x1 + x2) / 10)
display(mean.select("mean_vote","mean 1 to 10","weighted_average_vote"))

// COMMAND ----------

//Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
val diff = df_NotNull.withColumn("diff_mean_weighted_avg",round(abs($"mean_vote"-$"weighted_average_vote"),2))
            .withColumn("diff_median_weighted_avg",round(abs($"median_vote"-$"weighted_average_vote"),2))
display(diff.select("weighted_average_vote","mean_vote","diff_mean_weighted_avg","median_vote","diff_median_weighted_avg"))

// COMMAND ----------

//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
val best = df_NotNull.select("males_allages_avg_vote","females_allages_avg_vote").agg(avg("males_allages_avg_vote"),avg("females_allages_avg_vote")).show()
//Girls give higher ratings 

// COMMAND ----------

//Dla jednej z kolumn zmień typ danych do long
val column = df_NotNull.withColumn("total_votes",col("total_votes").cast("long"))
display(column.select("total_votes"))

// COMMAND ----------

// MAGIC %md **ZADANIE 2**
// MAGIC * Przejdź przez Spark UI i opisz w kilku zdaniach co można znaleźć w każdym z elementów Spark UI.

// COMMAND ----------

// MAGIC %md SPARK UI
// MAGIC * Jobs - wyświetla informacje o wszystkich zadaniach w klastrze oraz szczegóły dotyczące tych zadań
// MAGIC   * User - Użytkownik
// MAGIC   * Total Uptime - Całkowity czas pracy
// MAGIC   * Scheduling Mode - Kolejność wykonywania zadań
// MAGIC   * Completed Jobs - Liczba zadań na status - Aktywne, Ukończone, Nieudane
// MAGIC   * Event Timeline - Oś czasu zdarzenia: wyświetla w porządku chronologicznym zdarzenia związane z wykonawcami (dodane, usunięte) i zadaniami
// MAGIC   * Completed jobs - Wyświetla szczegółowe informacje o zadaniach - id zadania, opis, przesłany czas, czas trwania, podsumowanie etapów i pasek postępu zadań
// MAGIC * Stages -  pokazuje bieżący stan wszystkich etapów wszystkich zadań
// MAGIC   * Stages for All Jobs - Podsumowanie liczby wszystkich etapów według statusu
// MAGIC   * Fair Scheduler Pools - właściwości pul, zgrupowanie zadań w pule
// MAGIC   * Completed stages - Podsumowanie wykonanych etapoów
// MAGIC * Storage - mówi nam o utrwalonych danych, rozmiarach danych
// MAGIC * Environment - wyświetla wartości różnych zmiennych środowiskowych i konfiguracyjnych
// MAGIC   * Runtime Information - wyświetla wersje Javy i Sparka
// MAGIC   * Spark Properties - zawiera listę właściwości aplikacji, takich jak „spark.app.name” i „spark.driver.memory”.
// MAGIC * Executors - podsumowanie informacji o executorach, które zostały utworzone dla aplikacji, w tym wykorzystanie pamięci i dysku oraz informacje o zadaniach i losowaniu.
// MAGIC * SQL - wyświetla informacje, takie jak czas trwania, zadania oraz plany fizyczne i logiczne dla zapytań które zostały użyte w trakcje działania aplikacji
// MAGIC   * JDBC/ODBC Server - Pokazuje informacje o sesjach i przesłanych operacjach SQL.
// MAGIC * Structured Streaming - wyświetla kilka krótkich statystyk dotyczących uruchomionych i zakończonych zapytań

// COMMAND ----------

// MAGIC %md **ZADANIE 3**
// MAGIC * Do jednej z Dataframe dołóż transformacje groupBy i porównaj jak wygląda plan wykonania 

// COMMAND ----------

val withoutGrBy = df_NotNull.select("imdb_title_id", "mean_vote").explain()
val withGrBy = df_NotNull.select("imdb_title_id", "mean_vote").groupBy("imdb_title_id").count().explain()

// COMMAND ----------

// MAGIC %md **ZADANIE 4**
// MAGIC * Pobierz dane z SQL Server

// COMMAND ----------

var username = "sqladmin"
var password = "$3bFHs56&o123$" 
val data = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable","(SELECT * FROM SalesLT.ProductModel) as x")
      .option("user", username)
      .option("password",password)
      .load()

display(data)
