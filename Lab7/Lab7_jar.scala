import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}
import org.apache.spark.SparkFiles

object Lab7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-applikacja")
      .getOrCreate()

    val actorsUrl = "https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"

    spark.sparkContext.addFile(actorsUrl)

    val df = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("file:///" + SparkFiles.get("actors.csv"))
    val df2 = df.withColumn("category", upper(col("category")))
    df2.show(3)
  }
}