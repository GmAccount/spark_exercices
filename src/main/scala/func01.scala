import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object func01 {
  //def pathExists(path: String): Boolean = ???
  def readData(path: String, fileType: String)(implicit sparkSession: SparkSession): Either[String, DataFrame] = {
    fileType match {
      case "CSV"     => Right(sparkSession.read.option("inferSchema", true).option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _         => Left("File format is not allowed")
    }
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import org.apache.spark.sql.functions._
    val maybeDataFrame = readData("/Users/lbadre/Documents/dir02/netflix_titles.csv", "CSV")
    val df = maybeDataFrame.right.get
    println(df.show(10))

    // question 1
    val dftvshow = df.filter("type == 'TV Show'")
    println(dftvshow.show(3))

    // question 2

    val correctedDf = df.withColumn("type",
      when(col("type").equalTo("Movie"), lit("Film"))
        .otherwise(col("type")))
    correctedDf.write.mode(SaveMode.Overwrite).option("header", true).csv("/Users/lbadre/Documents/dir02/netflix_titles_result")

    // 3
    df.createTempView("tabledf2")
    val dfFilmsUSA = correctedDf.filter("type == 'Film'").filter("country == 'United States'")
    println(dfFilmsUSA.show(4))




     // 3 3
    val df3 = df.sqlContext.sql("select count(*) from tabledf2")
    df3.show()

    //println(df.groupBy("release_year").agg(collect_list("release_year")))



    //df.filter("rule_id != ''")
    //println(df.where(col("type") == "TV Show").show(3))

    // println(df.show(10))
    /*
    val correctedDf = df.withColumn("pays_exporteur_corrige",
      when(col("pays_exporteur").equalTo("Allemagne"), lit("Italie"))
        .otherwise(col("pays_exporteur")))
    correctedDf.write.option("header", true).csv("result")

     */
  }
}