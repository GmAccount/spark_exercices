import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}



//import org.joda.time._
import java.util.Date
import java.text.SimpleDateFormat


import org.apache.spark.SparkConf


object blframe {

  def readData(path: String, fileType: String)(implicit sparkSession: SparkSession):
  Either[String, DataFrame] = {
    fileType match {
      case "CSV"     => Right(sparkSession.read.option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _         => Left(s"The file Type ${fileType} is not implemented yet")
    }
  }

  def pathExists(path: String): Boolean = ???
  def readData2(path: String, fileType: String)(implicit sparkSession: SparkSession):
  Either[Exception, DataFrame] = {
    pathExists(path) match {
      case true => fileType match {
        case "CSV"     => Right(sparkSession.read.option("header", true).csv(path))
        case "PARQUET" => Right(sparkSession.read.parquet(path))
        case _         => Left(new Exception("File format is not allowed"))
      }
      case false => Left(new Exception(s"The path ${path} does not exist"))
    }
  }

  def main(args: Array[String]) {



    val logFile = "/Users/lbadre/Documents/dir02/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")


    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val session = SparkSession.builder().master("local").getOrCreate()
    import org.apache.spark.sql.functions._

    val maybeDataframe = readData("file01.csv","csv")
    // val sc = session.sparkContext

//      val correctedDf = df.withColumn("pays_exporteur_corrige",
    //      when(col("pays_exporteur").equalTo("Allemagne"), lit("Italie"))
    //    .otherwise(col("pays_exporteur")))
    //    correctedDf.write.option("header", true).csv("result")

    //val sc = new SparkContext(conf)
    //val logData = sc.textFile(logFile, 2).cache()




    /*
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("==========================")
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    println("==========================")
     */
    /*
    val filmrdd = sc.textFile("/Users/lbadre/Documents/dir02/IMDb_ratings.csv")
    val entete = filmrdd.first()
    val rddWithoutHeader = filmrdd.filter(elem => elem!=entete)
    val columnsArray = rddWithoutHeader.map(x =>
      x.split(","))
    val filmrddSeamntic = columnsArray.map(x =>Film(x(0),
      x(1).toDouble,x(2).toInt,x(3).toDouble,x(4).toDouble,
      x(5).toInt,x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt))
    val filmsorted = filmrddSeamntic.sortBy(x =>
      (x.total_votes, false), ascending = false)
    filmsorted.take(10).foreach(println)
 */

    /*
    val movieRDD = sc.textFile("/Users/lbadre/Documents/dir02/IMDb_movies.csv")
    val header = movieRDD.first()
    val movieRDDWithoutHeader = movieRDD.filter(e => e!=header)



    val movieObjectRDD = movieRDDWithoutHeader.map(elem => (Movies2(elem.split(",")(0),elem.split(",")(5), elem.split(",")(7),elem.split(",")(8),elem.split(",")(9))))

    movieObjectRDD.filter(elem=>elem.director.equals("Ernst Lubitsch")).take(10).foreach(println)
  */



//    val df = session.read.option("header",true).csv("/Users/lbadre/Documents/dir02/IMDb_movies.csv")


/*
    val df = session.read.format("csv")
      .option("header",true)
      .option("delimiter",",")
      .option("quote","\"")
      .option("escape","\"")
      .load("/Users/lbadre/Documents/dir02/IMDb_movies.csv")

    df.printSchema()
*/
    //val dfFilred = df.filter($"director" === "Charles Tait" || $"director" === "Sidney Olcott")
    //display(dfFilred)
    println("Hiiiii")

  }
}