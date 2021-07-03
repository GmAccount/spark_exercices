/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//import org.joda.time._
import java.util.Date
import java.text.SimpleDateFormat


import org.apache.spark.SparkConf

case class Film(imdb_title_id: String,
                weighted_average: Double,
                total_votes: Int,
                mean_vote: Double,
                median_vote: Double,
                votes_10: Int,
                votes_9: Int,
                votes_8: Int,
                votes_7: Int,
                votes_6: Int
               )

case class Movies2(
                    imdb_title_id: String,
                    genre: String,
                    country: String,
                    language: String,
                    director: String
                  )


object SimpleApp {

  def getDataByDirector(id1: String, id2: String): Boolean = {
    id1 == id2
  }

  def main(args: Array[String]) {
    val logFile = "/Users/lbadre/Documents/dir02/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")


    Logger.getLogger("org").setLevel(Level.OFF)
    val session = SparkSession.builder().master("local").getOrCreate()
    val sc = session.sparkContext



    //val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()


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


    val movieRDD = sc.textFile("/Users/lbadre/Documents/dir02/IMDb_movies.csv")
    val header = movieRDD.first()
    val movieRDDWithoutHeader = movieRDD.filter(e => e!=header)



    val movieObjectRDD = movieRDDWithoutHeader.map(elem => (Movies2(elem.split(",")(0),elem.split(",")(5), elem.split(",")(7),elem.split(",")(8),elem.split(",")(9))))

    movieObjectRDD.filter(elem=>elem.director.equals("Ernst Lubitsch")).take(10).foreach(println)

  }
}