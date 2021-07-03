import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object func02 {
  //def pathExists(path: String): Boolean = ???
  def readData(path: String, fileType: String)(implicit sparkSession: SparkSession): Either[String, DataFrame] = {
    fileType match {
      case "CSV" => Right(sparkSession.read.option("inferSchema", true).option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _ => Left("File format is not allowed")
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    //Catalyst Optimizer
    //val maybeDataFrame = readData("/Users/lbadre/Documents/dir02/voitures.csv", "CSV")
    val rdd: RDD[String] = sc.textFile("/Users/lbadre/Documents/dir02/voitures.csv")
    val rddRow: RDD[Row] = rdd.map(e => Row(e.split(",")(0), e.split(",")(1).toInt, e.split(",")(2), e.split(",")(3).toInt))

    //rddRow.collect()
    rddRow.take(2).foreach(println)
    val schema = new StructType()
      .add(StructField("marque", StringType))
      .add(StructField("annee", IntegerType))
      .add(StructField("pays", StringType))
      .add(StructField("age", IntegerType))
    val dfFromRdd = sparkSession.createDataFrame(rddRow, schema)
    dfFromRdd.printSchema()
    //dfFromRdd.show()
  }
}
