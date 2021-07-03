case class Voiture(marque: String, annee: Int, pays: String, age: Int)

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object Main {
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
    import sparkSession.implicits._
    //val dfFromRdd = sparkSession.createDataFrame(rddRow, schema)
    val schema = Encoders.product[Voiture].schema.add("_corrupt_record", StringType, true)
    val allDataset = sparkSession.read.schema(schema)
      .option("header", true).csv("voitures.csv").as[Voiture]
    allDataset.map(_.age + 2)
  }
}

