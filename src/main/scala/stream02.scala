import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
object stream02 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[4]").setAppName("Africa Data Lab")
    val ssc = new StreamingContext(conf, Seconds(2))
    val dstream = ssc.textFileStream("/Users/lbadre/spark/streaming_test")
    dstream.persist(StorageLevel.MEMORY_AND_DISK)
    dstream.foreachRDD(rdd => {
      //println(rdd.count())
      rdd.foreach(println)
      val result01 = rdd.flatMap (line => line.split(",") )
      val list01 = result01.collect().toList

      if (list01.length> 0 ) {
        val list02 = list01.map((s: String) => s.toInt)
        val res = list02.reduceLeft((x, y) => x + y)
        println(res)
      }

    })
    ssc.start()
    ssc.awaitTermination()
    //waiting for a condition
  }
}
