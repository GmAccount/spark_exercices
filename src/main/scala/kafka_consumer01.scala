import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import java.lang
import java.util.Properties
object kafka_consumer01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    import scala.collection.JavaConversions._
    val kafkaConsumer = new KafkaConsumer[String,String](props)
    kafkaConsumer.subscribe(java.util.Arrays.asList("bl2-africa-datalab"))
    while(true) {
      val record: ConsumerRecords[String, String] = kafkaConsumer.poll(200)
      for (r <- record.iterator()) {
        println(r)
      }
    }
  }
}