import org.apache.log4j.{Level, Logger}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object kafka_producer01 {
      def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
        val record = new ProducerRecord[String, String]("bl2-africa-datalab",
          "hello adl ...")
        println("Sending record: " + record.value())
        kafkaProducer.send(record)
        println("record sent")
        kafkaProducer.close()
        println("closing producer")
      }
}