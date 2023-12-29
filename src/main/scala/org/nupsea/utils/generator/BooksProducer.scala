package org.nupsea.utils.generator

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.{BufferedReader, FileReader, InputStream, InputStreamReader}
import java.util.Properties
import scala.util.Random
import org.slf4j.{Logger, LoggerFactory}

object BooksProducer {

  private val TOPIC = "book_events"
  val logger: Logger = LoggerFactory.getLogger(getClass)


  def main(args: Array[String]):Unit = {

    // Kafka conf
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Read CSV
    val ioStream = getClass.getResourceAsStream("/data/books/books.csv")
    val bufferedReader = scala.io.Source.fromInputStream(ioStream)

    var count = 0
    for (line <- bufferedReader.getLines()) {
      if (count % 5 == 0) {
        val delay = Random.nextInt(10) * 1000 + 1000
        Thread.sleep(delay)
      }
      val record = new ProducerRecord[String, String](TOPIC, line)
      producer.send(record)
      logger.info(s" ** Record sent: ${record.value()}")
      count += 1
    }

    bufferedReader.close()
    producer.close()
  }

}
