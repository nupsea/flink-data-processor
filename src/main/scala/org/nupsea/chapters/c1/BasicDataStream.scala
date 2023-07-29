package org.nupsea.chapters.c1

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.{Logger, LoggerFactory}


object BasicDataStream {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  private def getBookData(env: StreamExecutionEnvironment): DataStream[BookDTO] = {
    val rawBooks = env.fromElements (
      (121, "Da Vinci Code", 22.2f),
      (32, "Simulacra and Simulation", 30.1f),
      (24, "Being Logical", 12.2f),
      (12, "The Fountainhead", 20.2f)
    )
    rawBooks.map(x => BookDTO(x._1, x._2, x._3))

  }

  private def priceInINR(books: DataStream[BookDTO],
                         conversion_rate: Float): DataStream[BookDTO] = {
    books.map(
      x => BookDTO(x.id, x.title, x.price * conversion_rate)
    )
  }

  private def filter_price_range(elements: DataStream[BookDTO],
                                 lower_range: Float,
                                 upper_range: Float): DataStream[BookDTO] = {
    elements.filter(
      x => x.price >= lower_range && x.price <= upper_range
    )
  }

  def main(args: Array[String]): Unit = {

    // Setup a streaming execution environment for Flink to run
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // Define a stream of book entries through tuples (Book ID, Title, Price)
    val books: DataStream[BookDTO] = getBookData(streamEnv)

    // Convert AUD to INR
    val AUD_TO_INR = 55.2f
    val booksINR: DataStream[BookDTO] = priceInINR(books, AUD_TO_INR)

    // Filter the books in range 1000 -> 1500 INR
    val MIN=1000
    val MAX=1500
    val filtered: DataStream[BookDTO] = filter_price_range(booksINR, MIN, MAX)

    logger.info(s"Results: ")
    filtered.print()

    // Execute the pipeline
    streamEnv.execute("Basic Data Stream in Scala")


  }

}
