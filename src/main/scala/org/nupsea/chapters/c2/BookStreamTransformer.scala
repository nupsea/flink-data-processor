package org.nupsea.chapters.c2
import scala.collection.mutable.ArrayBuffer

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class BookStreamTransformer extends RichFlatMapFunction[String, BooksDTO] {

  override def flatMap(in: String, collector: Collector[BooksDTO]): Unit = {
    val books = BooksParser.parseCSVLine(in)
    books match {
      case Right(book) => collector.collect(book)
      case Left((err, line)) => println(err); println(line) // TODO Store erroneous records for later analysis.
      case _ => println("Error retrieving book")
    }
  }
}



