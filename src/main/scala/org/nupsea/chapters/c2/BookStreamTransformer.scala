package org.nupsea.chapters.c2
import scala.collection.mutable.ArrayBuffer

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class BookStreamTransformer extends RichFlatMapFunction[String, BooksDTO]{
  override def flatMap(in: String, collector: Collector[BooksDTO]): Unit = {
    val books = BooksParser.parseCSVLine(in)
    books match {
      case Right(book) => collector.collect(book)
      case Left((err, line)) => println(err); println(line) // TODO Store erroneous records for later analysis.
      case _ => println("Error retrieving book")
    }
  }
}


object BooksParser {
  private val CSV_DIMENSION = 23

  def parseCSVLine(line: String): Either[(String, String), BooksDTO] = {

    var insideQuotes = false
    val buffer: StringBuilder = new StringBuilder()
    val fields = new ArrayBuffer[String]()

    for (c <- line) {
      c match {
        case '"' => insideQuotes = !insideQuotes
        case ',' =>
          if (insideQuotes) {
            buffer.append(c)
          } else {
            fields.append(buffer.toString())
            buffer.clear()
          }
        case _ => buffer.append(c)
      }
    }

    fields.append(buffer.toString())

    if (fields.length == CSV_DIMENSION) {
      try {
        Right(BooksDTO(
          id = fields(0).toInt,
          book_id = fields(1).toInt,
          authors = fields(7),
          original_publication_year = fields(8).toFloat.toInt,
          title = fields(10),
          language_code = fields(11),
          average_rating = fields(12).toFloat
        ))
      } catch {
        case e: Exception => Left(e.getMessage, s"ERR_LINE: $line")
      }

    } else
    {
      Left(
        (s"Found ${fields.length} fields. Expected $CSV_DIMENSION fields", s"ERR_LINE: $line")
      )
    }

  }
}
