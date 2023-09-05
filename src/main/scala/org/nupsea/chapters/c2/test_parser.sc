import org.nupsea.chapters.c2.BooksDTO

import scala.collection.mutable.ArrayBuffer

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


BooksParser.parseCSVLine("2,3,3,4640799,491,439554934,9.78043955493e+12,\"J.K. Rowling, Mary GrandPré\",1997.0,Harry Potter and the Philosopher's Stone,\"Harry Potter and the Sorcerer's Stone (Harry Potter, #1)\",eng,4.44,4602479,4800065,75867,75504,101676,455024,1156318,3011543,https://images.gr-assets.com/books/1474154022m/3.jpg,https://images.gr-assets.com/books/1474154022s/3.jpg")
BooksParser.parseCSVLine("abc,2,3,3,4640799,491,439554934,9.78043955493e+12,\"J.K. Rowling, Mary GrandPré\",1997.0,Harry Potter and the Philosopher's Stone,\"Harry Potter and the Sorcerer's Stone (Harry Potter, #1)\",eng,4.44,4602479,4800065,75867,75504,101676,455024,1156318,3011543,https://images.gr-assets.com/books/1474154022m/3.jpg,https://images.gr-assets.com/books/1474154022s/3.jpg")
BooksParser.parseCSVLine("\".\",3,3,4640799,491,439554934,9.78043955493e+12,\"J.K. Rowling, Mary GrandPré\",1997.0,Harry Potter and the Philosopher's Stone,\"Harry Potter and the Sorcerer's Stone (Harry Potter, #1)\",eng,4.44,4602479,4800065,75867,75504,101676,455024,1156318,3011543,https://images.gr-assets.com/books/1474154022m/3.jpg,https://images.gr-assets.com/books/1474154022s/3.jpg")
