package org.nupsea.chapters.c2

import org.apache.flink.api.common.serialization.Encoder

import java.io.OutputStream
import java.nio.charset.StandardCharsets

class BooksDTOEncoder extends Encoder[BooksDTO] {

  override def encode(element: BooksDTO, stream: OutputStream): Unit = {
    val str = s"${element.id},${element.title},${element.authors},${element.average_rating}\n"
    stream.write(str.getBytes(StandardCharsets.UTF_8))
  }
}
