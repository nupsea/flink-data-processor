package org.nupsea.chapters.c2

import java.lang

case class BooksDTO(
                     id: Int,
                     book_id: Int,
                     authors: String,
                     original_publication_year: Int,
                     title: String,
                     language_code: String,
                     average_rating: Float
                   )
