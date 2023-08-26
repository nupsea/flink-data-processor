package org.nupsea.chapters.c2

import java.lang

case class BooksDTO(
                     id: int,
                     book_id: int,
                     authors: String,
                     original_publication_year: int,
                     title: String,
                     language_code: String,
                     average_rating: Float
                   )
