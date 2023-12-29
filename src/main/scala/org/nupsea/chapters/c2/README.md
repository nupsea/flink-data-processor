## Chapter 2

#### Premise 
To read the book data from a streaming source `[ kafka | kinesis ]` and write it to a sink `[ lake | warehouse | database ]`


#### Pre-req
* Setup Kafka and point to a Kafka cluster. 
* Create a topic `book_events` 


#### Run
* Publish the book-read events onto a topic (Run BooksProducer)
* Run C2StreamProcessor to read events from a topic, transform and write it to a File Sink.

