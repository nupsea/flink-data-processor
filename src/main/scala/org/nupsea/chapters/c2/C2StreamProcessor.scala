package org.nupsea.chapters.c2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.{FSDataOutputStream, Path}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{CheckpointRollingPolicy, DefaultRollingPolicy}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.time.Duration
import java.util.Properties

object C2StreamProcessor {

  // Local File system write path
  val WRITE_PATH = "/Users/anup.sethuram/fs/DATA/DEV/dump"


  private def fetchBookSource(env: StreamExecutionEnvironment): DataStream[String] = {
    val consumerProps = new Properties()
    consumerProps.setProperty("bootstrap.servers", "localhost:9092")
    consumerProps.setProperty("group.id", "book_updates")

    val kafkaSource: KafkaSource[String] = KafkaSource
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setGroupId("c2_book_updates")
      .setTopics("book_events")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()

    val bookStream: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Books Kafka Source")
    bookStream
  }


  private def transformBookStream(bookStream: DataStream[String]): DataStream[BooksDTO] = {
    bookStream.flatMap(new BookStreamTransformer)
  }

  private def writeToFileSink(bookStream: DataStream[BooksDTO]): Unit = {
    val outputPath = new Path(WRITE_PATH)
    val sink: FileSink[BooksDTO] = FileSink
      //.forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
      .forRowFormat(outputPath, new BooksDTOEncoder)
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofMinutes(1))
          .withInactivityInterval(Duration.ofMinutes(5))
          .withMaxPartSize(MemorySize.ofMebiBytes(2))
          .build()
      )
      .build()

    // Add the sink to the data stream
    bookStream.sinkTo(sink)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bookStream: DataStream[String] = fetchBookSource(env)
    val trBookStream: DataStream[BooksDTO] = transformBookStream(bookStream)
    // trBookStream.print()  # Print to Console for debugging.
    writeToFileSink(trBookStream)
    env.enableCheckpointing(60 * 1000) // 1 minute
    env.execute("Flink_Kafka_To_FileSink")
  }
}
