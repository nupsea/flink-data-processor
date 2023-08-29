package org.nupsea.chapters.c2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object C2StreamProcessor {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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

    bookStream.print()

    env.execute("Flink kafka consumer")
  }

}
