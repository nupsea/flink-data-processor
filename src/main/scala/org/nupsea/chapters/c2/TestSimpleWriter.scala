package org.nupsea.chapters.c2

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

import java.time.Duration

object TestSimpleWriter {

  val WRITE_PATH = "/Users/anup.sethuram/fs/DATA/DEV/test_dump"

  def main(args: Array[String]): Unit = {
    // Set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create a simple data stream
    val textStream = env.fromElements("Hello", "World", "Flink", "Streaming")

    // Define the file sink
    val outputPath = new Path(WRITE_PATH)
    val sink: FileSink[String] = FileSink
      .forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofMinutes(15))
          .withInactivityInterval(Duration.ofMinutes(5))
          .withMaxPartSize(MemorySize.ofMebiBytes(128)) // 128 MB
          .build()
      )
      .build()

    // Add the sink to the data stream
    textStream.sinkTo(sink)

    // Execute the Flink job
    env.execute("Simple File Writing Job")
  }
}