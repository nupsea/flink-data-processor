package org.nupsea.proc

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object DataStream {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val counts = data.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        value.toLowerCase.split("\\W+").filter(_.nonEmpty).foreach { word =>
          out.collect((word, 1))
        }
      }
    }).keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)

    counts.print( " -- " )

    env.execute("WordCount_with_Windowing")
  }
}
