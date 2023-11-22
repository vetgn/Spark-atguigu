
package com.zjj.bigdata.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.lang.Thread

object SparkStreaming07_Close {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))

    wordToOne.print()

    ssc.start()

    // 如果想要关闭采集器，那么需要创建新的线程
    // 而且需要在第三方程序中增加关闭状态
    new Thread(
      new Runnable {
        override def run(): Unit = {
          // 优雅的关闭
          // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
          val state: StreamingContextState = ssc.getState() // 获取SparkStreaming状态
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
        }
      }
    )

    ssc.awaitTermination()

  }
}
