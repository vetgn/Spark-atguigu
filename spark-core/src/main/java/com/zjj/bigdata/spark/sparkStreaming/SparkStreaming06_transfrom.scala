package com.zjj.bigdata.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_transfrom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)

    // transform方法可以将底层RDD获取到后进行操作
    // 1、DStream功能不完善
    // 2、需要代码周期性执行

    // Code：Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        // Code：Driver端，（周期性执行）
        rdd.map(
          str => {
            // Code：Executor端
            str
          }
        )
      }
    )

    // Code：Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Code：
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
