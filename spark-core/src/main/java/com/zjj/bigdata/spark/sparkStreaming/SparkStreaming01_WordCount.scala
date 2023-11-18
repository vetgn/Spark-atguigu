package com.zjj.bigdata.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO:创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第一个参数表示环境配置
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 逻辑处理

    /* 通过监控端口创建 DStream，读进来的数据为一行行 */
    val lineStreams = ssc.socketTextStream("localhost", 9999)

    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))
    val mapStreams: DStream[(String, Int)] = wordStreams.map((_, 1))
    val reduce: DStream[(String, Int)] = mapStreams.reduceByKey(_ + _)
    reduce.print()

    //由于SparkStreaming采集器是长期执行的任务,所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    // 1、启动采集器
    ssc.start()

    // 2、等待采集器的关闭
    ssc.awaitTermination()

  }
}
