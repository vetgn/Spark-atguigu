package com.zjj.bigdata.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val data8 = ssc.socketTextStream("localhost", 8888)
    val data9 = ssc.socketTextStream("localhost", 9999)

    val map888: DStream[(String, Int)] = data8.map((_, 8))
    val map999: DStream[(String, Int)] = data9.map((_, 9))

    val joinDS: DStream[(String, (Int, Int))] = map888.join(map999)
    joinDS.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
