package com.zjj.bigdata.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //5.处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //6.打印结果
    reducedStream.print()
    ssc.start()
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()

  }
}
