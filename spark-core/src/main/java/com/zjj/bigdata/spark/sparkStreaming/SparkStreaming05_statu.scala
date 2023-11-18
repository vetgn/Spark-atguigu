package com.zjj.bigdata.spark.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.OptionalInt

object SparkStreaming05_statu {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周期內的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据汇总
    // 使用有状态操作时，需要设定检查点路径
    val datas = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))

    //    val wordToCount = wordToOne.reduceByKey(_ + _)

    // updateStateByKey：根据key对数据的状态进行更新
    // 传递的参数中含有两个值
    // 第一个值表示相同的key的value数据
    // 第二个值表示缓存区相同的key的value数据
    val state = wordToOne.updateStateByKey(
      (seq: Seq[Int], opt: Option[Int]) => {
        val newcount = opt.getOrElse(0) + seq.sum
        Option(newcount)
      }
    )

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
