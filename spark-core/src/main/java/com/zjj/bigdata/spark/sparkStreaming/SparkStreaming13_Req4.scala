package com.zjj.bigdata.spark.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming13_Req4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "zjj",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //4.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))
    val adClickData = kafkaDStream.map(
      kafkaData => {
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val reduceDS = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTS = ts / 10000 * 10000
        (newTS, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(60), Seconds(10))

    //    reduceDS.print()
    reduceDS.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()
        val datas = rdd.sortByKey(true).collect()
        datas.foreach {
          case (time, cnt) => {

            val timeString = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
            list.append(s"""{"xtime":"${timeString}","yval":"${cnt}"}""")
          }
        }
        val out = new PrintWriter(new FileWriter(new File("D:\\Code\\Spark-atguigu\\datas\\adclick\\adclick.json")))

        out.println("[" + list.mkString(",") + "]")
        out.flush()
        out.close()
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

  // 广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
