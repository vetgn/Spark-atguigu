package com.zjj.bigdata.spark.sparkStreaming

import com.zjj.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object SparkStreaming12_Req2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka")
    sparkConf.set("spark.port.maxRetries", "128")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

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
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val pstat = conn.prepareStatement(
              """
                |insert into area_city_ad_count ( dt, area, city, adid, count )
                |values ( ?, ?, ?, ?, ? )
                |on DUPLICATE KEY
                |UPDATE count = count + ?
                |""".stripMargin)
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, ad)
                pstat.setInt(5, sum)
                pstat.setInt(6, sum)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

  // 广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
