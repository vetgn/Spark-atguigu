package com.zjj.bigdata.spark.sparkStreaming

import com.zjj.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.ResultSet
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

object SparkStreaming11_Req1_BlackList {
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

    val ds = adClickData.transform(
      rdd => {
        /* 通过JDBC周期性获取黑名单数据 */
        val blackList = ListBuffer[String]()

        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")

        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        conn.close()
        /* 判断用户是否在黑名单中 */
        val filterRDD = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )

        /* 如果用户不在黑名单中，那么进行统计数量（每个采集周期） */
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            /*
            *  为何使用foreachPartition？
            *   如果使用foreach——那么每次遍历都会创建一个conn，而数据库数据是不能序列化的
            *   因此使用foreachPartition，这样每个分区创建一个conn
            * */
            val conn = JDBCUtil.getConnection
            iter.foreach {
              case ((day, user, ad), count) => {
                println(s"${day} ${user} ${ad} ${count}")
                if (count >= 30) {
                  /* 如果统计数量超过点击阈值（30），那么将用户纳入黑名单 */

                  val sql =
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid=?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(conn, sql, Array(user, user))
                  conn.close()
                } else {
                  /* 如果没有超过阈值，那么需要将当天的广告点击数量进行更新 */
                  val conn = JDBCUtil.getConnection
                  val sql =
                    """
                      |select
                      | *
                      | from user_ad_count
                      | where dt = ? and userid = ? and adid = ?
                      |""".stripMargin
                  val flg = JDBCUtil.isExist(conn, sql, Array(day, user, ad))
                  // 查询统计表数据
                  if (flg) {
                    // 如果存在数据，那么更新
                    val sql =
                      """
                        |update user_ad_count
                        |set count = count +?
                        |where dt = ? and userid = ? and adid = ?
                        |""".stripMargin
                    JDBCUtil.executeUpdate(conn, sql, Array(count, day, user, ad))
                    /* 判断更新后的点击数量是否超过点击阈值，如果超过，那么将用户拉入黑名单 */
                    val sql2 =
                      """
                        |select
                        | *
                        | from user_ad_count
                        | where dt = ? and userid = ? and adid = ? and count >=30
                        |""".stripMargin
                    val flg1 = JDBCUtil.isExist(conn, sql2, Array(day, user, ad))
                    if (flg1) {
                      val sql3 =
                        """
                          |insert into black_list (userid) values (?)
                          |on DUPLICATE KEY
                          |UPDATE userid=?
                          |""".stripMargin
                      JDBCUtil.executeUpdate(conn, sql3, Array(user, user))
                    }
                  } else {
                    // 如果不存在数据，那么新增
                    val sql4 =
                      """
                        |insert into user_ad_count( dt,userid,adid,count ) values (?,?,?,?)
                        |""".stripMargin
                    JDBCUtil.executeUpdate(conn, sql4, Array(day, user, ad, count))
                  }
                  conn.close()
                }
              }
            }
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
