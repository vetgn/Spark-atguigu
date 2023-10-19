package com.zjj.bigdata.spark.core.RDD.案例实操

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description HotTop10Analysis
 *         #Date: 2023/10/12 20:40
 */
object HotTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotTop10Analysis"))

    // 1、读取原始数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    // 2、统计点击数量
    val clickActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    // 3、下单数量
    val orderActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)
    // 4、支付数量
    val payActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)
    // 5、排序，取前10名
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val value = cogroupRDD.mapValues(
      {
        case (clickIter, orderIter, payIter) => {
          var clickCnt = 0
          val iter1 = clickIter.iterator
          if (iter1.hasNext) {
            clickCnt = iter1.next()
          }
          var orderCnt = 0
          val iter2 = orderIter.iterator
          if (iter2.hasNext) {
            orderCnt = iter2.next()
          }
          var payCnt = 0
          val iter3 = payIter.iterator
          if (iter3.hasNext) {
            payCnt = iter3.next()
          }
          (clickCnt, orderCnt, payCnt)
        }
      }
    )
    // 6、打印
    value.sortBy(_._2, false).take(10).foreach(println)
    sc.stop()
  }
}
