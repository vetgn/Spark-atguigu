package com.zjj.bigdata.spark.core.RDD.案例实操

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description HotTop10Analysis
 *         #Date: 2023/10/12 20:40
 */
object HotTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotTop10Analysis"))

    // TODO: actionRdd重复使用、cogroup性能较低

    // 1、读取原始数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    actionRdd.cache()
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
    val rdd1 = clickCountRDD.map(
      {
        case (cid, cnt) => {
          (cid, (cnt, 0, 0))
        }
      }
    )
    val rdd2 = orderCountRDD.map(
      {
        case (cid, cnt) => {
          (cid, (0, cnt, 0))
        }
      }
    )
    val rdd3 = payCountRDD.map(
      {
        case (cid, cnt) => {
          (cid, (0, 0, cnt))
        }
      }
    )
    // 将三个数据聚合在一起，统一进行聚合计算
    val source: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val analysisRDD = source.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    // 6、打印
    analysisRDD.sortBy(_._2, false).take(10).foreach(println)
    sc.stop()
  }
}
