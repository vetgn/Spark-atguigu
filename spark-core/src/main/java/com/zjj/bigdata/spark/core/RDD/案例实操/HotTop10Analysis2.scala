package com.zjj.bigdata.spark.core.RDD.案例实操

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description HotTop10Analysis
 *         #Date: 2023/10/12 20:40
 */
object HotTop10Analysis2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotTop10Analysis"))

    // TODO: 存在大量的shuffle操作（reduceByKey）

    // 1、读取原始数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    // 2、将数据转换结构
    val flatRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD = flatRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    // 6、打印
    analysisRDD.sortBy(_._2, false).take(10).foreach(println)
    sc.stop()
  }
}
