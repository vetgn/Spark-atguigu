package com.zjj.bigdata.spark.core.RDD.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Action
 *         #Date: 2023/10/10 14:16
 */
object Spark02_RDD_Action_aggregate {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Action"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 10 +(13 + 17) = 40
    // aggregateByKey：初始值只会参与分区内计算
    // aggregate：初始值会参与分区内和分区间的计算
    println(rdd.aggregate(10)(_ + _, _ + _))
    println(rdd.fold(10)(_ + _))

  }
}
