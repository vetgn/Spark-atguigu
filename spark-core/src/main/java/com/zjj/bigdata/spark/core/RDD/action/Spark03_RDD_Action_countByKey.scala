package com.zjj.bigdata.spark.core.RDD.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Action
 *         #Date: 2023/10/10 14:16
 */
object Spark03_RDD_Action_countByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Action"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)))

    println(rdd.countByValue())
    println(rdd1.countByKey())
  }
}
