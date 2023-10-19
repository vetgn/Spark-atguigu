package com.zjj.bigdata.spark.core.RDD.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Action
 *         #Date: 2023/10/10 14:16
 */
object Spark01_RDD_Action {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Action"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
    println(rdd.reduce(_ + _))

    // 在驱动程序中，以数组 Array 的形式返回数据集的所有元素
    rdd.collect().foreach(println)

    // 返回 RDD 中元素的个数
    println(rdd.count())

    // 返回 RDD 中的第一个元素
    println(rdd.first())

    // 返回一个由 RDD 的前 n 个元素组成的数组
    rdd.take(3).foreach(println)

    // 返回该 RDD 排序后的前 n 个元素组成的数组
    val rdd1 = sc.makeRDD(List(3, 2, 4, 1))
    rdd1.takeOrdered(3).foreach(println)

    sc.stop()
  }
}
