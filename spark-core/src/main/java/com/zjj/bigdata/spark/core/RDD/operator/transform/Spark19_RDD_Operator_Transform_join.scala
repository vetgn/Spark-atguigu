package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark19_RDD_Operator_Transform_join {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    // 相同key的会连接形成元组
    // 没有匹对的，不会显示
    // 多种相同的会依次匹配，可能会出现笛卡尔积
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("b", 4), ("c", 5), ("a", 6)))
    val value: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    value.collect().foreach(println)
    sc.stop()
  }
}
