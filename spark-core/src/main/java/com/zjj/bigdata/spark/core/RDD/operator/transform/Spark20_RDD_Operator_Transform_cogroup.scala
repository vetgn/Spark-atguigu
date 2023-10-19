package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark20_RDD_Operator_Transform_cogroup {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("b", 4), ("c", 5)))
    val value = rdd1.cogroup(rdd2)
    value.collect().foreach(println)
    sc.stop()
  }
}
