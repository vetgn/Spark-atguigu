package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform_Test
 *         #Date: 2023/10/3 15:40
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))
    val rdd = sc.textFile("datas/apache.log")
    val mapRDD: RDD[String] = rdd.map(_.split(" ")(6))
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
