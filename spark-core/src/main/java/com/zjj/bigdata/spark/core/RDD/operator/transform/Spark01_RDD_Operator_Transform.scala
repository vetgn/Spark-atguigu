package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
