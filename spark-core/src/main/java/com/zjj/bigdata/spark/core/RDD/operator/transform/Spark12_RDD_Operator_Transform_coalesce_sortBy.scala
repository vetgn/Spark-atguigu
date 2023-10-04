package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark12_RDD_Operator_Transform_coalesce_sortBy {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[Int] = sc.makeRDD(List(2, 5, 1, 6, 3, 4), 2)
    // sortBy可根据规则进行排序，第二个参数默认true，为升序
    rdd.sortBy(num => num).collect().foreach(println)
    sc.stop()
  }
}
