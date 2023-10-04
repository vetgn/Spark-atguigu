package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark09_RDD_Operator_Transform_distinct {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    // map(x => (x,null)).reduceByKey((x,_) => x, numPartitions.map(_._1))
    // 这是rdd的distinct()方法
    // List的distinct方法使用的是哈希表
    rdd.distinct().collect().foreach(println)
    sc.stop()
  }
}
