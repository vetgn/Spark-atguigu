package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark08_RDD_Operator_Transform_sample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // sample算子需要传递三个参数
    // 1、抽取数据后是否将数据放回 true（放回）
    // 2、数据源中每条数据被抽取的概率
    // 3、抽取数据时随机算法的种子
    println(rdd.sample(
      false,
      0.4,
      1
    ).collect().mkString(","))
    sc.stop()
  }
}
