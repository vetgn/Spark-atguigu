package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark14_RDD_Operator_Transform_partitionBy {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD = rdd.map((_, 1)) // partitionBy是k-v类型，需要转换
    // RDD => PairRDDFunctins
    // partitionBy并不是RDD的方法，通过隐式转换（二次编译）使得RDD可以调用

    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    sc.stop()
  }
}
