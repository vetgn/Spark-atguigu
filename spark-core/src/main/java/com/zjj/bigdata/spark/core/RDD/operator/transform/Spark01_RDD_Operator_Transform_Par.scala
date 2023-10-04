package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    // 1、rdd的计算一个分区內的数据是一个一个执行逻辑
    // 只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据
    // 分区內数的执行是有序的
    // 2、不同分区数据计算是无序的
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD1 = rdd.map(
      num => {
        println(">>>>" + num)
        num
      }
    )
    val mapRDD2 = mapRDD1.map(
      num => {
        println("####" + num)
        num
      }
    )
    mapRDD2.collect()
    sc.stop()
  }
}
