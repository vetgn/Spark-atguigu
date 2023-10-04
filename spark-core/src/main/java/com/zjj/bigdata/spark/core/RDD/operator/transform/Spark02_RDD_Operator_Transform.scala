package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // mapPartitions：可以以分区为单位进行数据转换操作
    //                但是会将整个分区的数据加载到内存进行引用，如果处理完的数据是不会被释放掉的，存在对象的应用，在内存较小，数量较大的场合下，容易出现内存溢出
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>")
        iter.map(_ * 2)
      }
    )
    sc.stop()
    sc.stop()
  }
}
