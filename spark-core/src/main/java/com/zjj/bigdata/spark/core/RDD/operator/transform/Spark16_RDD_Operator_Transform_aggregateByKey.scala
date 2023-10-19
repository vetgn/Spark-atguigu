package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark16_RDD_Operator_Transform_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    // 取【1,2】，【3,4】中最大数（分区内）
    // 最大数相加=【6】（分区间）
    // 使用reduceByKey则不行，因为其分区内和分区间是同一个方法

    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表，需要传递一个参数，表示为初始值
    //      主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第一个参数表示分区间计算规则
    /*
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      _ + _
    ).collect().foreach(println)
    */

    // aggregateByKey最终返回的数据结果类型应与初始值类型一致
    // 取相同key的平均值
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
    rdd1.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    ).mapValues {
      case (num, n) => {
        num / n
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
