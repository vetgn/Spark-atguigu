package com.zjj.bigdata.spark.core.RDD.ACC

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_ACC
 *         #Date: 2023/10/12 14:08
 */
object Spark01_RDD_ACC {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ACC"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    /*
    * 如果单纯的只用sum进行累加，则diver会将sum发送到Executor端，但是Executor并不会将sum返回回来，
    * 因此sum便是0，而累加器便会返回
    * */
    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(
      num =>{
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)
    /*获取累加器的值
    少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    一般情况下，紧加器会放置在行动算子进行操作*/
    sc.stop()
  }
}
