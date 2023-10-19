package com.zjj.bigdata.spark.core.RDD.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Action
 *         #Date: 2023/10/10 14:16
 */
object Spark04_RDD_Action_foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action")
    conf.set("spark.port.maxRetries", "1000")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // foreach 是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)

    println("**************")
    // foreach 是Executor端内存数据打印
    rdd.foreach(println)

    /*
    * 算子：Operator（操作）
    *     RDD的方法和Scala集合对象的方法不一样
    *     集合对象的方法都是在同一个节点的内存中完成的。
    *     RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    *     为了区分不同的处理效果，所以将RDD的方法称之为算子。
    *     RDD的方法外部的操作都是在Driver端执行的,而方法内部的逻辑代码是在Executor端执行。
    *
    * */
    sc.stop()
  }
}
