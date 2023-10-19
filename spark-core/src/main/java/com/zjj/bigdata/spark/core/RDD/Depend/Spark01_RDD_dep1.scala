package com.zjj.bigdata.spark.core.RDD.Depend

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_dep1
 *         #Date: 2023/10/10 20:35
 */
object Spark01_RDD_dep1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Depend"))
    val rdd = sc.textFile("datas/word")
    println(rdd.toDebugString)
    println("============")
    val flatRdd = rdd.flatMap(_.split(" "))
    println(flatRdd.toDebugString)
    println("============")
    val mapRdd = flatRdd.map((_, 1))
    println(mapRdd.toDebugString)
    println("============")
    mapRdd.reduceByKey(_ + _).collect().foreach(println)
    sc.stop()
    /*
    * RDD 依赖关系
     这里所谓的依赖关系，其实就是两个相邻 RDD 之间的关系

    * RDD 窄依赖
     窄依赖表示每一个父(上游)RDD 的 Partition 最多被子（下游）RDD 的一个 Partition 使用，
     窄依赖我们形象的比喻为独生子女
    *
    * RDD 宽依赖
    宽依赖表示同一个父（上游）RDD 的 Partition 被多个子（下游）RDD 的 Partition 依赖，会
    引起 Shuffle，总结：宽依赖我们形象的比喻为多生。
    * */
  }
}
