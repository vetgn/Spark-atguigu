package com.zjj.bigdata.spark.core.RDD.Persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_persist
 *         #Date: 2023/10/11 20:34
 */
object Spark01_RDD_persist {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Persist"))
    val rdd = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRdd = flatRdd.map(word => {
      println("$$$$$$$$")
      (word, 1)
    })

    // RDD是不储存数据的，但为什么groupByKey还能调用？——因为又从头执行，导致代码减少但性能没有减少
    //mapRdd.cache() // 这时只要存储到mapRdd的数据就行，这就叫持久化
    // 默认情况cache使用的是内存存储，而persist能够更改存储级别
    mapRdd.persist(StorageLevel.DISK_ONLY)

    mapRdd.reduceByKey(_ + _).collect().foreach(println)

    println("*******************")

    val value = mapRdd.groupByKey()
    value.collect().foreach(println)

    sc.stop()
  }
}
