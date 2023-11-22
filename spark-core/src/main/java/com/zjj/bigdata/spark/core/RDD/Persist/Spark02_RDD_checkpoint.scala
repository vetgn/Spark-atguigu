package com.zjj.bigdata.spark.core.RDD.Persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_persist
 *         #Date: 2023/10/11 20:34
 */
object Spark02_RDD_checkpoint {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Persist"))
    sc.setCheckpointDir("datas/cp")
    val rdd = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRdd = flatRdd.map(word => {
      println("$$$$$$$$")
      (word, 1)
    })
    // checkpoint需要落盘，需要指定检查点保存路径
    // 检查点路径保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式存储系统：HDFS
    mapRdd.checkpoint()
    mapRdd.reduceByKey(_ + _).collect().foreach(println)

    println("*******************")

    val value = mapRdd.groupByKey()
    value.collect().foreach(println)

    sc.stop()
  }
}
