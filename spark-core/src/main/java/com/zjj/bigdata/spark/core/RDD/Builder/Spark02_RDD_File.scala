package com.zjj.bigdata.spark.core.RDD.Builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark02_RDD_File
 *         #Date: 2023/10/2 15:49
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 从文件中创建RDD
    val value = sc.textFile("datas/")
    value.foreach(println)
    sc.stop()
  }
}
