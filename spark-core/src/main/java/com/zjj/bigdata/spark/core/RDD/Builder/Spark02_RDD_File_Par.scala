package com.zjj.bigdata.spark.core.RDD.Builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark02_RDD_File_Par
 *         #Date: 2023/10/3 10:23
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 从文件中创建RDD
    // 默认分区数量defaultMinPartitions: Int = math.min(defaultParallelism, 2)
//    val value = sc.textFile("datas/1.txt")

    // Spark读取文件，底层其实使用的就是hadoop的读取方式
    // https://www.bilibili.com/video/BV11A411L7CK/?p=38&spm_id_from=pageDriver&vd_source=afa7a8da6c5827a9f43477d9151881f7
    val value = sc.textFile("datas/1.txt",2)
    value.saveAsTextFile("output")
    sc.stop()
  }
}
