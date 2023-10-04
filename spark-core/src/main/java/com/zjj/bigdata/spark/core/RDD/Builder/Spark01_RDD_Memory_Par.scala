package com.zjj.bigdata.spark.core.RDD.Builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Memory_Par
 *         #Date: 2023/10/2 16:04
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 创建RDD
    // RDD的并行度 & 分区
    // 第二个参数可以不传递，那么makeRDD方法会使用默认值：defaultParallelism
    // scheduler.conf.getInt("spark.default.parallelism",totalCores)
    // spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    // 如果获取不到，那么使用totalCores属性，这个值取决当前环境最大核数
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 将处理的数据保存为分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
