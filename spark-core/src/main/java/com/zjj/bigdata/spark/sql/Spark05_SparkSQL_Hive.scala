package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author ZJJ
 *         #Description Spark01_SparkSQL_Basic
 *         #Date: 2023/10/26 10:20
 */
object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1.拷贝Hive-size.xml文件到classpath下
    // 2.启用HIve的支持
    // 3.增加对应的依赖关系
  spark.sql("show tables").show

    spark.close()
  }
}