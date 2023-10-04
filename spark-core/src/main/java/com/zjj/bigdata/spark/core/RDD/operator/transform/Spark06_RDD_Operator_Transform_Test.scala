package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.status.api.v1.SimpleDateParam
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))
    // 得到apache.log每个时间段的访问数
    // 17/05/2015:10:05:03
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val value: RDD[(Int, Iterable[(Int, Int)])] = rdd.map(
      lines => {
        val datas: Array[String] = lines.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val date = sdf.parse(time)
        var hour = date.getHours
        (hour, 1)
      }
    ).groupBy(_._1)
    value.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
