package com.zjj.bigdata.spark.core.RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Operator_Transform
 *         #Date: 2023/10/3 15:23
 */
object Spark21_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator_Transform"))
    /*
    * 1) 数据准备
    agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    2) 需求描述
    统计出每一个省份每个广告被点击数量排行的 Top3
    * */
    // 1、获取原始数据
    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    // 2、数据转换 => （（省份，广告），1）
    val mapRDD: RDD[((String, String), Int)] = rdd.map(
      lines => {
        val datas: Array[String] = lines.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    // 3、转换后的数据进行聚合 （（省份，广告），sum）
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    // 4、聚合数据转换 （省份，（广告，sum）
    val newRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) => (prv, (ad, sum))
    }
    // 5、转换结构后的数组根据省份进行分组 （省份，【广告，sum】【】）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newRDD.groupByKey()
    // 6、分组后的数据排序（降序），取前3名
    val result = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    // 7、采集
    result.collect().foreach(println)
    sc.stop()
  }
}
