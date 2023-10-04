package com.zjj.bigdata.spark.core.wc

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description WordCount
 *         #Date: 2023/9/19 21:17
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 建立和Spark框架的连接
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    // 执行业务操作

    // 1、读取文件
    val lines: RDD[String] = sc.textFile("datas")
    // 2、将一行数据进行拆分
    // 扁平化：将整体拆分成个体的操作
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //    words.collect().foreach {
    //      print
    //    }
    //3、根据单词成组统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //    wordGroup.collect().foreach {
    //      print
    //    }
    //4、对数据进行转换
    val wordToCount = wordGroup.map {
      //      case (word, list) => {
      //        (word, list.size)
      //      }
      word => (word._1, word._2.size)
    }
    //5、采集结果
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // 关闭连接
    sc.close()
  }
}
