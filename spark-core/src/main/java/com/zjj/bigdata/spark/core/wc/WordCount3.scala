package com.zjj.bigdata.spark.core.wc

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ZJJ
 *         #Description WordCount3
 *         #Date: 2023/9/24 15:21
 */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))

    // Spark可以将分组和聚合使用一个方法实现
    // reduceByKey：相同的key数据，可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)
//    wordToCount.saveAsTextFile("output")
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    sc.close()
  }
}
