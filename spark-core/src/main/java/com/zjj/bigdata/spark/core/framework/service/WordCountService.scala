package com.zjj.bigdata.spark.core.framework.service

import com.zjj.bigdata.spark.core.framework.common.TService
import com.zjj.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author ZJJ
 *         #Description 服务层
 *         #Date: 2023/10/19 10:44
 */
class WordCountService extends TService{
  private val wordCountDao = new WordCountDao()

  def dataAnalysis() = {
    val lines = wordCountDao.readFile("D:\\Code\\Spark-atguigu\\datas\\word")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))

    // Spark可以将分组和聚合使用一个方法实现
    // reduceByKey：相同的key数据，可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordToCount.collect()
    array
  }
}
