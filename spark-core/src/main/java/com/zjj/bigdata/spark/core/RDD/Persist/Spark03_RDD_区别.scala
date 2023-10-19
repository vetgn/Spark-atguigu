package com.zjj.bigdata.spark.core.RDD.Persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_persist
 *         #Date: 2023/10/11 20:34
 */
object Spark03_RDD_区别 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Persist"))
    val rdd = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRdd = flatRdd.map(word => {
      println("$$$$$$$$")
      (word, 1)
    })

    // cache：将数据临时存储在内存中进行数据重用
    //        会在血缘关系中添加新的依赖。一旦出现问题，可以重头读取数据
    // persist :将数据临时存储在磁盘文件中进行数据重用
    //          涉及到磁盘IO，性能较低，但是数据安全如果作业执行完毕，
    //          如果作业执行完毕，临时保存的数据文件就会丢失
    // checkpoint：将数据长久地保存在磁盘文件中进行数据重用
    //          涉及到磁盘IO，性能较低，但是数据安全
    //          为了保证数据安全，所以一般情况下，会独立执行作业
    //          为了能够提高效率，一般情况下，是需要和cache联合使用
    //          执行过程中，会切断血缘关系。重新建立新的血缘关系
    //          checkpoint等同于改变数据源

    mapRdd.cache()
    mapRdd.checkpoint()
    mapRdd.reduceByKey(_ + _).collect().foreach(println)
    println("*******************")
    val value = mapRdd.groupByKey()
    value.collect().foreach(println)
    sc.stop()
  }
}
