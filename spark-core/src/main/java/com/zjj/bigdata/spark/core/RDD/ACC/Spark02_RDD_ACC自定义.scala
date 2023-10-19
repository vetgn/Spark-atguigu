package com.zjj.bigdata.spark.core.RDD.ACC

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author ZJJ
 *         #Description Spark01_RDD_ACC
 *         #Date: 2023/10/12 14:08
 */
object Spark02_RDD_ACC自定义 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ACC"))
    val rdd = sc.makeRDD(List("Hello", "World", "Hello", "Spark"))
    // 创建对象
    val myAccumulator = new MyAccumulator()
    // 注册
    sc.register(myAccumulator, "Wc")
    rdd.foreach(
      word => {
        myAccumulator.add(word)
      }
    )
    println(myAccumulator.value)
    sc.stop()
  }

  /*
  * 1、继承AccumulatorV2，定义泛型
  * IN：累加器输入数据类型
  * OUT：累加器返回的数据类型
  * */
  private class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap: mutable.Map[String, Long] = mutable.Map()

    // 累加器是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    // 重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 向累加器中添加数据
    override def add(v: String): Unit = {
      val newCnt = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v, newCnt)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = wcMap
      val map2 = other.value
      map2.foreach(
        {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
      )
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
