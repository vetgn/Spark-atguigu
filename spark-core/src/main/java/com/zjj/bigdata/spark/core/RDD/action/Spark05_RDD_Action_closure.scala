package com.zjj.bigdata.spark.core.RDD.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description Spark01_RDD_Action
 *         #Date: 2023/10/10 14:16
 */
object Spark05_RDD_Action_closure {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Action"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User
    // 由于是在Executor端执行，需要从Driver中调用User，因此需要序列化
    //如果是 val rdd1 = sc.makeRDD(List[Int]())，那么数据个数为0，不会调用foreach，而方法有个闭包检测，外面数据没传，因此还会报错
    rdd.foreach(
      num => {
        println("age =" + (user.age + num))
      }
    )
    sc.stop()
  }

  class User extends Serializable {
    val age = 10
  }
}
