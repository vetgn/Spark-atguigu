package com.zjj.bigdata.spark.core.RDD.案例实操

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description HotTop10Analysis
 *         #Date: 2023/10/12 20:40
 */
object HotTop10Analysis2_1 {
  // 需求 2：Top10 热门品类中每个品类的 Top10 活跃 Session 统计
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotTop10Analysis")
    conf.set("spark.port.maxRetries", "1000")
    val sc = new SparkContext(conf)

    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    val top10Ids: Array[String] = top10Category(actionRdd)

    // 1、过滤原始数据，保留点击和前10品类ID
    val filterActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )

    // 2、根据品类ID和sessionid进行点击量的统计
    val reduceRDD = filterActionRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 3、将统计结果进行结构转换
    val mapRDD = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 4、相同的品类进行分组
    val groupRDD = mapRDD.groupByKey()

    // 5、将分组后的数据进行点击量的排序，取前10名
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRDD.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRdd: RDD[String]) = {
    val flatRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD = flatRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
