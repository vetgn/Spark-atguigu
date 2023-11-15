
package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author ZJJ
 *         #Description Spark01_SparkSQL_Basic
 *         #Date: 2023/10/26 10:20
 */
object Spark08_SparkSQL_Test2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "zjj")
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    spark.sql("use atguigu");
    spark.sql(
      """
        |select
        |				a.*,
        |				p.product_name,
        |				c.area,
        |				c.city_name
        |			from user_visit_action a
        |			join product_info p on a.click_product_id = p.product_id
        |			join city_info c on a.city_id = c.city_id
        |			where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        |select
        |			area,
        |			product_name,
        |			count(*) as clickCnt,
        |   cityRemark(city_name) as city_remark
        |		from t1 group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |		*,
        |		rank() over( partition by area order by clickCnt desc ) as rank
        |	from t2
        |""".stripMargin).createOrReplaceTempView("t3")


    spark.sql(
      """
        |select
        |	*
        |from t3 where rank <=3;
        |""".stripMargin).show(false)
    spark.close()
  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  /*
  * 实现城市备注功能
  * 1、继承Aggregator
  *   IN：城市名称
  *   BUF：Buffer => 【总点击数量，Map[ (city,cnt) ]】
  *   OUT：备注信息
  * */
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    override def reduce(b: Buffer, a: String): Buffer = {
      b.total += 1
      val newCount = b.cityMap.getOrElse(a, 0L) + 1
      b.cityMap.update(a, newCount)
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total

      val map1 = b1.cityMap
      val map2 = b2.cityMap

      // 两个map合并操作
      b1.cityMap = map1.foldLeft(map2) {
        case (map, (city, cnt)) => {
          val newCount = map.getOrElse(city, 0L) + cnt
          map.update(city, newCount)
          map
        }
      }
      b1
    }

    override def finish(reduction: Buffer): String = {
      val remarkList = ListBuffer[String]()
      val totalcnt = reduction.total
      val cityMap = reduction.cityMap

      // 降序排序
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      val hasMore = cityMap.size > 2
      var rsum = 0L
      cityCntList.foreach {
        case (city, cnt) => {
          val r = cnt * 100 / totalcnt
          remarkList.append(s"${city}${r}%")
          rsum += r
        }
      }
      if (hasMore) {
        remarkList.append(s"其他${100 - rsum}%")
      }
      remarkList.mkString("，")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}