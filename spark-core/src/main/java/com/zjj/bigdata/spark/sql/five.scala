package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col, count, max, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.postfixOps

/**
 * @author ZJJ
 *         #Description five
 *         #Date: 2023/10/31 14:54
 */
object five {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd = spark.sparkContext.textFile("datas/1.txt").map {
      line => {
        val datas: Array[String] = line.split("\t")
        Moive(datas(0), datas(1), datas(2).toInt, datas(3))
      }
    }
    val df: DataFrame = rdd.toDF()
    df.createOrReplaceTempView("Ass")
    spark.sql("SELECT user_id,AVG(`rank`) from Ass GROUP BY user_id").show()
//    spark.sql("SELECT moive_id,AVG(`rank`) from Ass GROUP BY moive_id").show()

//    df.where($"rank" >= df.agg(round(avg("rank"), 2)).first().get(0)).show()

//    val userID = df.where('rank > 3).groupBy('user_id).count().withColumnRenamed("count", "cnt").orderBy(col("cnt").desc).first()(0)
//    df.filter(df("user_id") === userID).select(round(avg("rank"), 2)).show()

//    df.groupBy("user_id").agg(round(avg("rank"), 2).alias("avg"), min("rank").alias("min"), max("rank").alias("max")).show(100)

//    df.groupBy("moive_id").agg(round(avg("rank"), 2).alias("avg"), count("moive_id").alias("cnt")).where("cnt >1").show()
//    spark.close()
  }

  case class Moive(user_id: String, moive_id: String, rank: Int, ts: String)
}
