package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ZJJ
 *         #Description Spark01_SparkSQL_Basic
 *         #Date: 2023/10/26 10:20
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    // TODO:创建SparkSQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // TODO:执行逻辑操作
    //    val df: DataFrame = spark.read.json("datas/user.json")
    //    df.show()

    /* DataFrame => SQL */
    //    df.createOrReplaceTempView("user")
    //    val sqlDF: DataFrame = spark.sql("select * from user")
    //    sqlDF.show()
    /* DataFrame => DSL */
    //    df.select('age).show()
    /* DataSet */
    //    val ints = List(1, 2, 3)
    //    val ds: Dataset[Int] = ints.toDS()
    /* RDD <=> DataFrame */
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lishi", 20)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df.rdd
    /* DataFrame <=> DataSet */
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()
    /* RDD <=> DataSet */
    val ds1: Dataset[User] = rdd.map(
      {
        case (i, str, i1) => User(i, str, i1)
      }
    ).toDS()
    val userRDD: RDD[User] = ds1.rdd
    // TODO:关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)
}
