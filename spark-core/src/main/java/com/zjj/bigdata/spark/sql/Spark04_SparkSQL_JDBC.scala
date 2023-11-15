package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager}

/**
 * @author ZJJ
 *         #Description Spark01_SparkSQL_Basic
 *         #Date: 2023/10/26 10:20
 */
object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/vetgn?serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "314545")
      .option("dbtable", "student")
      .load().show()

//    df.write.format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/vetgn?serverTimezone=UTC")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "314545")
//      .option("dbtable", "user1")
//      .save()

    spark.close()
  }
}