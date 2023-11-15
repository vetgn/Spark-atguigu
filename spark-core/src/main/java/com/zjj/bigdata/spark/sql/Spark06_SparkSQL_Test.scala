package com.zjj.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author ZJJ
 *         #Description Spark01_SparkSQL_Basic
 *         #Date: 2023/10/26 10:20
 */
object Spark06_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "zjj")
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    conf.set("spark.port.maxRetries", "128")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    spark.sql("use atguigu")
    // 1、准备数据
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/spark-sql/user_visit_action.txt' into table atguigu.user_visit_action;
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/spark-sql/product_info.txt' into table atguigu.product_info
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/spark-sql/city_info.txt' into table atguigu.city_info;
        |""".stripMargin)

    spark.sql("select * from city_info").show

    spark.close()
  }
}