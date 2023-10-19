package com.zjj.bigdata.spark.core.framework.common

import com.zjj.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZJJ
 *         #Description TApplication
 *         #Date: 2023/10/19 13:36
 */
trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparConf)
    EnvUtil.put(sc)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
