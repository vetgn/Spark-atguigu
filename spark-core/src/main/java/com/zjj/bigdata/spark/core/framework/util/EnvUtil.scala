package com.zjj.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author ZJJ
 *         #Description EnvUtil
 *         #Date: 2023/10/19 20:42
 */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}
