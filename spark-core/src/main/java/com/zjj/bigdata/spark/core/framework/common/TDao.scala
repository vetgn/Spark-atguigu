package com.zjj.bigdata.spark.core.framework.common

import com.zjj.bigdata.spark.core.framework.util.EnvUtil

/**
 * @author ZJJ
 *         #Description TDao
 *         #Date: 2023/10/19 20:39
 */
trait TDao {
  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
