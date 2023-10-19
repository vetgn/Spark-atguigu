package com.zjj.bigdata.spark.core.framework.controller

import com.zjj.bigdata.spark.core.framework.common.TController
import com.zjj.bigdata.spark.core.framework.service.WordCountService

/**
 * @author ZJJ
 *         #Description 控制层
 *         #Date: 2023/10/19 10:43
 */
class WordCountController extends TController{
  private val wordCountService = new WordCountService()

  def dispatch(): Unit = {
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
