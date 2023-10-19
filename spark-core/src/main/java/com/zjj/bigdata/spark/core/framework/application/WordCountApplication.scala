package com.zjj.bigdata.spark.core.framework.application

import com.zjj.bigdata.spark.core.framework.common.TApplication
import com.zjj.bigdata.spark.core.framework.controller.WordCountController

/**
 * @author ZJJ
 *         #Description WordCountApplication
 *         #Date: 2023/10/19 10:42
 */
object WordCountApplication extends App with TApplication {
  start() {
    val wordCountController = new WordCountController()
    wordCountController.dispatch()
  }
}
