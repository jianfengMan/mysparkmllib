package com.zjf.scala.ml.mr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-06
  */
object WordCount extends App {

  val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
  val sc = sparkSession.sparkContext

  Logger.getRootLogger.setLevel(Level.WARN)

  val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/MR/"
  val result = sc.textFile(basePath + "wordcount.txt")
    .flatMap(_.split(" ")).map((_, 1L)).reduceByKey(_ + _)

  result.foreach(println)

  //关闭和Spark的连接
  sc.stop()
}
