package com.zjf.scala.ml.tools

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/11/2
  */
object GenerateSvmFileDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Tools/"
    val data = sc.textFile(basePath + "testlibsvm.txt")


    val libData: RDD[LabeledPoint] = data.map(line => {
      val fs = line.trim.split("\t")
      val label = fs(fs.length - 1).toDouble
      val features = new ArrayBuffer[Double]()
      for (feature <- fs) {
        features.append(feature.toDouble)
      }
      features.remove(features.length - 1)
      LabeledPoint(label, Vectors.dense(features.toArray))
    }).coalesce(1,true)
    MLUtils.saveAsLibSVMFile(libData, basePath + "tranlibsvm.libsvm")
  }

}
