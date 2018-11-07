package com.zjf.scala.ml.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/11/6
  */
object LinearRegressionDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Regression/"

    val data = sc.textFile(basePath +"linear.txt")
    val parsedData = data.map(line =>{
      val parts = line.split("\\|")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(',').map(_.toDouble)))
    }).cache()

    //LabeledPoint,　numIterations, stepSize
    val model = LinearRegressionWithSGD.train(parsedData, 2, 0.1)
    val result = model.predict((Vectors.dense(2,3)))
    println(model.weights)
    println(model.intercept)
    println(model.weights.size)
    println(result)	//打印预测结果
  }

}
