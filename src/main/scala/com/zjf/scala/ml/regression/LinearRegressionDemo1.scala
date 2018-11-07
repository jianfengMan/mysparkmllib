package com.zjf.scala.ml.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/11/7
  */
object LinearRegressionDemo1 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Regression/"
    /**
      * 认识两种数据
      * 1、普通标签数据，数据格式为："标签，特征值1 特征值2...."
      * 2、LibSVM格式的数据，数据格式为："标签 特征ID:特征值 ...."
      */
    val data: RDD[String] = sc.textFile(basePath + "linear1.txt")
    val parsedData: RDD[LabeledPoint] = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

    //val parsedData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "sample_linear_regression_data.txt").cache()
    val numExamples: Long = data.count()

    /**
      * 新建线性回归模型，并设置训练参数
      */
    val model: LinearRegressionModel = LinearRegressionWithSGD.train(parsedData, 100, 1, 1.0)

    /**
      * 获取权重
      */
    val weights: linalg.Vector = model.weights
    println(s"weights: $weights")

    /**
      * weights: [0.5808575763272221,0.1893000148294698,0.2803086929991066,0.11108341817778758,
      * 0.4010473965597894,-0.5603061626684255,-0.5804740464000983,0.8742741176970946]
      * intercept: 0.0
      */

    /**
      * 获取偏置项
      */
    val intercept: Double = model.intercept
    println(s"intercept: $intercept")

    /**
      * 对样本进行测试
      */
    val predict: RDD[Double] = model.predict(parsedData.map(_.features))

    /**
      * 对结果进行zip拼接
      */
    val predictionAndLable: RDD[(Double, Double)] = predict.zip(parsedData.map(_.label))

    val print_predict: Array[(Double, Double)] = predictionAndLable.take(50)
    println("prediction" + "\t\t\t\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t\t" + print_predict(i)._2)
    }

    /**
      * 计算测试误差
      *
      */
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
    // training Mean Squared Error = 6.207597210613578

    // 模型保存
    val modelPath = basePath + "modelpath"
    model.save(sc, modelPath)

    // 加载模型
    val load: LinearRegressionModel = LinearRegressionModel.load(sc, modelPath)
  }

}
