package com.zjf.scala.ml.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description:保序回归
  * @Author: zhangjianfeng
  * @Date: Created in 2018/11/7
  */
object IsotonicRegressionDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Regression/"

    val data: RDD[String] = sc.textFile(basePath + "sample_isotonic_regression_data.txt")
    val parseData: RDD[(Double, Double, Double)] = data.map { line =>
      val parts = line.split(",").map(_.toDouble)
      (parts(0), parts(1), 1.0)
    }

    /**
      * 样本数据划分训练样本与测试样本
      */
    val splits: Array[RDD[(Double, Double, Double)]] = parseData.randomSplit(Array(0.6, 0.4), seed = 1L)
    val training: RDD[(Double, Double, Double)] = splits(0)
    val test: RDD[(Double, Double, Double)] = splits(1)

    /**
      * 新建保序回归模型并训练
      *
      * boundaries: 边界数组，即分段函数X的分段点数组，边界数组按顺序存储
      * predictions: 对应边界数组的y值，即分段函数x的分段点对应的Y值
      *
      */
    val model: IsotonicRegressionModel = new IsotonicRegression()
      .setIsotonic(true) // 设置升序、降序参数
      .run(training) // 模型训练run方法

    // 取X
    val x: Array[Double] = model.boundaries

    // 取最终保序Y
    val y: Array[Double] = model.predictions
    println("boundaries" + "\t\t" + "predictions")
    for (i <- 0 to (x.length - 1))
      println(x(i) + "\t\t\t" + y(i))

    /**
      * 误差计算
      */
    val predictionAndLabel: RDD[(Double, Double)] = test.map { point =>
      val predictedLabel: Double = model.predict(point._2)
      (predictedLabel, point._1)
    }

    val print_predict: Array[(Double, Double)] = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1)
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)

    val meanSquaredError: Double = predictionAndLabel.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("mean squared error = " + meanSquaredError)

    /**
      * 保存模型
      */
    val modelPath = basePath + "IsotonicRegressionModel"
    model.save(sc, modelPath)

    /**
      * 加载模型
      */
    val load: IsotonicRegressionModel = IsotonicRegressionModel.load(sc, modelPath)
  }

}
