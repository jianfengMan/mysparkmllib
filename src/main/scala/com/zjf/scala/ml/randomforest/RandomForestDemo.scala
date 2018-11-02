package com.zjf.scala.ml.randomforest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/11/2
  */
object RandomForestDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/DecisionTree/"

    // 加载数据
    val data = MLUtils.loadLibSVMFile(sc, basePath + "sample_libsvm_data.txt")
    // 切分训练集和测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]() // 空categoricalFeaturesInfo表明所有特征是连续的
    val numTrees = 3 //树的个数
    val featureSubsetStrategy = "auto"
    val impurity = "gini" //决策树类型
    val maxDepth = 4
    val maxBins = 32
    // 训练随机森林模型
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // 评估模型，计算误差
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    /**
      * Test Error = 0.0
      * Learned classification forest model:
      * TreeEnsembleModel classifier with 3 trees
      *
      * Tree 0:
      * If (feature 377 <= 100.0)
      * If (feature 545 <= 91.0)
      * Predict: 0.0
      * Else (feature 545 > 91.0)
      * Predict: 1.0
      * Else (feature 377 > 100.0)
      * If (feature 374 <= 5.0)
      * Predict: 1.0
      * Else (feature 374 > 5.0)
      * Predict: 0.0
      * Tree 1:
      * If (feature 351 <= 4.0)
      * If (feature 185 <= 0.0)
      * If (feature 376 <= 0.0)
      * Predict: 0.0
      * Else (feature 376 > 0.0)
      * Predict: 1.0
      * Else (feature 185 > 0.0)
      * Predict: 0.0
      * Else (feature 351 > 4.0)
      * If (feature 509 <= 0.0)
      * Predict: 1.0
      * Else (feature 509 > 0.0)
      * Predict: 0.0
      * Tree 2:
      * If (feature 428 <= 0.0)
      * If (feature 397 <= 0.0)
      * Predict: 1.0
      * Else (feature 397 > 0.0)
      * Predict: 0.0
      * Else (feature 428 > 0.0)
      * Predict: 0.0
      */

  }
}
