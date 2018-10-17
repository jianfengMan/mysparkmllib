package com.zjf.scala.ml.decisiontree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/16
  */
object DecisionTreeDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/DecisionTree/"

    // libsvm格式数据   https://blog.csdn.net/qiao1245/article/details/45030469

    // 读取样本数据（libSVM格式）
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, basePath + "sample_libsvm_data.txt")
    // 切分训练集和测试集
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val sampleData = trainingData.take(10)
    sampleData.foreach(sample =>
      println(sample.features)
    )


    val numClasses = 2 // 分类数量
    val categoricalFeaturesInfo = Map[Int, Int]() // 用map存储类别（离散）特征及每个类别特征对应值（类别）的数量
    val impurity = "gini" // 纯度计算方法
    val maxDepth = 5 // 树的最大高度 建议值5
    val maxBins = 32 // 用于分裂特征的最大划分数量 建议值32


    //新建决策树
    val model: DecisionTreeModel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // 误差计算
    val labelAndPreds: RDD[(Double, Double)] = testData.map { point =>
      val prediction: Double = model.predict(point.features)
      (point.label, prediction)
    }


    val print_predict: Array[(Double, Double)] = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1){
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)

    }


    /**
      * label	prediction
      * 0.0	0.0
      * 1.0	1.0
      * 0.0	0.0
      * 0.0	0.0
      * 1.0	1.0
      * 0.0	1.0
      * 1.0	1.0
      * 0.0	0.0
      * 0.0	0.0
      * 1.0	1.0
      * 1.0	1.0
      * 0.0	0.0
      * 1.0	1.0
      * 1.0	1.0
      * 0.0	0.0
      * 1.0	1.0
      * 0.0	0.0
      * 1.0	1.0
      * 1.0	1.0
      * 1.0	1.0
      */


    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("learned classification tree model:\n" + model.toDebugString)

    /**
      * learned classification tree model:
      * DecisionTreeModel classifier of depth 2 with 5 nodes
      * If (feature 406 <= 20.0)
      * If (feature 99 <= 0.0)
      * Predict: 0.0
      * Else (feature 99 > 0.0)
      * Predict: 1.0
      * Else (feature 406 > 20.0)
      * Predict: 1.0
      */

    //保存模型
    val modelPath = basePath + "DecisionTreeModel"
    model.save(sc, modelPath)

    //加载模型
    val loadModel = DecisionTreeModel.load(sc, modelPath)

    // 误差计算
    val labelAndPreds1: RDD[(Double, Double)] = testData.map { point =>
      val prediction: Double = loadModel.predict(point.features)
      (point.label, prediction)
    }


    val print_predict1: Array[(Double, Double)] = labelAndPreds1.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict1.length - 1)
      println(print_predict1(i)._1 + "\t" + print_predict1(i)._2)

    val testErr1 = labelAndPreds1.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("learned classification tree model:\n" + loadModel.toDebugString)
  }


}
