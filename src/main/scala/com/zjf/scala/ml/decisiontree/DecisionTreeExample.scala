package com.zjf.scala.ml.decisiontree

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/17
  */
object DecisionTreeExample extends App {

  val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/DecisionTree/"

  val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
  val sc = sparkSession.sparkContext
  val spam = sc.textFile(basePath + "ham/")
  val normal = sc.textFile(basePath + "spam/")
  val tf = new HashingTF(numFeatures = 10000)
  val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
  val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
  val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
  val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
  val data = positiveExamples.union(negativeExamples)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = "gini"
  val maxDepth = 5
  val maxBins = 32

  val model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo,
    impurity, maxDepth, maxBins)

  // Evaluate model on test instances and compute test error
  val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
  println("Test Error = " + testErr)
  println("Learned classification tree model:\n" + model.toDebugString)
}
