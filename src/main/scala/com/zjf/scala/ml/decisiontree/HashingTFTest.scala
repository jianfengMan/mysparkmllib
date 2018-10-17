package com.zjf.scala.ml.decisiontree

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

object HashingTFTest extends  App {

  val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/DecisionTree/"

  val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
  val sc = sparkSession.sparkContext
  val spam = sc.textFile(basePath + "spam/0006.2003-12-18.GP.spam.txt")
  val normal = sc.textFile(basePath + "ham/0001.1999-12-10.farmer.ham.txt")
  val tf = new HashingTF(numFeatures = 10000)
  val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
  spamFeatures.foreach(println(_))


  println("---------------------------------------------------------------------")
  val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
  val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
  positiveExamples.foreach(println(_))


  val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
  val data = positiveExamples.union(negativeExamples)


}
