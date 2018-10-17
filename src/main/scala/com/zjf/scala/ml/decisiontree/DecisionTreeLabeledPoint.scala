package com.zjf.scala.ml.decisiontree

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/16
  */
object DecisionTreeLabeledPoint {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/DecisionTree/"


    //样本数据
    //身高 体重  label 1:fat 0:thin
    val trainData: RDD[LabeledPoint] = sc.textFile(basePath + "train").map(line => {
      var datas = line.split(" ")
      LabeledPoint(datas(2).toDouble, Vectors.dense(datas(0).toDouble, datas(1).toDouble))
    })

    val testData: RDD[linalg.Vector] = sc.textFile(basePath + "test")
      .map(s => Vectors.dense(s.trim.split(" ").map(_.toDouble)))

    val algo:Algo.Algo = Algo.Classification
    val numClasses = 2 // 分类数量
    val impurity = Gini // 纯度计算方法
    val maxDepth = 5 // 树的最大高度 建议值5
    val maxBins = 32 // 用于分裂特征的最大划分数量 建议值32

    val model: DecisionTreeModel =  DecisionTree.train(trainData,algo,impurity,maxDepth,maxBins)

    println(model.toDebugString)
    /**
      * DecisionTreeModel classifier of depth 4 with 9 nodes
      * If (feature 0 <= 1.8)
      * If (feature 1 <= 50.0)
      * Predict: 0.0
      * Else (feature 1 > 50.0)
      * If (feature 0 <= 1.6)
      * Predict: 1.0
      * Else (feature 0 > 1.6)
      * If (feature 1 <= 60.0)
      * Predict: 0.0
      * Else (feature 1 > 60.0)
      * Predict: 1.0
      * Else (feature 0 > 1.8)
      * Predict: 0.0
      */

    val result: RDD[Double] = model.predict(testData)

    testData.foreach(line =>{
      val result = model.predict(line)
      println(line+"----"+result)
    })
  }


}
