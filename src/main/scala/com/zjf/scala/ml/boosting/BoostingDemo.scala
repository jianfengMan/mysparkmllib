package com.zjf.scala.ml.boosting

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession


/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/31
  */
object BoostingDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Boosting/"

    val data = MLUtils.loadLibSVMFile(sc, basePath + "sample_libsvm_data.txt")

    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    var boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(3)
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(5)


    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + model.toDebugString)


    println("data.count:" + data.count())
    println("trainingData.count:" + trainingData.count())
    println("testData.count:" + testData.count())
    println("model.algo:" + model.algo)
    println("model.trees:" + model.trees)
    println("model.treeWeights:" + model.treeWeights)

    println("labelAndPreds")
    labelAndPreds.take(10).foreach(println)

    sc.stop
  }
}
