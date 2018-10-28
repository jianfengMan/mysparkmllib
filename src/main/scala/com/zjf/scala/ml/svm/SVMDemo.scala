package com.zjf.scala.ml.svm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/28
  */
object SVMDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/SVM/"

    /**
      * 读取样本数据 格式为LibSVM
      */
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, basePath + "sample_libsvm_data.txt")

    /**
      * 将样本数据划分训练样本与测试样本
      */
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    /**
      * 新建SVM模型，并训练
      *
      * @param input             训练样本，格式为RDD(label, features)
      * @param numIterations     迭代次数
      * @param stepSize          每次迭代步长
      * @param regParam          正则化因子
      * @param miniBatchFraction 每次迭代参与计算的样本比例
      */
    val numIterations = 100
    val model: SVMModel = SVMWithSGD.train(training, numIterations)
    //默认配置
    //    train(input, numIterations, 1.0, 0.01, 1.0)

    /**
      * 对测试样本进行测试
      */
    val predictionAndLabel: RDD[(Double, Double)] = test.map { point =>
      val score: Double = model.predict(point.features)
      (score, point.label)
    }
    val print_predict: Array[(Double, Double)] = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1)
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)

    /**
      * prediction	label
      *1.0	1.0
      *1.0	1.0
      *0.0	0.0
      *1.0	1.0
      *0.0	0.0
      *0.0	0.0
      *1.0	1.0
      *1.0	1.0
      *1.0	1.0
      *0.0	0.0
      *1.0	1.0
      *1.0	1.0
      *0.0	0.0
      *1.0	1.0
      *0.0	0.0
      *0.0	0.0
      *1.0	1.0
      *1.0	1.0
      *0.0	0.0
      *0.0	0.0
      */

    /**
      * 误差计算
      */
    val accuracy: Double = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(s"area under ROC = $accuracy")

    /**
      * 保存与加载模型
      */
    val modelPath = basePath + "/SVMModel"
    model.save(sc, modelPath)
    // val load: SVMModel = SVMModel.load(sc, modelPath)

  }
}
