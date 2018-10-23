package com.zjf.scala.ml.logisticregression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description: 胃癌转移判断
  *               https://my.oschina.net/sunmin/blog/719742
  *
  *               肾细胞癌转移情况(有转移 y=1,无转移 y=2)
  *               x1:确诊时患者年龄(岁)
  *               x2:肾细胞癌血管内皮生长因子(VEGF),其阳性表述由低到高共３个等级
  *               x3:肾细胞癌组织内微血管数(MVC)
  *               x4:肾癌细胞核组织学分级，由低到高共４级
  *               x5:肾细胞癌分期，由低到高共４级
  *
  *               y x1 x2 x3 x4 x5
  *               0 59 2 43.4 2 1
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/23
  */
object LogisticRegression {

  // lr 本质求最大似然估计的值，转换为代价函数的最小值

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/LogisticRegression/"

    val data = MLUtils.loadLibSVMFile(sc, basePath + "wa.txt")

    // 样本数据划分训练样本和测试样本
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.7, 0.3), seed = 1L)
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    // 新建逻辑回归模型，并训练
    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // 对测试样本进行测试
    val predictionAndLabels: RDD[(Double, Double)] = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val print_predict: Array[(Double, Double)] = predictionAndLabels.take(20)
    println("prediction" + "\t\t\t\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t\t\t" + print_predict(i)._2)
    }

    //另一种验证方式
    val metrics = new MulticlassMetrics(predictionAndLabels)
    //创建验证类
    val precision = metrics.precision //计算验证值
    println("Precision = " + precision) //打印验证值

    val patient = Vectors.dense(Array(25, 2, 94.6, 4, 3)) //计算患者可能性
    print(model.predict(patient))

  }
}
