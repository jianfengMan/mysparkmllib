package com.zjf.scala.ml.bayes

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * @Description: 朴素贝叶斯，
  *               优点：过程简单速度快，预测就是粉刺后进行概率乘积，使用log变为做加法，
  *               概率大小只有在比较时才有意义，不能作为真正概率的大小
  *               缺点：对于测试集中的一个类别变量特征，如果在训练集里没有见过，直接算概率就是0
  *               适用于文本分类，过滤，情感等等
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/18
  */
object NaiveBayesDemo {
  def main(args: Array[String]): Unit = {
    //https://www.cnblogs.com/zhoulujun/p/8893393.html ,这里有几道题讲贝叶斯，有助于理解

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Bayes/"
    val data = sc.textFile(basePath + "data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 1L)
    val trainingData = splits(0)
    val testData = splits(1)
    //默认是多项式
    //    val model = NaiveBayes.train(trainingData, lambda = 1.0)
    /**
      * @param input     样本RDD,格式为RDD(label, features)
      * @param lambda    平滑参数
      * @param modelType 模型类型：多项式或者伯努利
      *                  multinomial 多项式 适合于训练集大到内存无法一次性放入的情况
      *                  bernoulli 伯努利 对于一个样本来说，其特征用的是全局的特征，每个特征的取值是布尔型
      */
    val model: NaiveBayesModel = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
    model.labels.foreach(println) //打印　label(labels是标签类别)
    model.pi.foreach(println) //打印先验概率　(pi存储各个label先验概率)


    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(
      label => label._1 == label._2).count()
    println(accuracy)
    val test = Vectors.dense(0, 0, 10)
    val result = model.predict(test)
    println(result) //2


    // 保存与加载模型
    val modelPath = basePath + "NaiveBayesModel"
    model.save(sc, modelPath)
    NaiveBayesModel.load(sc, modelPath)

  }

}
