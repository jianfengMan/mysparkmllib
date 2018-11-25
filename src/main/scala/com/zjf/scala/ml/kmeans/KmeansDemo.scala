package com.zjf.scala.ml.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors

/**
  * @Description
  * @Author zhangjianfeng
  * @Date 2018-11-25
  **/
object KmeansDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Kmeans/"

    // 读取样本数据
    val data = sc.textFile(basePath + "kmeans_test.txt")

    // 转化数据格式
    val parsedData: RDD[linalg.Vector] = data.map { s =>
      Vectors.dense(s.split(" ").map(_.toDouble))
    }.cache()

    // 新建KMeans聚类模型，并训练
    val initMode = "k-means||"
    val numClusters = 2
    val numIterations = 20
    val model: KMeansModel = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(parsedData)

    // 调用模型预测
    val predict: Int = model.predict(Vectors.dense(9.0, 9.0, 9.0))
    println(s"predict : $predict") // predict : 1

    // 中心点
    val centers: Array[linalg.Vector] = model.clusterCenters
    println(centers.foreach(println(_))) // [0.1,0.1,0.1]
    //      [9.099999999999998,9.099999999999998,9.099999999999998]

    // 误差计算
    val WSSSE: Double = model.computeCost(parsedData)
    println(s"Whin Set Sum of Squared Errors = $WSSSE")
    // Whin Set Sum of Squared Errors = 0.11999999999994547

    // 保存模型
    model.save(sc, basePath + "kmeansmodel")
    KMeansModel.load(sc, basePath + "kmeansmodel")

    sc.stop()
  }

}
