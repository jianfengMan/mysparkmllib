package com.zjf.scala.ml.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-04 
  */
object PCAOnSourceVectorExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val data: RDD[LabeledPoint] = sc.parallelize(Seq(
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))))

    // 计算前5个主成分
    val pca = new PCA(5).fit(data.map(_.features))

    val projected = data.map(p => p.copy(features = pca.transform(p.features)))


    val collect = projected.collect()
    println("Projected vector of principal component:")
    collect.foreach { vector => println(vector) }

    sc.stop()
  }
}
