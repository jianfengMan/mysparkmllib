package com.zjf.scala.ml.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description: 生成kmeans的数据
  * @Author: zhangjianfeng
  * @Date: Created in 2018-11-25
  */
object GenerateKmeanDataDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * 随机生成20个样本，数据维度为三维，聚类中心数为5，初始中心分布的缩放因子为1.0，RDD的分区数为1
      */
    val KMeansRDD: RDD[Array[Double]] = KMeansDataGenerator.generateKMeansRDD(sc, 20, 5, 3, 1.0, 1)

    println("KMeansRDD.count: " + KMeansRDD.count())
    val take: Array[Array[Double]] = KMeansRDD.take(5)
    take.foreach(_.foreach(println(_)))

    /**
      * KMeansRDD.count: 20
      * 2.2838106309461095
      * 1.8388158979655758
      * -1.8997332737817918
      * -0.6536454069660477
      * 0.9840269254342955
      * 0.19763938858718594
      * 0.24415182644986977
      * -0.4593305783720648
      * 0.3286249752173309
      * 1.8793621718715983
      * 1.4433606519575122
      * -0.9420612755690412
      * 2.7663276890005077
      * -1.4673057796056233
      * 0.39691668230812227
      */

  }
}
