package com.zjf.scala.ml.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * * 数据降维
  * * 主成分分析PCA
  * * 设法将原来具有一定相关行（比如 P个指标）的指标
  * * 重新组合成一组新的互相无关的综合指标来代替原来的指标，从而实现数据降维的目的
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-04
  */
object PCADemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/PCA/"

    val data = sc.textFile(basePath + "sample_pca.txt")
      .map(_.split(" ").map(_.toDouble))
      .map(line => Vectors.dense(line))

    val rm = new RowMatrix(data)

    //提取主成分，设置主成分个数为３
    val pca = rm.computePrincipalComponents(3)

    //创建主成分矩阵
    val mx = rm.multiply(pca)

    mx.rows.foreach(println)
  }

}
