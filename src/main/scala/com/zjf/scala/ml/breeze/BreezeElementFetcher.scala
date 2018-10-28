package com.zjf.scala.ml.breeze

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/29 
  */
object BreezeElementFetcher {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val a: DenseVector[Int] = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(a(0))  // 1

    println(a(1 to 4))  // DenseVector(2, 3, 4, 5)

    println(a(5 to 0 by -1))  // DenseVector(6, 5, 4, 3, 2, 1)

    println(a(1 to -1))  // DenseVector(2, 3, 4, 5, 6, 7, 8, 9, 10)

    println(a(-1))  // 10  -1表示最后一个元素

    val matrix: DenseMatrix[Double] = DenseMatrix((1.0, 2.0, 3.0), (3.0, 4.0, 5.0))
    println(matrix)
    /**
      * 1.0  2.0  3.0
      * 3.0  4.0  5.0
      */

    println(matrix(0, 1))   // 2.0
    println(matrix(::, 1))  // DenseVector(2.0, 4.0)  // 矩阵指定列

  }
}
