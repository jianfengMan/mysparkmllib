package com.zjf.scala.ml.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-04
  */
object PCAOnRowMatrixExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

    val rows = sc.parallelize(data)

    val mat: RowMatrix = new RowMatrix(rows)

    val pc: Matrix = mat.computePrincipalComponents(4)

    val projected: RowMatrix = mat.multiply(pc)

    val collect = projected.rows.collect()
    println("Projected Row Matrix of principal component:")
    collect.foreach { vector => println(vector) }

    sc.stop()
  }
}
