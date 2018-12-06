package com.zjf.scala.ml.svd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * @Description:
  * A ~= U * S * V'
  * 降低A的储存和运算空间，提高效率
  * https://blog.csdn.net/wangpei1949/article/details/53191026 这篇文章不错
  * SVD的现实意义
  * 以下部分来自吴军老师的数学之美。
  * 如矩阵A100万∗50万A100万∗50万100万篇文章，每篇文章50万个特征，该矩阵的总元素有5000亿个，
  * 储存量和计算量非常大。如果用SVD做矩阵分解，A100万∗50万≈U100万∗100∗S100∗100∗V′50万×100A100万∗50万≈U100万∗100∗S100∗100∗V50万×100′，
  * 既把A近似的表示为3个矩阵U、S、V′U、S、V′，总元素不超过1.5亿，大大减少了储存量和计算量。
  * 也达到了降维的目的。
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-06
  */
object SVDExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

    val dataRDD = sc.parallelize(data, 2)
    val mat: RowMatrix = new RowMatrix(dataRDD)

    // 奇异值分解
    // def computeSVD(k: Int,computeU: Boolean = false,rCond: Double = 1e-9)
    // k：取top k个奇异值
    // computeU：是否计算矩阵U
    // rCond：小于1.0E-9d的奇异值会被抛弃
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U //U矩阵
    val s: Vector = svd.s //奇异值
    val V: Matrix = svd.V //V矩阵

    println(s)

  }

}
