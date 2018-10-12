package com.zjf.scala.ml.vectormatrix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/12
  */
object RowMatrixDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd: RDD[Array[Double]] = sc.parallelize(
      Array(
        Array(1.0, 2.0, 3.0, 4.0),
        Array(2.0, 3.0, 4.0, 5.0),
        Array(3.0, 4.0, 5.0, 6.0)
      ))

    val rdd1: RDD[linalg.Vector] = rdd.map(f => Vectors.dense(f))
    rdd1.foreach(println(_))

    println("---------------华丽的分隔符----------------")
    //创建实例
    val RM: RowMatrix = new RowMatrix(rdd1)
    RM.rows.foreach(println(_))
    /**
      * [2.0,3.0,4.0,5.0]
      * [3.0,4.0,5.0,6.0]
      * [1.0,2.0,3.0,4.0]
      */


    // 计算每列之间的相似度(抽样)
    val simic1: CoordinateMatrix = RM.columnSimilarities(0.5)
    println("************** simic1 *************")
    simic1.entries.foreach(println(_))

    /**
      * MatrixEntry(0,1,1.0151337142356327)
      * MatrixEntry(1,3,1.4066276648667395)
      * MatrixEntry(0,2,1.3196738285063225)
      * MatrixEntry(0,3,0.7105935999649429)
      * MatrixEntry(2,3,1.1541560327111708)
      * MatrixEntry(1,2,1.3705602888445152)
      **/


    // 计算每列之间的相似度
    val simic2: CoordinateMatrix = RM.columnSimilarities()
    println("************** simic2 *************")
    simic2.entries.foreach(println(_))

    /**
      * ************** simic2 *************
      * MatrixEntry(0,3,0.9746318461970762)
      * MatrixEntry(1,3,0.9946115458726394)
      * MatrixEntry(2,3,0.9992204753914715)
      * MatrixEntry(1,2,0.9979288897338914)
      * MatrixEntry(0,2,0.9827076298239907)
      * MatrixEntry(0,1,0.9925833339709303)
      */


    // 计算每列的统计汇总
    val simic3: MultivariateStatisticalSummary = RM.computeColumnSummaryStatistics()
    println("********* computeColumnSummaryStatistics *********")
    println(simic3.max) // [3.0,4.0,5.0,6.0]
    println(simic3.min) // [1.0,2.0,3.0,4.0]
    println(simic3.mean) // [2.0,3.0,4.0,5.0]


    // 计算每列之间的协方差，生成协方差矩阵
    // 协方差是样本与均值差值的乘积
    val cc1: Matrix = RM.computeCovariance()
    println("********* computeCovariance *********")
    println(cc1)

    /**
      * 1.0  1.0  1.0  1.0
      * 1.0  1.0  1.0  1.0
      * 1.0  1.0  1.0  1.0
      * 1.0  1.0  1.0  1.0
      */


    /**
      * ？？？ PCA，基于特征分解
      * 主成分分析计算
      * 取前k个主要变量，其结果矩阵的行为样本，列为变量
      */
    val cpc1: Matrix = RM.computePrincipalComponents(3)
    println("********* computePrincipalComponents *********")
    println(cpc1)
    /**
      * -0.5000000000000002  0.8660254037844388    1.6653345369377348E-16
      * -0.5000000000000002  -0.28867513459481275  0.8164965809277258
      * -0.5000000000000002  -0.28867513459481287  -0.40824829046386296
      * -0.5000000000000002  -0.28867513459481287  -0.40824829046386296
      */

    //    https://www.cnblogs.com/LeftNotEasy/archive/2011/01/19/svd-and-applications.html
    // 计算矩阵的奇异值分解
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = RM.computeSVD(4, true)
    println("********* SVD *********")
    svd.U.rows.foreach(println(_))
    println("svd.s"+ svd.s)
    println(svd.V)
    /**
      * [-0.5647271138038177,0.12006923114663204,1.1175870895385742E-8,0.0]
      * [-0.7117812888625867,-0.5715774052203693,-7.450580596923828E-9,-2.9802322387695312E-8]
      * [-0.4176729387450486,0.8117158675136333,3.3527612686157227E-8,3.3527612686157227E-8]
      *
      */


    val B: Matrix = Matrices.dense(4, 3, Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
    val multiply: RowMatrix = RM.multiply(B)
    multiply.rows.foreach(println(_))
    /**
      * [10.0,10.0,10.0]
      * [14.0,14.0,14.0]
      * [18.0,18.0,18.0]
      */

    val rows: Long = RM.numRows()
    println(rows)    // 3

    val cols: Long = RM.numCols()
    println(cols)    // 4

    // 矩阵转化成RDD
    RM.rows.foreach(println(_))


  }

}
