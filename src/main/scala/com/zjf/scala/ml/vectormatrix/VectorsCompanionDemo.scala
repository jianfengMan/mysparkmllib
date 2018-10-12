package com.zjf.scala.ml.vectormatrix

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors


/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/12
  */
object VectorsCompanionDemo {
  def main(args: Array[String]): Unit = {

    // 创建密集向量类
    val dense: linalg.Vector = Vectors.dense(1.0, 2.0, 3.0)
    println(dense) // [1.0,2.0,3.0]

    // 创建稀疏向量类(参数含义： 数据个数，下标，value)
    val sparse: linalg.Vector = Vectors.sparse(4, Array(0, 1, 3, 4), Array(1.0, 2.0, 3.0, 4.0))
    println(sparse)

    // 创建稀疏向量类(参数含义: 向量大小，向量元素Seq((下标， value),()...))
    val sparse1: linalg.Vector = Vectors.sparse(5, Seq((1, 1.0), (2, 3.0), (4, 5.0)))
    println(sparse1) // (5,[1,2,4],[1.0,3.0,5.0])

    // 创建0向量
    val zeros: linalg.Vector = Vectors.zeros(3)
    println(zeros) // [0.0,0.0,0.0]

    // 求向量的p范数
    val norm: Double = Vectors.norm(dense, 2.0)
    println(norm) // 3.7416573867739413

    //    向量范数
    //    1-范数：，即向量元素绝对值之和，matlab调用函数norm(x, 1) 。
    //
    //    2-范数：，Euclid范数（欧几里得范数，常用计算向量长度），即向量元素绝对值的平方和再开方，matlab调用函数norm(x, 2)。
    //
    //    ∞-范数：，即所有向量元素绝对值中的最大值，matlab调用函数norm(x, inf)。
    //
    //    -∞-范数：，即所有向量元素绝对值中的最小值，matlab调用函数norm(x, -inf)。
    //
    //    p-范数：，即向量元素绝对值的p次方和的1/p次幂，matlab调用函数norm(x, p)。
    //    ---------------------
    //    原文：https://blog.csdn.net/left_la/article/details/9159949?utm_source=copy


    //    矩阵范数
    //    1-范数：， 列和范数，即所有矩阵列向量绝对值之和的最大值，matlab调用函数norm(A, 1)。
    //
    //    2-范数：，谱范数，即A'A矩阵的最大特征值的开平方。matlab调用函数norm(x, 2)。
    //
    //    ∞-范数：，行和范数，即所有矩阵行向量绝对值之和的最大值，matlab调用函数norm(A, inf)。
    //
    //    F-范数：，Frobenius范数，即矩阵元素绝对值的平方和再开平方，matlab调用函数norm(A, ’fro‘)。


    val dense1 = Vectors.dense(1.0, 2.0, 3.0)
    val dense2 = Vectors.dense(1.0, 1.0, 1.0)


    // 求向量之间的平方距离
    val sqdist: Double = Vectors.sqdist(dense1, dense2)
    println(sqdist) // 5.0


    // 求向量之间的平方距离（向量为稀疏矩阵和密集矩阵）
    val sv: linalg.Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 1.0))
    val dv: linalg.Vector = Vectors.dense(1.0, 1.0, 1.0)
    val sqdist1 = Vectors.sqdist(sv, dv)
    println(sqdist1) // 1.0


    // 向量相等
    println(Vectors.dense(1.0, 2.0, 3.0).equals(Vectors.dense(1.0, 2.0, 3.0))) // true
  }

}
