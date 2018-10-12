package com.zjf.scala.ml.vectormatrix

import org.apache.spark.ml
import org.apache.spark.ml.linalg.{SparseVector, Vectors}

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/12
  */
object VectorDemo {
  def main(args: Array[String]): Unit = {
    val vector: ml.linalg.Vector = Vectors.dense(1.2, 1.1, 1.0)

    // 向量大小
    val size: Int = vector.size
    println(size) // 3

    // 向量转成数组
    val toArray: Array[Double] = vector.toArray
    for (i <- 0 until (toArray.length)) println(toArray(i)) // 1.2, 1.1, 1.0

    // 返回一个向量的Hash值
    val code: Int = vector.hashCode()
    println(code) // -1616093566


    // 活动项数
    val actives: Int = vector.numActives
    println(actives) // 3

    // 非零元素的数目
    val nonzeros: Int = vector.numNonzeros
    println(nonzeros) // 3


    // 转成稀疏向量
    val sparse: SparseVector = vector.toSparse
    println(sparse) // (3,[0,1,2],[1.2,1.1,1.0])

    //    密集和稀疏向量
    //
    //    一个向量(1.0,0.0,3.0)它有2中表示的方法
    //
    //    密集：[1.0,0.0,3.0]    其和一般的数组无异
    //
    //    稀疏：(3,[0,2],[1.0,3.0])     其表示的含义(向量大小，序号，值)   序号从0开始


    //    /向量压缩（自动转化成密集向量或者稀疏向量）
    val compressed: ml.linalg.Vector = vector.compressed
    println(compressed) // [1.2,1.1,1.0]

  }

}
