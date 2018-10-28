package com.zjf.scala.ml.breeze

import breeze.linalg.DenseVector

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/29
  */
object BreezeBooleanFunction {
  def main(args: Array[String]): Unit = {
    val a: DenseVector[Boolean] = DenseVector(true, false, true)
    val b: DenseVector[Boolean] = DenseVector(false, true, true)

    // 与
    val vector1: DenseVector[Boolean] = a :& b
    println(vector1)    // DenseVector(false, false, true)

    // 或
    val vector2: DenseVector[Boolean] = a :| b
    println(vector2)    // DenseVector(true, true, true)

    // 非
    val vector3: DenseVector[Boolean] = !a
    println(vector3)    // DenseVector(false, true, false)

    val vector4: DenseVector[Double] = DenseVector(1.0, 0.0, -2.0)
    println(vector4)    // DenseVector(1.0, 0.0, -2.0)

  }
}
