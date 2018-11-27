package com.zjf.scala.ml.apriori

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018-11-27
  */
object AprioriDemo1 {
  def main(args: Array[String]): Unit = {
    selfApriori()
  }


  /**
    * 频繁数据挖掘
    */
  def selfApriori(): Unit = {

    //制造数据
    val data = Array(
      Array("I1", "I2", "I5"),
      Array("I2", "I4"),
      Array("I2", "I3"),
      Array("I1", "I2", "I4"),
      Array("I1", "I3"),
      Array("I2", "I3"),
      Array("I1", "I3"),
      Array("I1", "I2", "I3", "I5"),
      //Array("I1","I2","I3","I5"),
      Array("I1", "I2", "I3"))


    //获取每种商品的支持度
    val c1: Array[String] = data.flatMap(_.toSet).distinct
    val l1: Array[Tuple2[String, Int]] = c1.map { line =>
      val support: Int = data.filter(_.contains(line)).size
      (line, support)
    }.filter(_._2 >= 2)
    getCandidate(c1)

    //由上级L1筛选出C2
    def getCandidate(lastItem: Array[String]): Unit = {
      val arr = new ArrayBuffer[Array[String]]
      for (i <- 0 until lastItem.size) {
        for (j <- i + 1 until lastItem.size) {
          arr.append(Array(lastItem(i), lastItem(j)))
        }
      }
      arr.foreach { line =>
        line.foreach(print)
        println("---------------")
      }
      val result: ArrayBuffer[(Array[String], Int)] = getSupport(arr.toArray).filter(_._2 >= 2)
      result.foreach { line =>
        println(s"${line._1.mkString("[", ",", "]")}+${line._2}")
        println("---------------")
      }
      //由上级l2筛选出c3
      //val result3:ArrayBuffer[(Array[String], Int)]=getCandidate2(result.map(_._1).toArray,3)
      //由上级l3筛选出c4
      //val result4:ArrayBuffer[(Array[String], Int)]=getCandidate2(result3.map(_._1).toArray,4)
      getAllCandidata(result, 3)

    }

    //查看候选集合的支持度
    def getSupport(candidate: Array[Array[String]]): ArrayBuffer[Tuple2[Array[String], Int]] = {
      val result = new ArrayBuffer[Tuple2[Array[String], Int]]
      for (i <- candidate) {
        var count = 0
        for (j <- data) {
          if (containsAll(i, j)) {
            count += 1
          }
        }
        result.append((i, count))
      }
      result
    }

    //由上级l2筛选出c3
    def getCandidate2(lastItem: Array[Array[String]], k: Int): ArrayBuffer[(Array[String], Int)] = {
      //可以先有comblations产生三个元素的项集，再判断每个项集的k-1子项集是否在l（k-1）中
      val c3: Array[Array[String]] = l1.map(_._1).combinations(k).filter(line => verify(line, lastItem, k)).toArray
      //查看候选集合的支持度
      val res = getSupport(c3).filter(_._2 >= 2)
      res.foreach { line =>
        println(s"${line._1.mkString("[", ",", "]")}+${line._2}")
        println("---------------")
      }
      res
    }

    //定义单重集合的包含方法
    def containsAll(arr1: Array[String], arr2: Array[String]): Boolean = {
      var flag = false
      if (arr2.intersect(arr1).size == arr1.size) {
        flag = true
      }
      flag
    }

    //定义多重集合的包含方法
    def ifArrayContains(arr1: Array[Array[String]], arr2: Array[String]): Boolean = {
      var flag = false
      val loop = new Breaks;
      loop.breakable {
        for (i <- arr1) {
          if (i.intersect(arr2).size.equals(arr2.size)) {
            flag = true
            loop.break()
          }
        }
      }
      flag
    }

    /**
      * 判断k-1子项集是否包含在上一个候选集
      */
    def verify(arr1: Array[String], arr2: Array[Array[String]], k: Int): Boolean = {
      var flag = true;
      val bool = arr1.combinations(k - 1).toArray.map { line =>
        if (!ifArrayContains(arr2, line)) {
          flag = false
        }
      }
      flag
    }

    /**
      * 根据规律构建ln筛选Cn+1----使用递归调用
      */
    def getAllCandidata(result: ArrayBuffer[(Array[String], Int)], k: Int): Unit = {
      val result3: ArrayBuffer[(Array[String], Int)] = getCandidate2(result.map(_._1).toArray, k)
      if (k < l1.length) {
        getAllCandidata(result3, k + 1)
      }
    }
  }
}
