package com.zjf.scala.ml.apriori

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description apriori算法主要就是通过conbinations产生候选集，同时验证候选集中每一项的k-1项子集是都在频繁项集L(k-1)中，
  *              如果都在则候选集中的该项满足频繁项集，然后就可以计算该项的支持度support，如果不在则直接filter掉。
  * @Author zhangjianfeng
  * @Date 2018-11-27
  **/
object AprioriDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/Apriori/"

    //模拟数据
    val mydata = Array(Array(1, 3, 4, 5), Array(2, 3, 5), Array(1, 2, 3, 4, 5), Array(2, 3, 4, 5))

    //转化为rdd
    val pamydata: RDD[Array[Int]] = sc.parallelize(mydata)

    //获取数据集中的每项数据
    val C1: Array[Set[Int]] = pamydata.flatMap(_.toSet).distinct().collect().map(Set(_))
    //    C1.foreach(println(_))

    //对每条数据去重
    val D = mydata.map(_.toSet)

    //广播数据集
    val D_bc = sc.broadcast(D)

    //获取数据集的条数大小
    val length = mydata.length

    //设置最小支持度
    var limit = 0.70

    //计算大于最小支持度的数据集（单个数据）
    var suppdata: Array[Any] = sc.parallelize(C1).map(f1(_, D_bc.value, 4, limit)).filter(_.!=(())).collect()
    println("suppdata:"+suppdata)
    var L = Array[Array[Set[Int]]]()
    val L1 = suppdata.map(_ match {
      case a: Tuple2[_, _] => a._1 match {
        case b: Set[_] => b.asInstanceOf[Set[Int]]
      }
    })


    L = L :+ L1
    var k = 2
    while (L(k - 2).length > 0) {
      var CK = Array[Set[Int]]()
      for ((var1, index) <- L(k - 2).zipWithIndex; var2 <- L(k - 2).drop(index + 1) if var1.take(k - 2).equals(var2.take(k - 2))) {
        CK = CK :+ (var1 | var2)
      }
      val suppdata_temp = sc.parallelize(CK).map(f1(_, D_bc.value, 4, limit)).filter(_.!=(())).collect()
      suppdata = suppdata :+ suppdata_temp
      L = L :+ suppdata_temp.map(_ match { case a: Tuple2[_, _] => a._1 match {
        case b: Set[_] => b.asInstanceOf[Set[Int]]
      }
      })
      k += 1
    }
    L = L.filter(_.nonEmpty)
    L.foreach(_.foreach(println))
  }


  //计算每单个数据的支持度大于最小支持度的相关集合
  def f1(a: Set[Int], B: Array[Set[Int]], length: Int, limit: Double) = {
    //只查找每条包含了该数字的的数据集
    if (B.filter(b => a.subsetOf(b)).size / length.toDouble >= limit)
      (a, B.filter(b => a.subsetOf(b)).size / length.toDouble)
  }


}