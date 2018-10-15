package com.zjf.scala.ml.knn

import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Author: zhangjianfeng
  * @Date: Created in 2018/10/15
  */
object KNNDemo {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/KNN/"
    val K = 3

    //样本数据
    val trainSet = sc.textFile(basePath + "train").map(line => {
      var datas = line.split(" ")
      (datas(0), datas(1), datas(2))
    })

    var bcTrainSet = sc.broadcast(trainSet.collect())
    var bcK = sc.broadcast(K)
    val testSet = sc.textFile(basePath + "test")

    val resultSet = testSet.map(line => {
      var datas = line.split(" ")
      var x = datas(0).toDouble
      var y = datas(1).toDouble
      var trainDatas = bcTrainSet.value
      var set = Set[Tuple6[Double, Double, Double, Double, Double, String]]()
      trainDatas.foreach(trainData => {
        val tx = trainData._1.toDouble
        val ty = trainData._2.toDouble
        val label = trainData._3
        var distance = Math.sqrt(Math.pow(x - tx, 2) + Math.pow(y - ty, 2))
        println(x + "," + y + "," + tx + "," + ty + "," + distance + "," + trainData._3)
        set += Tuple6(x, y, tx, ty, distance, label)
      })


      println("----------------------华丽的分割线-------------------------")
      var list = set.toList
      var sortList = list.sortBy(_._5)
      sortList.foreach(println(_))
      var categoryCountMap = Map[String, Int]()
      var k = bcK.value

      //对knn中取前k个值，计算每个类别出现的次数，少数服从多数
      for (i <- 0 to (k - 1)) {
        var category = sortList(i)._6
        var count = categoryCountMap.getOrElse(category, 0) + 1
        categoryCountMap += (category -> count)
      }
      var rCategory = ""
      var maxCount = 0
      categoryCountMap.foreach(item => {
        println(item._1 + "-" + item._2.toInt)
        if (item._2.toInt > maxCount) {
          maxCount = item._2.toInt
          rCategory = item._1
        }
      })
      (x, y, rCategory)
    })

    resultSet.saveAsTextFile(basePath + "output")

    System.exit(0)
  }


}
