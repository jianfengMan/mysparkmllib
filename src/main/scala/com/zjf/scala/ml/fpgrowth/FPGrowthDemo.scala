package com.zjf.scala.ml.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * /**
  * * 关联规则：研究不同类型的物品相互之间的关联关系的规则．
  * * 应用于＂超市购物分析＂( 啤酒与尿布), ＂网络入侵检测＂，＂医学病例共同特征挖掘＂
  * * 支持度：表示 X　和　Y　中的项在同一条件下出现的次数
  * * 置信度：表示 X 和　Y　中的项在一定条件下出现的概率
  * * Apriori算法：属于候选消除算法．是一个生成候选集，消除不满足条件的候选集，不断循环，直到不再产生候选集的过程．
  * * FP-growth算法过程：
  * * (1) 扫描样本数据库，将样本按照体递减规则排序，删除小于最小支持度的样本数
  * * (2) 重新扫描样本数据库，并将样本按照上标的支持度数据排列
  * * (3) 将重新生成的表按顺序插入 FP 树中,继续生成FP树，直到形成完整的FP树
  * * (4) 建立频繁项集规则　
  * * FP-Growth
  * * https://my.oschina.net/sunmin/blog/723852
  * *
  **/
  * @Author: zhangjianfeng
  * @Date: Created in 2018-12-03
  */
object FPGrowthDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.WARN)

    val basePath = "/Users/zhangjianfeng/workspaces/workspace_github_bg/mysparkmllib/data/FPGrowth/"

    // 读取样本数据
    val data: RDD[String] = sc.textFile(basePath + "sample_fpgrowth.txt")
    val examples: RDD[Array[String]] = data.map(_.split(" "))

    // 建立模型
    val minSupport = 0.2 // 最小支持度
    val numPartition = 10 // 设置分区数 默认为输入样本数据的分区数
    val model: FPGrowthModel[String] = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(examples)

    // 输出结果
    println(s"Number of frequent itemsets : ${model.freqItemsets.count()}")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    }

    /**
      * Number of frequent itemsets : 54
      * [z],5
      * [x],4
      * [x,z],3
      * [y],3
      * [y,x],3
      * [y,x,z],3
      * [y,z],3
      * [r],3
      * [r,x],2
      * [r,z],2
      * [s],3
      * [s,y],2
      * [s,y,x],2
      * [s,y,x,z],2
      * [s,y,z],2
      * [s,x],3
      * [s,x,z],2
      * [s,z],2
      * [t],3
      * [t,y],3
      * [t,y,x],3
      * [t,y,x,z],3
      * [t,y,z],3
      * [t,s],2
      * [t,s,y],2
      * [t,s,y,x],2
      * [t,s,y,x,z],2
      * [t,s,y,z],2
      * [t,s,x],2
      * [t,s,x,z],2
      * [t,s,z],2
      * [t,x],3
      * [t,x,z],3
      * [t,z],3
      * [p],2
      * [p,r],2
      * [p,r,z],2
      * [p,z],2
      * [q],2
      * [q,y],2
      * [q,y,x],2
      * [q,y,x,z],2
      * [q,y,z],2
      * [q,t],2
      * [q,t,y],2
      * [q,t,y,x],2
      * [q,t,y,x,z],2
      * [q,t,y,z],2
      * [q,t,x],2
      * [q,t,x,z],2
      * [q,t,z],2
      * [q,x],2
      * [q,x,z],2
      * [q,z],2
      */
  }

}
