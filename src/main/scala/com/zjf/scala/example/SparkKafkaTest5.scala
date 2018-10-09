package com.zjf.scala.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 监控hdfs目录文件拜变化
  */
object SparkKafkaTest5 {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()


    val batchDuration = Seconds(5) //时间单位为秒
    val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)
    val dataDStream = ssc.textFileStream("file:///text")
    ssc.checkpoint("./checkpoint")



    dataDStream.window(Seconds(5), Seconds(5)).foreachRDD { rdd =>
      rdd.checkpoint()
      //RDD操作
      rdd.foreachPartition { items =>
        //具体的业务
        items.foreach(message => {
          println(message)
        })
      }
    }

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination() //用于拦截跑出异常
  }

}
