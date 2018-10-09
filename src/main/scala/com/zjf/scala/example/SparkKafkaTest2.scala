package com.zjf.scala.example

import com.zjf.java.util.constant.ConfConstants
import com.zjf.scala.util.kafka.KafkaManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Description:sparkstreaming手动维护offset测试
  * Author: zhangjianfeng
  */
object SparkKafkaTest2 {

  def main(args: Array[String]): Unit = {

    var topics = "hdp_ubu_xxzl_hunter_strategy_adv"
    var groupId ="hdp_ubu_xxzl_hunter_strategy_adv_group0001"

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    val batchDuration = Seconds(1) //时间单位为秒
    val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)
    var maxBytes = "52428"

    var kafkaDStream: InputDStream[(String, String)] = KafkaManager.createDirectStream(
      ConfConstants.KAFKA_BROKER_LIST,
      ConfConstants.KAFKA_ZOOKEEPER_LIST,
      topics,
      groupId,
      maxBytes,
      ssc
    )

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream = kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //强制转型
      rdd
    }

    textKafkaDStream.window(Seconds(1), Seconds(2)).map(s => s._2).foreachRDD { rdd =>
      //      println(rdd.partitions.length)


      //RDD操作
      rdd.foreachPartition { items =>

        items.foreach(result =>{
          println("----------------------"+result)
        })

      }

//      KafkaManager.updateOffset(offsetRanges,
//        groupId,
//        topics,
//        ConfConstants.KAFKA_ZOOKEEPER_LIST)
    }

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination() //用于拦截跑出异常
  }

}
