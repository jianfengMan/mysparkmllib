package com.zjf.scala.example

import kafka.serializer.StringDecoder
import com.zjf.java.util.constant.ConfConstants
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Description:sparkstreaming手动维护offset测试
  * Author: zhangjianfeng
  */
object SparkKafkaTest3 {

  def main(args: Array[String]): Unit = {

    var topics = ConfConstants.KAFKA_TOPIC_MANAUDIT
    var groupId = ConfConstants.KAFKA_GROUP_ID_MANAUDIT

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()


    val batchDuration = Seconds(5) //时间单位为秒
    val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)
    var maxBytes = "524287"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> ConfConstants.KAFKA_BROKER_LIST,
      "group.id" -> groupId,
      "fetch.message.max.bytes" -> maxBytes)

    val kafkaManager = new KafkaManager(kafkaParams)
    val kafkaDStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream = kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //强制转型
      rdd
    }

    textKafkaDStream.window(Seconds(5), Seconds(5)).map(s => s._2).foreachRDD { rdd =>
      //      println(rdd.partitions.length)
      val sum = offsetRanges.map(_.count).sum
      println("sum---------------------------" + sum)
      //RDD操作
      rdd.foreachPartition { items =>

        //具体的业务
        items.foreach(message => {
//          println(message)
        })
      }

      kafkaManager.commitOffsetsToZK(offsetRanges)
    }

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination() //用于拦截跑出异常
  }

}
