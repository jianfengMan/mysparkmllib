package com.zjf.scala.example

import com.zjf.java.util.constant.ConfConstants
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Description:sparkstreaming消费kafka消息测试
  * Author: zhangjianfeng
  */
object SparkKafkaTest1 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val batchDuration = Seconds(5) //时间单位为秒
    val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)
    ssc.checkpoint("./checkpoint")


    val topics = Array(ConfConstants.KAFKA_TOPIC_MANAUDIT).toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfConstants.KAFKA_BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> ConfConstants.KAFKA_GROUP_ID_MANAUDIT,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConfConstants.KAFKA_OFFSET_RESET
    )

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    stream.window(Seconds(5),Seconds(5)).foreachRDD(rdd => {
      rdd.foreach(line => {
        println("key=" + line._1 + "  value=" + line._2)
      })
    })


    ssc.start() //spark.properties stream系统启动
    ssc.awaitTermination() //
  }
}
