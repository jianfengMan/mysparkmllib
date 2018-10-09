package com.zjf.scala.example

import com.alibaba.fastjson.JSON
import com.zjf.java.util.constant.ConfConstants
import kafka.serializer.StringDecoder
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaTest4 {

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

    textKafkaDStream.window(Seconds(5), Seconds(10)).map(s => s._2).foreachRDD { rdd =>


      //导入隐饰操作，否则RDD无法调用toDF方法
      import sparkSession.implicits._

      //1.准备数据
      //1.1 猎人审核人工日志表
      val auditLogRDD = rdd.map { item =>
        val paramMap = JSON.parseObject(item)
        (MapUtils.getString(paramMap, "accessid", ""), MapUtils.getString(paramMap, "tasksource", ""),
          MapUtils.getString(paramMap, "auditstate", ""))
      }.coalesce(1, true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

      val auditLogDF = auditLogRDD.toDF("accessid", "tasksource", "auditstate")
      auditLogDF.createOrReplaceTempView("tmp_audit_log")

      val detailResultDF = sparkSession.sql("select * from tmp_audit_log")

      //RDD操作
      detailResultDF.foreachPartition { items =>
        //具体的业务
        items.foreach(message => {
          println(message)
        })
      }

      kafkaManager.commitOffsetsToZK(offsetRanges)
    }

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination() //用于拦截跑出异常
  }

}
