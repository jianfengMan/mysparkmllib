package com.zjf.scala.util.kafka

import com.zjf.java.util.constant.ConfConstants
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.SimpleConsumerConfig
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

/**
  * Description: streaming-kafka工具类
  * Author: zhangjianfeng
  */
object KafkaManager {

  private val LOGGER = LoggerFactory.getLogger(KafkaManager.getClass)

  def createDirectStream(
                          brokerList: String,
                          zkList: String,
                          kafkaTopic: String,
                          groupId: String,
                          maxBytes: String,
                          ssc: StreamingContext): InputDStream[(String, String)] = {

    var kafkaDStream: InputDStream[(String, String)] = null

    val topics = Array(kafkaTopic).toSet

    val kafkaParams: Map[String, String] = getKafkaParams(brokerList, groupId, maxBytes)


    //获取ZK中保存group + topic 的路径
    val topicDirs = new ZKGroupTopicDirs(groupId, kafkaTopic)
    //最终保存Offset的地方
    val zkTopicPath = topicDirs.consumerOffsetDir


    val zkClient = new ZkClient(zkList)
    val children = zkClient.countChildren(zkTopicPath)

    // 判ZK中是否有保存的数据
    if (children > 0) {
      //从ZK中获取Offset，根据Offset来创建连接
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //首先获取每一个分区的主节点
      val topicList = List(kafkaTopic)
      //向Kafka集群获取所有的元信息，随便连接任何一个节点都可以
      val request = new TopicMetadataRequest(topicList, 0)

      var config = SimpleConsumerConfig(KafkaManager.getKafkaParams(brokerList, groupId, maxBytes))
      val getLeaderConsumer = new SimpleConsumer(config.seedBrokers(0)._1, 9092, 100000, 10000, "OffsetLookup")

      //该请求包含所有的元信息，主要拿到 分区 -》 主节点
      val response = getLeaderConsumer.send(request)
      val topicMetadataOption = response.topicsMetadata.headOption
      val partitons = topicMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None => Map[Int, String]()
      }
      getLeaderConsumer.close()
      LOGGER.info("partitions information is: " + partitons)
      LOGGER.info("children information is: " + children)

      for (i <- 0 until children) {
        //先从ZK读取i这个分区的offset保存
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        LOGGER.info(s"partition[${i}] 将更新写入到Path目前的offset是：${partitionOffset}")

        // 从当前i的分区主节点去读最小的offset，
        val tp = TopicAndPartition(kafkaTopic, i)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitons(i), 9092, 10000, 10000, "getMiniOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        //合并这两个offset
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
        }

        LOGGER.info(s"Partition[${i}] 修正后的偏移量是：${nextOffset}")
        fromOffsets += (tp -> nextOffset)
      }

      zkClient.close()
      LOGGER.info("从ZK中恢复创建Kafka连接")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //直接创建到Kafka的连接
      LOGGER.info("直接创建Kafka连接")
      kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(kafkaTopic))
    }

    kafkaDStream
  }


  def updateOffset(offsetRanges: Array[OffsetRange],
                   groupId: String,
                   kafkaTopic: String,
                   zkList: String) = {

    //保存Offset到ZK
    val updateTopicDirs = new ZKGroupTopicDirs(groupId, kafkaTopic)
    val updateZkClient = new ZkClient(zkList)
    for (offset <- offsetRanges) {

      LOGGER.info("将更新写入到Path-----------" + offset)
      val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
      ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
    }
    updateZkClient.close()
  }


  def getKafkaParams(brokerList: String, groupId: String, maxBytes: String) = {
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConfConstants.KAFKA_OFFSET_RESET,
      ConsumerConfig.FETCH_BUFFER_CONFIG -> maxBytes,
      ConsumerConfig.TOTAL_BUFFER_MEMORY_CONFIG -> ("" + Integer.parseInt(maxBytes) * 1000)
    )
    LOGGER.info(s"getKafkaParams:${kafkaParams.toString()}")
    kafkaParams
  }
}
