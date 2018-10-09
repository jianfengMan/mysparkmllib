package com.zjf.scala.util.kafka

import java.util.Properties

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Description:创建一个类用来代理Kafka连接的创建等工作
  * Author: zhangjianfeng
  */
class KafkaProxy(brokers: String) {
  private val pros: Properties = new Properties();
  pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
  pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private val kafkaConn = new KafkaProducer[String, String](pros)

  def send(topic: String, key: String, value: String): Unit = {
    kafkaConn.send(new ProducerRecord[String, String](topic, key, value))
  }

  def send(topic: String, value: String): Unit = {
    kafkaConn.send(new ProducerRecord[String, String](topic, value))
  }

  def close(): Unit = {
    kafkaConn.close()
  }
}

//创建一个负责生成KafkaProxy的工厂类
class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy] {
  //创建KafkaProxy的实例
  override def create(): KafkaProxy = {
    new KafkaProxy(brokers)
  }

  //包装KafkaProxy的实例为Pool中的对象
  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = {
    new DefaultPooledObject[KafkaProxy](t)
  }
}


object KafkaConnPool {
  // 代表连接池
  private var kafkaProxyPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers: String): GenericObjectPool[KafkaProxy] = {
    if (null == kafkaProxyPool) {
      KafkaConnPool.synchronized {
        if (null == kafkaProxyPool) {
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
        }
      }
    }
    kafkaProxyPool
  }

}
