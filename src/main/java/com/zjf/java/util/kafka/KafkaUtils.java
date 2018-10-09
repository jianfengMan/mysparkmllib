package com.zjf.java.util.kafka;

import java.util.*;

import com.zjf.java.util.common.StringUtils;
import com.zjf.java.util.constant.ConfConstants;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Description:kafka工具类
 * Author: zhangjianfeng
 */
public class KafkaUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    private static Producer<String, String> producer;
    private static ConsumerConnector consumer;

    //生产者初始化
    static {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("metadata.broker.list", ConfConstants.KAFKA_BROKER_LIST);
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }


    //消费者初始化
    static {
        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", ConfConstants.KAFKA_ZOOKEEPER_LIST);

        // group 代表一个消费组
        props.put("group.id", "group-id-your");

        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);



    }

    /**
     * 批次发送消息
     *
     * @param topic
     * @param contents
     */
    public static void sendMsgs(String topic, String[] contents) {
        sendMsgsByKey(topic, "", contents);
    }


    /**
     * 发送单条消息
     *
     * @param topic
     * @param msg
     */
    public static void sendMsg(String topic, String msg) {
        producer.send(new KeyedMessage<String, String>(topic, msg));
    }

    /**
     * 发送单条消息，指定分区key
     *
     * @param topic
     * @param key
     * @param msg
     */
    public static void sendMsgByKey(String topic, String key, String msg) {
        producer.send(new KeyedMessage<String, String>(topic, key, msg));
    }

    /**
     * 发送批次消息，指定分区key
     *
     * @param topic
     * @param key
     * @param contents
     */
    public static void sendMsgsByKey(String topic, String key, String[] contents) {
        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
        for (int i = 0; i < contents.length; i++) {
            if (StringUtils.isEmpty(contents[i])) {
                continue;
            }
            if (StringUtils.isEmpty(key)) {
                messages.add(new KeyedMessage<>(topic,System.currentTimeMillis()+"", contents[i]));
            } else {
                messages.add(new KeyedMessage<>(topic, key, contents[i]));
            }

        }
        producer.send(messages);
    }


    /**
     * 生产者和消费者的groupid不能相同，否则不能消费消息
     *
     * @param topic
     */
    public static void consume(String topic) {

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
                keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext())
            System.out.println(it.next().message());
    }


    @Test
    public void producerTest1() {
        String message = "测试消息1";
        String[] ar = new String[]{message,message+1};
        sendMsgs(ConfConstants.KAFKA_TOPIC_MANAUDIT, ar);
        System.out.println("message----" + message + "----发送成功");
    }

    @Test
    public void producerTest2() {
        String message = "测试消息2";
        sendMsg(ConfConstants.KAFKA_TOPIC_MANAUDIT, message);
        System.out.println("message:" + message + "发送成功");
    }

    @Test
    public void consumerTest() {
        consume(ConfConstants.KAFKA_TOPIC_MANAUDIT);
    }

}