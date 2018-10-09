package com.zjf.java.util.constant;

import com.zjf.java.util.conf.PropertiesUtil;

/**
 * Description:普通常量配置
 * Author: zhangjianfeng
 */
public class ConfConstants {

    //数据库配置
    public static final String JDBC_DRIVER = PropertiesUtil.getProperties(PropertiesUtil.JDBC_PROPERTIES).getProperty("jdbc.driver");
    public static final String JDBC_URL = PropertiesUtil.getProperties(PropertiesUtil.JDBC_PROPERTIES).getProperty("jdbc.url");
    public static final String JDBC_USER = PropertiesUtil.getProperties(PropertiesUtil.JDBC_PROPERTIES).getProperty("jdbc.user");
    public static final String JDBC_PASSWORD = PropertiesUtil.getProperties(PropertiesUtil.JDBC_PROPERTIES).getProperty("jdbc.password");
    public static final String JDBC_DATASOURCE_SIZE = PropertiesUtil.getProperties(PropertiesUtil.JDBC_PROPERTIES).getProperty("jdbc.datasource.size");


    //kakfa配置
    public static final String KAFKA_BROKER_LIST = PropertiesUtil.getProperties(PropertiesUtil.KAFKA_PROPERTIES).getProperty("kafka.broker.list");
    public static final String KAFKA_OFFSET_RESET = PropertiesUtil.getProperties(PropertiesUtil.KAFKA_PROPERTIES).getProperty("kafka.offset.reset");
    public static final String KAFKA_ZOOKEEPER_LIST = PropertiesUtil.getProperties(PropertiesUtil.KAFKA_PROPERTIES).getProperty("kafka.zookeeper.list");

    public static final String KAFKA_TOPIC_MANAUDIT = PropertiesUtil.getProperties(PropertiesUtil.KAFKA_PROPERTIES).getProperty("kafka.topic.manaudit");
    public static final String KAFKA_GROUP_ID_MANAUDIT = PropertiesUtil.getProperties(PropertiesUtil.KAFKA_PROPERTIES).getProperty("kafka.group.id.manaudit");


    //spark配置
    public static final String SPARK_DEPLOY_MODE = PropertiesUtil.getProperties(PropertiesUtil.SPARK_PROPERTIES).getProperty("spark.deploy.mode");


}
