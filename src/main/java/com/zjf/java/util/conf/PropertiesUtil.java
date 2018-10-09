package com.zjf.java.util.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;

/**
 * Description:读取配置工具类
 * Author: zhangjianfeng
 */
public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    public static final String JDBC_PROPERTIES = "jdbc.properties";//数据库配置
    public static final String KAFKA_PROPERTIES = "kafka.properties";//kafka配置
    public static final String HDFS_PROPERTIES = "hdfs.properties";//hdfs配置
    public static final String HBASE_PROPERTIES = "hbase.properties";//hbase配置
    public static final String SPARK_PROPERTIES = "spark.properties";//spark配置
    public static final String SCF_PROPERTIES = "scf.properties";//scf配置
    public static final String MONITOR_PROPERTIES = "monitor.properties";//监控配置


    private static HashMap<String, Properties> propertiesMap = new HashMap<String, Properties>();

    private PropertiesUtil() {
    }

    /**
     * 根据filename 获取Properties对象
     *
     * @param filename
     * @return
     */
    public static Properties getProperties(String filename) {
        Properties props = propertiesMap.get(filename);
        if (null == props) {
            props = new Properties();
            InputStreamReader in = null;
            try {
                in = new InputStreamReader(PropertiesUtil.class.getResourceAsStream("/" + filename), "utf-8");
                props.load(in);
                propertiesMap.put(filename, props);
                return props;

            } catch (Exception e) {
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        return props;
    }

}
