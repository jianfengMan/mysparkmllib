package com.zjf.scala.util.sparksql

import java.util.Properties

import com.zjf.java.util.constant.ConfConstants
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
  * Description: sparksql工具类，方便操作数据库
  * Author: zhangjianfeng
  */
object SparkSqlUtils {

  @transient private var instance: SQLContext = _

  def getInstance(): SQLContext = {
    if (instance == null) {
      SparkSqlUtils.synchronized {
        instance = SparkSession.builder().getOrCreate().sqlContext
      }
    }
    instance
  }

  /**
    * 使用默认配置读取
    *
    * @param tableName
    * @param condition
    * @param col
    * @param cols
    * @return
    */
  def executeQuery(tableName: String, condition: Array[String], col: String, cols: String*): DataFrame = {
    val sqlContext = getInstance()
    val prop = new Properties()
    prop.setProperty("driver", ConfConstants.JDBC_DRIVER)
    prop.setProperty("user", ConfConstants.JDBC_USER)
    prop.setProperty("password", ConfConstants.JDBC_PASSWORD)

    sqlContext.read.jdbc(ConfConstants.JDBC_URL, tableName, condition, prop).select(col, cols: _*)
  }

  /**
    * 传入配置读取
    *
    * @param prop
    * @param jdbcUrl
    * @param tableName
    * @param condition
    * @param col
    * @param cols
    * @return
    */
  def executeQuery(prop: Properties, jdbcUrl: String, tableName: String, condition: Array[String], col: String, cols: String*): DataFrame = {
    val sqlContext = getInstance()
    sqlContext.read.jdbc(jdbcUrl, tableName, condition, prop).select(col, cols: _*)
  }


  /**
    * 默认配置
    *
    * @param dataFrame
    * @param tableName
    */
  def insertMySQL(dataFrame: DataFrame, tableName: String): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("url", ConfConstants.JDBC_URL)
      .option("dbtable", tableName)
      .option("user", ConfConstants.JDBC_USER)
      .option("password", ConfConstants.JDBC_PASSWORD)
      .mode(SaveMode.Append)
      .save()
  }


  /**
    * 传入配置
    *
    * @param jdbcUrl
    * @param user
    * @param password
    * @param dataFrame
    * @param tableName
    */
  def insertMySQL(jdbcUrl: String, user: String, password: String, dataFrame: DataFrame, tableName: String): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .save()
  }


}
