package com.zjf.scala.combine

import com.zjf.java.util.constant.ConfConstants
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Description: 合并文件
  * Author: zhangjianfeng
  */
object CombineFile {

  private val LOGGER = LoggerFactory.getLogger(CombineFile.getClass)

  def main(args: Array[String]): Unit = {
    if (args == null || args.length != 2) {
      LOGGER.error("the args is error")
    }
    val inputDir = args(0)
    val outputDir = args(1)
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(ConfConstants.SPARK_DEPLOY_MODE)
      .getOrCreate()
    val _sc = sparkSession.sparkContext
    _sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress.codec", "com.hadoop.compression.lzo.LzoCodec")
    _sc.hadoopConfiguration.set("mapred.output.compress", "true")
    _sc.getConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256000000")
    _sc.getConf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "1024000")
    _sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    _sc.getConf.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable,org.apache.hadoop.io.Text")

    val path = new Path(inputDir)
    val inputRDD = _sc.newAPIHadoopFile(path.toString,
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      _sc.hadoopConfiguration)
    inputRDD.map(_._2.toString).saveAsTextFile(outputDir)

    _sc.stop()
  }
}
