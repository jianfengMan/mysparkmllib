package com.zjf.scala.util.accumulator

import org.apache.spark.util.AccumulatorV2


/**
  * Description:自定义累加器
  * Author: zhangjianfeng
  */
class MyAccumulator extends AccumulatorV2[Object, List[Object]] {

  private var items: List[Object] = List()

  // 判断内部的数据结构是否为空
  override def isZero: Boolean = {
    items.isEmpty
  }

  // 深度拷贝一个自定义累加器的实例
  override def copy(): AccumulatorV2[Object, List[Object]] = {
    val newAcc = new MyAccumulator
    items.synchronized {
      newAcc.items ++ (items)
    }
    newAcc
  }

  // 重置累加器内的数据结构
  override def reset(): Unit = {
    items = List()
  }

  //实现累加逻辑
  override def add(v: Object): Unit = {
    items = items.:+(v)
  }

  //合并各个分区的累加器实例
  override def merge(other: AccumulatorV2[Object, List[Object]]): Unit = {
    items = items.++(other.value)
  }

  //输出值
  override def value: List[Object] = {
    items
  }
}
