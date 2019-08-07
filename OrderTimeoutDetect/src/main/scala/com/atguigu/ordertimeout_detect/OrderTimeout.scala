package com.atguigu.ordertimeout_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.ordertimeout_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/7 9:06
  */

// 输入订单事件数据流的样例类
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 输出的订单检测结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 0. 读入订单数据
    //    val orderEventStream = env.fromCollection(
    //      List(
    //        OrderEvent(1, "create", 1558430842),
    //        OrderEvent(2, "create", 1558430843),
    //        OrderEvent(2, "other", 1558430845),
    //        OrderEvent(2, "pay", 1558430850),
    //        OrderEvent(1, "pay", 1558431920)
    //      )
    //    )

    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(line => {
        val dataArray = line.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 1. 定义一个事件匹配的模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 在keyBy之后的订单事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 分拣想要的匹配流
    // 先定义一个输出标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val complexResultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    // 4. 用output tag来获取侧输出流，输出结果
    val timeoutResultStream = complexResultStream.getSideOutput(orderTimeoutOutputTag)
    timeoutResultStream.print("timeout")
    complexResultStream.print("pay")

    env.execute("Order timeout detect job")

  }
}

// 自定义pattern timeout function，输出timeout 的结果
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = pattern.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "order timeout")
  }
}

// 自定义 pattern select function，输出正常pay结果
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "order payed successfully")
  }
}