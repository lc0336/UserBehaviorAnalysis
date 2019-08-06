package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/6 14:19
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.fromCollection(
      List(
        LoginEvent(1, "192.168.0.1", "fail", 1558430842),
        LoginEvent(1, "192.168.0.2", "fail", 1558430843),
        LoginEvent(1, "192.168.0.3", "fail", 1558430844),
        LoginEvent(2, "192.168.10.10", "success", 1558430845)
      )
    )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.userId)

    // 1. 定义一个事件匹配模式
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 2. 对原始数据流应用pattern，得到一个pattern stream
    val patternStream= CEP.pattern( loginEventStream, loginFailPattern )

    // 3. 检出符合匹配条件的事件流
    val loginFailDataStream = patternStream.select( new LoginFailMatch() )

    // 4. 打印输出
    loginFailDataStream.print()

    env.execute("Login fail with CEP job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中可以拿到匹配出的两次登录失败事件
    val firstFail = map.get("begin").iterator().next()
    val secondFail = map.get("next").iterator().next()
    Warning( firstFail.userId, firstFail.eventTime, secondFail.eventTime, "login fail!" )
  }
}