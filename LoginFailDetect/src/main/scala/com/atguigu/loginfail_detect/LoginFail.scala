package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/6 10:15
  */

// 输入登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
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

    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarning())
      .print()

    env.execute("Login fail detect job")

  }
}

class LoginWarning() extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义状态变量，用于存储登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //    if( value.eventType == "fail" ){
    //      // 如果是失败事件，加入失败列表，注册定时器，两秒后触发
    //      loginFailState.add(value)
    //      ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 2000L )
    //    } else {
    //      // 如果是成功事件，直接清空状态，重新开始
    //      loginFailState.clear()
    //    }

    // 改进的实现方式：首先判断是否是失败事件
    if (value.eventType == "fail") {
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        val firstFailEvent = iter.next()
        // 如果两次失败时间间隔小于2秒，输出报警信息
        if (value.eventTime < firstFailEvent.eventTime + 2) {
          out.collect(Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "在两秒内连续登录失败！"))
        }

        // 不管报不报警，清空列表，把最近一次失败事件写入
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果失败列表还没有数据，直接存入第一次失败事件
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，直接清空状态
      loginFailState.clear()
    }
  }

  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
  //    // 先把失败事件从列表中取出
  //    val allLoginFailEvents: ListBuffer[LoginEvent] = ListBuffer()
  //
  //    val iter = loginFailState.get().iterator()
  //    while(iter.hasNext)
  //      allLoginFailEvents += iter.next()
  //
  //    if( allLoginFailEvents.length >= 2 ){
  //      // 如果失败次数大于等于2，报警
  //      out.collect( Warning( allLoginFailEvents.head.userId,
  //        allLoginFailEvents.head.eventTime,
  //        allLoginFailEvents.last.eventTime,
  //        "在2秒内连续登录失败" + allLoginFailEvents.length + "次。"
  //      ) )
  //    }
  //    // 时间已经够2秒，不论是否报警，都清空状态
  //    loginFailState.clear()
  //  }
}