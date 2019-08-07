package com.atguigu.ordertimeout_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.ordertimeout_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/7 10:32
  */
object OrderTimeoutWithCep {
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

    val timeoutWarningStream = orderEventStream.process( new OrderTimeoutAlert() )

    timeoutWarningStream.print()

    env.execute("order timeout without cep job")
  }
}

class OrderTimeoutAlert() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 定义一个boolean类型ValueState，用来表示是否已经支付过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean]) )

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先拿出标识位
    val isPayed = isPayedState.value()

    // 根据不同的事件类型来做处理
    if ( value.eventType == "create" && !isPayed ){
      // 如果是create，而且没有被pay过，就创建定时器
      ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 15 * 60 * 1000L )
    } else if( value.eventType == "pay" ){
      // 如果是pay事件，更新状态
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    if( !isPayed ){
      out.collect( OrderResult( ctx.getCurrentKey, "order timeout" ) )
    }
    // 清空状态
    isPayedState.clear()
  }
}