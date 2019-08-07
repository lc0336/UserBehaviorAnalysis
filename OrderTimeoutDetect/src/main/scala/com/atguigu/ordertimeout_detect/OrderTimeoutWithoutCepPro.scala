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
  * Created by wushengran on 2019/8/7 11:17
  */
object OrderTimeoutWithoutCepPro {

  val orderTimeoutOutput = new OutputTag[OrderResult]("orderTimeout")

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

    val orderResultStream = orderEventStream.process( new OrderPayMatch() )

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutput).print("timeout")

    env.execute("order timeout without cep pro job")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    // 定义状态：是否已支付，定时器时间戳
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean]) )
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]) )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 拿到当前状态
      val isPayed = isPayedState.value()
      val timerTS = timerState.value()

      // 根据事件类型做判断
      if( value.eventType == "create" ){
        // 如果是create事件，根据是否已经pay过来判断是否注册定时器
        if( isPayed ){
          // 1. 已经支付，create和pay匹配成功，输出正常result
          out.collect( OrderResult( value.orderId, "order payed successfully" ) )
          // 取消定时器，清空状态
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 2. 如果没有支付过，那么注册定时器开始等待
          ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 15 * 60 * 1000L )
          timerState.update(value.eventTime * 1000L + 15 * 60 * 1000L)
        }
      } else if( value.eventType == "pay" ){
        // 如果是pay事件，判断是否之前已经有create
        if( timerTS > 0 ){
          // 3. 如果有timer设置，那么说明之前来过create事件，现在可以输出结果，然后清理状态
          if ( timerTS >= value.eventTime * 1000L ){
            // 定时器时间大于当前pay事件的时间，说明正常pay成功
            out.collect( OrderResult( value.orderId, "order payed successfully" ) )
          } else{
            // 如果定时器时间小于pay时间，说明pay已经是在timeout之后才发生的，报一个超时警告
            ctx.output(orderTimeoutOutput, OrderResult(value.orderId, "order payed but already timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else{
          // 4. timer 是 0,代表create没有来过
          isPayedState.update(true)
          // 注册一个定时器，等待create事件到来
          ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L )
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if(isPayedState.value()){
        // 如果pay过，说明是create超时没来，可能是数据丢失或异常
        ctx.output(orderTimeoutOutput, OrderResult(ctx.getCurrentKey, "order already payed but not found create log"))
      }else{
        // 如果没有pay过，那么pay超时报警
        ctx.output(orderTimeoutOutput, OrderResult(ctx.getCurrentKey, "order timeout"))
      }

      // 清理状态
      isPayedState.clear()
      timerState.clear()
    }
  }
}
