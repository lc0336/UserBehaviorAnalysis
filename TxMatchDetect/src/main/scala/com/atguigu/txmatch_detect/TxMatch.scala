package com.atguigu.txmatch_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.txmatch_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/7 14:06
  */

// 定义用户支付信息的样例类，来自订单事件流
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
// 定义支付到账信息的样例类，来自另一条流
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object TxMatch {

  // 定义不匹配时的侧输出流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 定义输入数据流：订单流，支付到账流
    val orderEventStream = env.fromCollection(
      List(
        OrderEvent( 1, "create", "", 1558430842),
        OrderEvent( 2, "create", "", 1558430843),
        OrderEvent( 1, "pay", "111", 1558430844),
        OrderEvent( 2, "pay", "222", 1558430845),
        OrderEvent( 3, "create", "", 1558430849),
        OrderEvent( 3, "pay", "333", 1558430849)
      )
    ).assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)

    val receiptEventStream = env.fromCollection(
      List(
        ReceiptEvent( "111", "wechat", 1558430847),
        ReceiptEvent( "222", "alipay", 1558430848),
        ReceiptEvent( "444", "alipay", 1558430850)
      )
    ).assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream = orderEventStream.connect(receiptEventStream).process( new TxMatchDetection() )

    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatechReceipts")

    processedStream.print("matched")

    env.execute("tx match job")
  }

  class TxMatchDetection() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
    // 定义两个value状态，用于表示当前的订单支付事件和到账事件是否已经发生
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 订单支付事件，需要判断当前是否已经有对应的receipt来了
      val receipt = receiptState.value()
      if( receipt != null){
        // 已经都到齐了，正常匹配输出
        out.collect( (pay, receipt) )
        receiptState.clear()
      } else{
        // 如果receipt还没到，注册一个定时器，等待receipt到来
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 3000L )
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 支付到账事件
      val pay = payState.value()
      if( pay != null){
        // 已经都到齐了，正常匹配输出
        out.collect( (pay, receipt) )
        payState.clear()
      } else{
        // 如果pay还没到，注册一个定时器，等待receipt到来
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 3000L )
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 如果某个状态没有被清空，另一个没有收到
      if( payState.value() != null ){
        ctx.output(unmatchedPays, payState.value())
      }
      if( receiptState.value() != null ){
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}
