package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/6 9:00
  */

// 定义输入的数据样例类
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String )

// 定义聚合结果样例类
case class UrlViewCount( url: String, windowEnd: Long, count: Long )

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map( line => {
        val dataArray = line.split(" ")
        // 把事件发生的时间转换为时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent( dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6) )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(50)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      } )
      .keyBy(_.url)
      .timeWindow( Time.minutes(10), Time.seconds(5) )
      .aggregate( new CountAgg(), new WindowResultFunction() )
      .keyBy(_.windowEnd)
      .process( new TopNHotUrls(5) )

    stream.print()

    env.execute("Network flow job")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 包装成UrlViewCount输出
class WindowResultFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect( UrlViewCount(key, window.getEnd, input.iterator.next()) )
  }
}

class TopNHotUrls(size: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState( new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]) )

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer( value.windowEnd + 1 )
  }

  // 数据集齐，触发定时器，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViewCounts: ListBuffer[UrlViewCount] = ListBuffer()

//    for( urlViewCount <- urlState.get() ){
//      allUrlViewCounts += urlViewCount
//    }

    val iter = urlState.get().iterator()
    while(iter.hasNext){
      allUrlViewCounts += iter.next()
    }

    urlState.clear()

    // 按照count数量大小排序
    val sortedUrlViewCounts = allUrlViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(size)

    // 格式化成string输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for( i <- sortedUrlViewCounts.indices ){
      val currentUrlViewCount = sortedUrlViewCounts(i)

      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlViewCount.url)
        .append("  流量=").append(currentUrlViewCount.count).append("\n")

    }
    result.append("====================================\n\n")

    Thread.sleep(500)

    out.collect(result.toString())
  }
}