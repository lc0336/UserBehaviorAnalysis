package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/5 11:39
  */

// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 窗口聚合结果样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 定义时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val stream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )
      .map( line => {
        val dataArray = line.split(",")
        UserBehavior(  dataArray(0).trim.toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )
      .filter(_.behavior == "pv")
      .keyBy( "itemId" )
      .timeWindow(Time.minutes(60), Time.minutes(5))   // 设置滑动窗口
      .aggregate( new CountAgg(), new WindowResultFuntion() )   // 对窗口数据进行聚合，得到ItemViewCount
      .keyBy("windowEnd")
      .process( new TopNHotItems(3) )    // 统计每一个窗口内点击量前3的商品

    stream.print()

    env.execute("Hot Items kafka job")
  }
}

// 自定义预聚合函数，来一条数据就加一
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义window function，输出ItemViewCount
class WindowResultFuntion() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect( ItemViewCount( itemId, windowEnd, count ) )
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  // 定义一个list state，用来保存当前窗口的所有数据
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 从上下文中获取list state，进行赋值
    itemState = getRuntimeContext.getListState( new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]) )
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都保存到state中
    itemState.add(value)
    // 注册一个定时器，延迟100ms触发，当它触发时，就收集到了窗口所有的数据
    ctx.timerService().registerEventTimeTimer( value.windowEnd + 100 )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发，从状态中提取所有数据，进行排序，取前N个输出

    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    // 把state中的数据导出到allItems
    import scala.collection.JavaConversions._
    for( item <- itemState.get ){
      allItems += item
    }

    // 提前清除state
    itemState.clear()

    // 按照点击量大小排序，取前topSize个
    val sortedItems = allItems.sortWith(_.count > _.count).take(topSize)

    // 把排序数据格式化成String，便于打印输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append( new Timestamp(timestamp - 100) ).append("\n")

    // 对窗口内的每条数据格式化输出
    for( i <- sortedItems.indices ){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i+1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")

    // 控制输出频率
    Thread.sleep(500)
    out.collect(result.toString())
  }
}