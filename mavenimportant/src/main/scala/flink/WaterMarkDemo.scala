package flink

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * 水位线。解决网络乱序和网路延迟的
  *   这里原理是先确定事件窗口，然后在确定延迟
  */
object WaterMarkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /** 设置事件时间
      * def setStreamTimeCharacteristic(characteristic: TimeCharacteristic) : Unit
      * TimeCharacteristic是枚举
      * ProcessingTime  处理时间 算子处理数据时间
      * IngestionTime   提取时间  提取数据源的时间
      * EventTime     事件时间  消息源本身携带的时间
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop4:9092,hadoop5:9092,hadoop6:9092")
    prop.setProperty("group.id", "li")
    prop.setProperty("auto.offset.reset", "latest")

    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      "test09",
      new SimpleStringSchema(),
      prop
    )

    val kafkaData: DataStream[String] = env.addSource(kafkaConsumer)

    val bossData: DataStream[Boss] = kafkaData
      .filter(line => null != line && line.trim.split(",").length == 4)
      .map { line =>
        val Array(time, boss, product, price) = line.trim.split(",")
        Boss(time.toLong, boss, product, price.toDouble)
      }

    /** 设置水位线。窗口触发比时间延迟2s钟
      * def assignTimestampsAndWatermarks(
      * assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T]
      *
      * 基于某些元素带有的某些标记 使用AssignerWithPunctuatedWatermarks
      */
    val waterData: DataStream[Boss] =
      bossData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Boss] {

        val delayTime: Long = 2000L
        var currentTinmestamp: Long = 0L

        /**
          * 当前水位线，水位线是一个延迟的时间轴
          *
          * @return
          */
        override def getCurrentWatermark: Watermark = {
          //当前水位线
          new Watermark(currentTinmestamp - delayTime)
        }

        /**
          *
          * @param element
          * @param previousElementTimestamp 上一个元素的时间戳
          * @return
          */
        override def extractTimestamp(element: Boss, previousElementTimestamp: Long): Long = {
          val evenTime: Long = element.time
          //为了让时间窗口一直是向前的
          currentTinmestamp = Math.max(evenTime, currentTinmestamp)
          evenTime
        }
      })

    //设置侧边流，将窗口没有收集到的数据进行收集
    val outTag: OutputTag[Boss] = new OutputTag[Boss]("outTag")

    val result: DataStream[Boss] = waterData
      .keyBy(_.boss)
      //3s钟一个无重叠的窗口
      .timeWindow(Time.seconds(3))
      //在设置延迟的基础上再延迟2s钟
//      .allowedLateness(Time.seconds(0))
      .sideOutputLateData(outTag)
      .maxBy(3)
    result.print("正常结果：")

    //获得延迟数据并打印
    val delayData: DataStream[Boss] = result.getSideOutput(outTag)
    delayData.print("延迟数据")

    env.execute()
  }
}

//数据：(时间，公司，产品，价格)
case class Boss(time: Long, boss: String, product: String, price: Double)