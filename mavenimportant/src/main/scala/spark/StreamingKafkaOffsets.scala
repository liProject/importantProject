package spark

import java.util.Properties

import Untils.OffsetsUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 订单ID,省份ID,订单金额
  * orderId,provinceId,orderPrice
  * 创建样例类，用于接收kafka中values的值
  */
//case class KafkaValues(orderId: String, provinceId: Long, orderPrice: Double)

/**
  * 集成Kafka，使用New Consumer API，获取topic中数据
  * 实时累加统计各个省份销售总的订单额
  */
object StreamingKafkaOffsets {

  val URL = "jdbc:mysql://hadoop4:3306/mytest"

  val Tables = "orderPrice_cnt"

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "root")
  prop.put("driver", "com.mysql.jdbc.Driver")
  //设置检查点路径
  val CHECKPOINT_PATH: String = "datas/spark/streaming/order-state-00001"
  //设置kafka中的topic
  val TOPICS: Array[String] = Array("Moduletopic")
  //设置groupid
  val GROUPID: String = "group_id_001"

  /**
    * 将计算的过程单独创建放入一个方法里
    *
    * @param ssc
    */
  private def processStreamingData(ssc: StreamingContext): Unit = {
    //设置kafka连接参数,这里用的是lamabda表达式
    val KAFKAPARAMS: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop4:9092,hadoop5:9092,hadoop6:9092",

      /**
        * 设置key和value的序列化
        */
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id_0001",

      /**
        * 设置自动从最大的偏移量读数据
        */
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
      * PreferBrokers:仅当您的执行者与您的Kafka经纪人位于同一节点上时，才使用此选项
      * PreferConsistent:在大多数情况下使用此功能，它将在所有执行程序之间一致地分配分区。
      * def PreferFixed(hostMap: collection.Map[TopicPartition, String]): LocationStrategy
      * def PreferFixed(hostMap: ju.Map[TopicPartition, String]): LocationStrategy
      * :如果负载不均衡，可使用此选项将特定的TopicPartitions放置在特定的主机上。
      *    在地图中未指定的任何TopicPartition将使用一致的位置。
      * 表示读取kafka Topic中数据是，采取位置策略
      */
    val LOCATIONSTRATEGY: LocationStrategy = LocationStrategies.PreferConsistent

    /**
      * def Subscribe[K, V](
      * topics: ju.Collection[jl.String],
      * kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V]
      * param K type of Kafka message key
      * param V type of Kafka message value
      * 设置kafka的消费者策略
      * 构建消费策略的时候，当mysql表中存储topic中各个分区的偏移量时依据偏移量读取数据，如果没有从最大偏移量读取
      * 从mysql数据库中获取偏移量信息
      */
    val offsets: Map[TopicPartition, Long] = OffsetsUtils.getoffsetsTOmap(TOPICS, GROUPID)

    /**
      * 判断读取偏移量是否有值
      */
    val consumerStrategy: ConsumerStrategy[String, String] = if (offsets.isEmpty) {
      ConsumerStrategies.Subscribe[String, String](TOPICS, KAFKAPARAMS)
    } else {
      ConsumerStrategies.Subscribe[String, String](TOPICS, KAFKAPARAMS, offsets)
    }
    /**
      * def createDirectStream[K, V](
      * ssc: StreamingContext,
      * locationStrategy: LocationStrategy,
      * consumerStrategy: ConsumerStrategy[K, V]
      * ): InputDStream[ConsumerRecord[K, V]
      * 创建kafkainputDStream（kafka的输入DStream）
      */
    val kafkainputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LOCATIONSTRATEGY, consumerStrategy)


    var offsetrangsNew: Array[OffsetRange] = Array.empty

    /**
      * 读取出kafkainputDStream中的数据
      */
    val kafkaValueDStream: DStream[(Long, Double)] = kafkainputDStream
      .transform { rdd =>
        //          val rddtest: RDD[ConsumerRecord[String, String]] = rdd
        // 从kafkaRDD中获取偏移量数据信息
        offsetrangsNew = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val KafkaValueRDD: RDD[(Long, Double)] = rdd
          .filter { consumer =>
            //拿到kafka中的value
            val kafkaValue = consumer.value()
            null != kafkaValue && kafkaValue.trim.length > 0 && kafkaValue.trim.split(",").length < 5
          }
          //将kafka中value的数据保存到样例类中
          .mapPartitions { iter =>
          iter.map { consumer =>
            val Array(_, provinceId, orderPrice) = consumer.value().trim.split(",")
            (provinceId.toLong, orderPrice.toDouble)
          }
        }
          .coalesce(1)

        KafkaValueRDD
      }

    /**
      * def updateStateByKey[S: ClassTag](
      * updateFunc: (Seq[V], Option[S]) => Option[S]
      * ): DStream[(K, S)]
      * 个人理解：seq[]表示的是当前的batch中所有的要收集的数据，
      * 而Option[]表示的是当前状态之前的所有要收集数据的和
      * 相当于reducebykey中的 item , temp
      */
    //    val kafkaValuesCntDStream: DStream[(Long, Double)] = kafkaValueDStream.updateStateByKey(
    //      (formerValues: Seq[Double], presentValues: Option[Double]) => {
    //        //求出之前的订单金额的总值
    //        val formerValuesSUM: Double = formerValues.sum
    //        //求出现在的订单金额，如果当前状态无值就返回0.0
    //        val presentValue: Double = presentValues.getOrElse(0.0)
    //        //返回所有订单金额的总值
    //        Option(formerValuesSUM + presentValue)
    //      }
    //    )

    /**
      * 用DSL处理kafkaValueDStream
      */
    kafkaValueDStream
      //这里的Time是org.apache.spark.streaming.Time
      .foreachRDD { (rdd, Time) =>
      //       val rddShow: RDD[(Long, Double)] = rdd
      /**
        * FastDateFormat是的快速且线程安全的版本
        * 在大数据领域用
        */
      val strTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss").format(Time.milliseconds)

      //DStream.print()底层源码就是这个
      println("-------------------------------------------")
      println(s"${offsetrangsNew(0).untilOffset}")
      println(s"Time: $strTime")
      println("-------------------------------------------")

      if (!rdd.isEmpty()) {
        //在控制台上打印数据
        rdd.foreachPartition { iter =>
          iter.foreach(println)
        }
        OffsetsUtils.setoffsetsTOtable(offsetrangsNew, GROUPID)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    /**
      * 创建StreamingContext
      */
    val ssc: StreamingContext = {

      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //        .set("spark.streaming.kafka.maxRatePartition", "10000")
        /**
          * 设置使用kyro序列化，spark默认序列为java序列化
          * 并告知那些类使用序列化
          */
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))

      val streamingContext: StreamingContext = StreamingContext.getActiveOrCreate(
        () => {
          new StreamingContext(sparkConf, Seconds(3))
        }
      )

      streamingContext
    }


    processStreamingData(ssc)


    //启动流式应用，针对流式应用来说，主要运行，正常情况一直运行，除非人为关闭终止或程序异常终止
    //加载运行Receiver接收器，作为Task运行在Executor中，实时接收源端数据，Receiver运行需要一个线程
    ssc.start()
    //启动流式应用以后，一直等待终止
    ssc.awaitTermination()
    //流式应用结束，管理资源
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
