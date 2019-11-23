package flink

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisConfigBase}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


object KafkaToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置kafka消费者配置
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop4:9092,hadoop5:9092,hadoop6:9092")
    prop.setProperty("group.id", "test1027")
    prop.setProperty("auto.offset.reset", "latest") //最近消费，是与偏移量相关

    //得到kafaka的消费者客户端
    val kafkaCousumer = new FlinkKafkaConsumer011[String]("test09",new SimpleStringSchema(),prop)

    val kafkaData: DataStream[String] = env.addSource(kafkaCousumer)

    //将kafka的数据进行处理
    val resultData: DataStream[(String, Int)] = kafkaData
      .filter(line => null != line && line.split("\\W+").length > 0)
      .flatMap(_.split("\\W+"))
      .map((_, 1))

      /** 代替groupBy
        * def keyBy(fields: Int*): KeyedStream[T, JavaTuple]
        */
      .keyBy(0)
      .sum(1)

    /**
      * 将redis的节点封装到util.HashSet中
      */
    val inetSocketAddresses = new util.HashSet[InetSocketAddress]()
    inetSocketAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop4"),7001))
    inetSocketAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop5"),7001))
    inetSocketAddresses.add(new InetSocketAddress(InetAddress.getByName("hadoop6"),7001))

    /**
      * 配置redis的基本设置
      */
    val jedisClusterConfig: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
      .setNodes(inetSocketAddresses)
      .setMaxIdle(10)
      .build()

    /** 得到redis的客户端
      * public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper)
      */
    val redisCluster: RedisSink[(String, Int)] = new RedisSink(jedisClusterConfig,new RedisMapperFunc)

    resultData.addSink(redisCluster)

    env.execute()
  }
}

class RedisMapperFunc extends RedisMapper[(String,Int)]{
  /**
    * 指定写入redis的数据类型
    * @return
    */
  override def getCommandDescription: RedisCommandDescription = {
    /**
      * public RedisCommandDescription(RedisCommand redisCommand, String additionalKey)
      */
    new RedisCommandDescription(RedisCommand.HSET,"redisSink")
  }

  /**
    * 指定写入redis的key
    * @param data
    * @return
    */
  override def getKeyFromData(data: (String, Int)): String = {
    data._1
  }

  /**
    * 指定写入redis的value
    * @param data
    * @return
    */
  override def getValueFromData(data: (String, Int)): String = {
    data._2.toString
  }
}