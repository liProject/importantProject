package Untils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
  * 将消费kafka topic偏移量数据存储mysql数据库，工具类用于读取和保存偏移量数据
  */
object OffsetsUtils {

  /**
    * 依据Topic名称和消费GroupId获取各个分区的偏移量
    *
    * @param topicNames
    * @param groupID
    * @return
    *
    * CREATE TABLE `tb_offset` (
    * `topic` varchar(255) NOT NULL,
    * `partition` int(11) NOT NULL,
    * `groupid` varchar(255) NOT NULL,
    * `offset` bigint(20) DEFAULT NULL,
    * PRIMARY KEY (`topic`,`partition`,`groupid`)
    * )
    */
  def getoffsetsTOmap(topicNames: Array[String], groupID: String): Map[TopicPartition, Long] = {

    val topicStr: String = topicNames.map(topic => s"\'${topic}\'").mkString(",")


    val querySQL: String =
      s"""
         |SELECT
         |topic,
         |partition,
         |groupid,
         |offset
         |FROM
         | tb_offset
         |WHERE
         | topic in (${topicStr}) AND groupid = ?
         |
        |
      """.stripMargin

    var conn = JdbcUtil.getConn

    var prepStat: PreparedStatement = conn.prepareStatement(querySQL)

    prepStat.setString(1, groupID)

    var resultSet: ResultSet = prepStat.executeQuery()

    var resultMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    //    val resultMap1: Seq[(TopicPartition, Long)] => mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition,Long]

    while (resultSet.next()) {
      val topic: String = resultSet.getString("topic")
      val partition: Int = resultSet.getInt("partition")
      val topicPartition: TopicPartition = new TopicPartition(topic, partition)
      val offset: Long = resultSet.getLong("offset")
      resultMap += topicPartition -> offset
    }

    prepStat.close()

//    conn.close()
    resultMap.toMap
  }

  /**
    * final class OffsetRange private(
    * val topic: String,
    * val partition: Int,
    * val fromOffset: Long,
    * val untilOffset: Long)
    * 保存Streaming每次消费kafka数据后最新偏移量到mysql表中
    *
    * @param offsetRanges
    * @param groupID
    */
  def setoffsetsTOtable(offsetRanges: Array[OffsetRange], groupID: String) = {

    val insertSQL: String =
      """
        |REPLACE
        |INTO
        | tb_offset
        | (topic, partition, groupid, offset)
        | VALUES (?,?,?,?)
      """.stripMargin

    var prepStat: PreparedStatement = null

    var conn: Connection = JdbcUtil.getConn
    try {

      conn.setAutoCommit(false)

      prepStat = conn.prepareStatement(insertSQL)

      offsetRanges.foreach { offsetRange =>
        prepStat.setString(1, offsetRange.topic)
        prepStat.setInt(2, offsetRange.partition)
        prepStat.setString(3, groupID)
        prepStat.setLong(4, offsetRange.untilOffset)

        //加入批次中
        prepStat.addBatch()
      }

      prepStat.executeBatch()

      conn.commit()
    }catch {
      case e:Exception => e.printStackTrace()
    }
//    finally {
//      if (prepStat != null) prepStat.close(); if (null != conn) conn.close()
//    }
  }

  def main(args: Array[String]): Unit = {

//    val groupID = "group_1"
//
//    setoffsetsTOtable(Array(
//      OffsetRange("xx-tp", 0, 11L, 50L),
//      OffsetRange("xx-tp", 1, 11L, 50L),
//      OffsetRange("xx-tp", 2, 11L, 50L),
//      OffsetRange("xx-tp", 3, 11L, 50L)
//    ), groupID)

//    def getoffsetsTOmap(topicNames: Array[String], groupID: String): Map[TopicPartition, Long]
    val partitionToLong = getoffsetsTOmap(Array("xx-tp"),"group_1")

    partitionToLong.foreach(println)

  }
}
