package flink

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import Untils.JdbcUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object MysqlToKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val mysqlData: DataStream[String] = env.addSource(new MysqlSourceFunc)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop4:9092,hadoop5:9092,hadoop6:9092")
    prop.setProperty("group.id", "test1027")

    /**
      * public FlinkKafkaProducer011(
      *   String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig)
      */
    val kafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](
      "test1",
      new SimpleStringSchema(),
      prop)

    mysqlData.addSink(kafkaProducer)

    env.execute()
  }
}

class MysqlSourceFunc extends RichSourceFunction[String] {

  var pst: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val conn: Connection = JdbcUtil.getConn
    val sql: String =
      """
        |SELECT
        | word,cnt
        |FROM
        | test
      """.stripMargin
    pst = conn.prepareStatement(sql)
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val resultSet: ResultSet = pst.executeQuery()

    while (resultSet.next()) {
      val word: String = resultSet.getString(1)
      val cnt: Int = resultSet.getInt(2)
      ctx.collect(s"${word}|${cnt}")
    }
  }

  override def cancel(): Unit = {
    if (pst != null) pst.close()
  }
}
