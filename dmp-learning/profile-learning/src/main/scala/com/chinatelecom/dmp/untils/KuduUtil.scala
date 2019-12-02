package com.chinatelecom.dmp.untils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kudu.client.{CreateTableOptions, DeleteTableResponse, KuduClient}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class KuduUtil extends Logging{

  private var spark :SparkSession = _
  private var df :DataFrame = _

  private lazy val config: Config = ConfigFactory.load("kudu.conf")

  private var kuduContext: KuduContext = _

  def this(sparkSession: SparkSession){
    this()
    this.spark = sparkSession
    /**
      * class KuduContext(val kuduMaster : scala.Predef.String, sc : org.apache.spark.SparkContext)
      */
    kuduContext = new KuduContext(config.getString("kudu.master"),sparkSession.sparkContext)
  }

  def this(dataFrame: DataFrame){
    this(dataFrame.sparkSession)
    this.df = dataFrame
  }

  def createKuduTable(tableName:String,schema: StructType,keyField:Seq[String],partitionNum:Int,isDelete: Boolean = true):Unit={
    /**
      *  public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
      */
    if (kuduContext.tableExists(tableName)){
      if (isDelete){
        deletKuduTable(tableName)
        logInfo(s"${tableName}表存在，删除中。。。。。。")
      }else{
        logInfo(s"${tableName}表存在，不删除。。。。。。")
        return
      }
    }
      val tableOptions = new CreateTableOptions()
      import scala.collection.JavaConverters._
      tableOptions.addHashPartitions(keyField.asJava,partitionNum)
      tableOptions.setNumReplicas(config.getString("kudu.table.factor").toInt)
      logInfo(s"${tableName}表不存在，创建中。。。。。。")
      kuduContext.createTable(tableName,schema,keyField,tableOptions)
    }

  def readKudu(tableName:String) ={
    if (kuduContext.tableExists(tableName)) {
      logInfo(s"正在读取${tableName}表")
      import org.apache.kudu.spark.kudu._
      val kuduDF: DataFrame = spark
        .read
        .option("kudu.master", config.getString("kudu.master"))
        .option("kudu.table", tableName)
        .kudu
      Some(kuduDF)
    }else{
      logWarning(s"${tableName}不存在")
      None
    }
  }

  def saveAsKuduTable(tableName:String){
    import org.apache.kudu.spark.kudu._
    if (!kuduContext.tableExists(tableName)){
      createKuduTable(tableName,df.schema,Seq("uuid"),3)
    }
    df.write
      .mode(SaveMode.Append)
      .option("kudu.master", config.getString("kudu.master"))
      .option("kudu.table", tableName)
      .kudu
    logInfo(s"将数据保存在${tableName}中")
  }

  def deletKuduTable(tableName:String){
    if (kuduContext.tableExists(tableName)){
      val response: DeleteTableResponse = kuduContext.deleteTable(tableName)
      logInfo(s"Kudu中表：${tableName}存在，已删除成功, 耗时: ${response.getElapsedMillis} 毫秒 ..............")
    }
  }
}

object KuduUtil {
  implicit def sparkToKudu(spark:SparkSession)={
    new KuduUtil(spark)
  }

  implicit def dataframeToKudu(dataFrame: DataFrame)={
    // 需要判断dataframe是否有值，如果为空直接抛出异常
    if (null == dataFrame){
      throw new RuntimeException("dataFrame中数据为空")
    }
    new KuduUtil(dataFrame)
  }
}
