package com.chinatelecom.dmp.process

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait ReportProcessor extends Logging{

  def realProcessData(df:DataFrame,groupField:Seq[String]):DataFrame

  /**
   * @Author li
   * @Description
   * @Date 21:34 2019/12/1
   * @Param [odsDF, tableName, tableKeyField]
   * @return void
   **/
  def processData(odsDF: DataFrame,tableName:String,tableKeyField:Seq[String]) = {
    // 获取SparkSession实例对象
    val spark = odsDF.sparkSession

    spark.sparkContext.setLogLevel("WARN")

    val groupField: Seq[String] = tableKeyField.drop(1)

    // TODO: 第三、按照省市分组，统计结果：编写SQL分析
    val reportDF: DataFrame = realProcessData(odsDF,groupField)

    // TODO:  第四、结果数据保存到Kudu表中
    import com.chinatelecom.dmp.untils.KuduUtil._
    // a. 当表不存在时，创建表
    spark.createKuduTable(
      tableName, reportDF.schema, tableKeyField, 3,isDelete = false
    )
    // b. 保存数据
    reportDF.saveAsKuduTable(tableName)
  }
}
