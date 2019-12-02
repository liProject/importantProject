package li

import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkKuduTableDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

//      SparkContext.getOrCreate(sparkConf)
      SparkSession.builder().config(sparkConf).getOrCreate()
    }

    val KuduMaster: String = "hadoop4:7051,hadoop5:7051,hadoop6:7051"

    /**
      * class KuduContext(
      *   val kuduMaster : scala.Predef.String,
      *   sc : org.apache.spark.SparkContext)
      */
    val kuduContext: KuduContext = new KuduContext(
      KuduMaster,
      sc.sparkContext
    )

    val schema: StructType = new StructType(
      Array(
        new StructField("id", IntegerType, true),
        new StructField("name", StringType, true),
        new StructField("gender", StringType, true),
        new StructField("age", IntegerType, true)
      )
    )

    val id: Seq[String] = Seq("id")

    import scala.collection.JavaConverters._

    val tableOptions: CreateTableOptions = new CreateTableOptions()
        .addHashPartitions(List("id").asJava,3)
        .setNumReplicas(3)

    /**
      * def createTable(
      *   tableName : scala.Predef.String,
      *   schema : org.apache.spark.sql.types.StructType,
      *   keys : scala.Seq[scala.Predef.String],
      *   options : org.apache.kudu.client.CreateTableOptions)
      *  : org.apache.kudu.client.KuduTable
      */
    val table: KuduTable = kuduContext.createTable(
      "sparkKuduTable",
      schema,
      id,
      tableOptions
    )

    println(table.getTableId)

    sc.stop()
  }
}
