package spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadcastAccu extends App {

  val sc: SparkContext = {

    val conf: SparkConf = new SparkConf()

    conf
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

    val context: SparkContext = SparkContext.getOrCreate(conf)

    context
  }

  private val textRDD: RDD[String] = sc.textFile("/mydata/wordcount.data")

  //字典数据，只要有这些单词，就过滤：特殊字符存储列表List中
  private val list: List[String] = List(",", ".","!","#","$","%")

  //通过广播变量将list广播到各个Executor内存中，便于多个task使用
  private val broadcast: Broadcast[List[String]] = sc.broadcast(list)

  //定义累加器，记录单词为符号数据的个数
  private val accumulator: LongAccumulator = sc.longAccumulator("number_accu")

  private val wrodsRDD: RDD[String] = textRDD
    .filter(line => line.trim.length > 0 && null != line)
    .flatMap { line =>
      line.trim.split("\\s+")
    }
    .filter { word =>
      //获取符合列表 从广播变量中获取列表list的值
      val listvalue = broadcast.value

      //判断单词是否为符号数据，如果是就过滤掉
      val isFlag = listvalue.contains(word)

      //如果单词是否为符号数据，如果是就过滤掉
      if (isFlag) accumulator.add(1L)

      !isFlag
    }

  wrodsRDD.mapPartitions{ iter =>
    iter.map( word => (word,1))
  }
    .reduceByKey( (temp, item) => temp + item)
    .foreachPartition{ iter =>
      iter.foreach(println)
    }

  println("累加器前")
  println(s"过滤符合数据的个数为：${accumulator.value}")
  println("累加器后")


  Thread.sleep(100000000)

  sc.stop()

}
