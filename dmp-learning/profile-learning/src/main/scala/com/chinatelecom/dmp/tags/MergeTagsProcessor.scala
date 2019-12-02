package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.bean.IdsWithTags
import com.chinatelecom.dmp.process.Processor
import com.chinatelecom.dmp.untils.{SparkSessionUtils, TagUtils}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object MergeTagsProcessor extends Processor {

  override def processor(df: DataFrame): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val allTagRDD: RDD[IdsWithTags] = df.as[IdsWithTags].rdd


    val vertexRDD: RDD[(VertexId, String)] = allTagRDD
      .mapPartitions { iter =>
        iter.flatMap { case IdsWithTags(mainId, ids, tags) =>
          val mainIdVer = s"mainId@$mainId#$tags"
          val idsMap: Map[String, String] = TagUtils.idsStrToMap(ids)
          val list: ListBuffer[(VertexId, String)] = new scala.collection.mutable.ListBuffer[(VertexId, String)]
          list += mainId.hashCode.toLong -> mainIdVer
          idsMap.map { tuple =>
            val idVertex: String = s"${tuple._1}->${tuple._2}"
            list += idVertex.hashCode.toLong -> idVertex
          }
          list
        }
      }
      //这是针对所有数据的，图构建看边的。顶点相同的去除
      .distinct()

    val edgeRDD: RDD[Edge[String]] = allTagRDD
      .mapPartitions { iter =>
        iter.flatMap { case IdsWithTags(mainId, ids, _) =>
          val list: ListBuffer[Edge[String]] = new ListBuffer[Edge[String]]
          val mainIdVer: VertexId = mainId.hashCode.toLong
          val idsMap: Map[String, String] = TagUtils.idsStrToMap(ids)
          idsMap.map { tuple =>
            val idVertex: VertexId = s"${tuple._1}->${tuple._2}".hashCode.toLong
            list += Edge(mainIdVer, idVertex, "")
          }
          list
        }
      }


    val graph: Graph[String, String] = Graph(vertexRDD, edgeRDD)
    // 将图的数据缓存，由于图中算法属于迭代算法，反复使用顶点和边的数据
    graph.cache()

    val ccGraph: Graph[VertexId, String] = graph.connectedComponents()

    logWarning(s"Vertex 数目：${graph.vertices.count()}")
    logWarning(s"Edge 数目：${graph.edges.count()}")

    val tagRDD: RDD[IdsWithTags] = ccGraph
      .vertices
      .join(vertexRDD)
      .map { case (_, (ccVertexId, attr)) =>
        (ccVertexId, attr)
      }
      .groupByKey()
      .values
      .mapPartitions { iters =>
        iters.map { iter =>
          val list: List[String] = iter.toList

          val mainId: String = list
            .filter(_.startsWith("mainId"))
            .head
            .split("#")(0)
            .split("@")(1)


          val idsMap: Map[String, String] = list
            .filter(!_.startsWith("mainId"))
            .map { vertexAttr =>
              val Array(idsName, idsValue) = vertexAttr.split("->")
              (idsName, idsValue)
            }
            .toMap


          val userTags: String = list
            .filter(_.startsWith("mainId"))
            .map(vertexAttr => vertexAttr.stripPrefix("mainId@").split("#")(1))
            .reduce { (tmpTags, tags) =>
              val tmpTagsMap: Map[String, Double] = TagUtils.tagStrToMap(tmpTags)
              val tagsMap: Map[String, Double] = TagUtils.tagStrToMap(tags)
              val newTagsMap: Map[String, Double] = tagsMap.map { case (tag, weight) =>
                if (tmpTagsMap.contains(tag)) {
                  tag -> (weight + tmpTagsMap.get(tag).get)
                } else {
                  tag -> weight
                }
              }

              TagUtils.tagMapToStr(tmpTagsMap ++ newTagsMap)
            }
          IdsWithTags(mainId, TagUtils.idsMapToStr(idsMap), userTags)
        }
      }

    graph.unpersist()
    tagRDD.toDF()
  }


//  def main(args: Array[String]): Unit = {
//    val spark: SparkSession = SparkSessionUtils.getSparkSession(this.getClass.getSimpleName.stripPrefix("$"))
//    import com.chinatelecom.dmp.untils.KuduUtil._
//
//    val tagsDF: DataFrame = spark.readKudu("tags_20191201").get
//
//    processor(tagsDF)
//  }
}
