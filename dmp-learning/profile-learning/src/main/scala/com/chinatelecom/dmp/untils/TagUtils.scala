package com.chinatelecom.dmp.untils

/**
  * (AD@banner->1.0),(AGE@17->1.0)
  */
object TagUtils {
  def tagMapToStr(tagMap:Map[String,Double]) ={
    tagMap
      .toList
      .sortBy(_._1)
      .map{case (tagName,tagValue) => s"($tagName->$tagValue)"}
      .mkString(",")
  }

  def idsMapToStr(idsMap:Map[String,String])={
    idsMap
      .toList
      .sortBy(_._1)
      .map{case(idsName,idsValue) => s"($idsName->$idsValue)"}
      .mkString(",")
  }

  def tagStrToMap(tagStr:String) ={
    tagStr
      .split("\\,")
      .map { line =>
        val Array(tagKey, tagValue) = line.stripSuffix(")").stripPrefix("(").split("->")
        (tagKey, tagValue.toDouble)
      }
      .toMap
  }

  def idsStrToMap(idsStr:String) ={
    idsStr
      .split("\\,")
      .map{ line =>
        val Array(idsKey, idsValue) = line.stripSuffix(")").stripPrefix("(").split("->")
        (idsKey, idsValue)
      }
      .toMap
  }
}
