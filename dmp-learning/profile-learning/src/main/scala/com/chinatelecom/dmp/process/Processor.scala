package com.chinatelecom.dmp.process

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait Processor extends Logging{
  def processor(df: DataFrame):DataFrame
}
