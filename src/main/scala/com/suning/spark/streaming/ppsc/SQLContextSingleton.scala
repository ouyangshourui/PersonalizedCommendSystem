package com.suning.spark.streaming.ppsc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
/**
 * Created by 14070345 on 2015/12/9 0009.
 */
object SQLContextSingleton {
  @transient private var instance: HiveContext = null
  def getInstance(sparkContext: SparkContext): HiveContext = synchronized{
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}
