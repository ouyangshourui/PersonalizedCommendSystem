package com.suning.spark.streaming

import org.apache.spark.SparkConf

import scala.beans.BeanProperty

/**
 * Created by 14070345 on 2015/12/7 0007.
 */
object SparkConstant{
  @BeanProperty
  var sparkDefaultParallelism="200" // 3*core
  @BeanProperty
  var sparkStreamingReceiverMaxRate="1000"
  @BeanProperty
  var sparkStreamingHandleRate= 1
  @BeanProperty
  var sparkStreamingBlockInterval="200" //default value is 200
 /* @BeanProperty
  val appJarPath="/home/spark/workspace/ppcs/ppcs.streaming-1.0-SNAPSHOT-jar-with-dependencies.jar"*/
  def initConf(conf:SparkConf):SparkConf={
    conf.set("spark.default.parallelism",sparkDefaultParallelism)
    conf.set("spark.streaming.receiver.maxRate",sparkStreamingReceiverMaxRate)
    conf.set("spark.streaming.blockInterval",sparkStreamingBlockInterval)
    conf.set("spark.executor.logs.rolling.strategy","size")
    conf.set("spark.executor.logs.rolling.maxSize","134217728")  //128M
    conf.set("spark.executor.logs.rolling.maxRetainedFiles","8")
    conf.set("spark.speculation","true")
    val maxCores=conf.get("spark.cores.max").toInt
    if(maxCores > 0){
      conf.set("spark.default.parallelism",(3*maxCores).toString)
      conf.set("spark.sql.shuffle.partitions",(3*maxCores).toString)
    }
    conf
  }
}
