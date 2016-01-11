package com.suning.spark.streaming.ppsc

import com.suning.spark.streaming.{ApplicationConstant, SparkConstant}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
 * Created by 14070345 on 2015/12/29 0029.
 */
object RecoverableUtil {

  /**
   * Recoverable for driver failure
   * @return StreamingContext
   */
  def createContext(sparkConf:SparkConf,checkpointDirectory:String,sparkStreamingHandleRate:Int): StreamingContext = {
    val initConf = SparkConstant.initConf(sparkConf)
    val ssc = new StreamingContext(initConf, Milliseconds(sparkStreamingHandleRate))
    //set checkpoint Directory
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

}
