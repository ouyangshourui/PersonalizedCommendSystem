package com.suning.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 14070345 on 2015/12/8 0008.
 */
object Receiver{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FixedAdvertising_receiver_test")
    val initConf = SparkConstant.initConf(sparkConf)
    val ssc = new StreamingContext(initConf, Seconds(1))
    val topicMap = ApplicationConstant.getTopics.split(",")
      .map((_, ApplicationConstant.getNumThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, ApplicationConstant.getZkQuorum,
      ApplicationConstant.getGroup, topicMap)
    lines.saveAsTextFiles(ApplicationConstant.getCheckpointDirectory,System.currentTimeMillis().toString)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
