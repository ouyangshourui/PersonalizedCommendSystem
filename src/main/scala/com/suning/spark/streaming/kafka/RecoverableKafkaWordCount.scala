package com.suning.spark.streaming.kafka


import com.suning.spark.streaming.SparkConstant
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
/**
 * Created by 14070345 on 2015/12/4 0004.
 */
object RecoverableKafkaWordCount {

  def createContext(zkQuorum:String, group:String, topics:String,
                    numThreads:String ,checkpointDirectory: String):
                  StreamingContext ={
    val sparkConf = new SparkConf().setAppName("RecoverableKafkaWordCount")
    val ssc = new StreamingContext(SparkConstant.initConf(sparkConf), Seconds(1))
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def main(args: Array[String]) {
/*    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads> <checkpointDirectory>")
      System.exit(1)
    }
   // StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads,checkpointDirectory) = args*/
    val zkQuorum="10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181"
    val group ="sparkstreamingtest"
    val topics="spark-streaming-test1"
    val numThreads="2"
    val checkpointDirectory="/user/spark/sparkstreamingcheckpoint/"+System.currentTimeMillis()
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,()=>
    {
      createContext(zkQuorum, group, topics,numThreads,checkpointDirectory)
    })

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
