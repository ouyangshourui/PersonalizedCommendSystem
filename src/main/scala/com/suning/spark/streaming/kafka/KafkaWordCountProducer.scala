package com.suning.spark.streaming.kafka
import java.util.HashMap

import com.suning.spark.streaming.SparkConstant
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 14070345 on 2015/12/4 0004.
 */
object KafkaWordCountProducer {
  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }*/
   // val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCountProducer")
    val sc = new SparkContext(sparkConf)
    sc.parallelize(1 to 4,4).mapPartitions(
    p =>{
      val brokers= "10.27.25.164:9092,10.27.25.165:9093,10.27.25.166:9094";
      val topic ="spark-streaming-test1"
      val messagesPerSec="1000"
      val wordsPerMessage="100"
      val props = new HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
     p.map(x=>{
       println(x)
       var i =0;
         while(i<messagesPerSec.toInt) {
         (1 to messagesPerSec.toInt).foreach(messageNum => {
           val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")
           val message = new ProducerRecord[String, String](topic, str)
           producer.send(message)
         })
           i +=1
       }

     })
    }
    ).collect()
  //  Thread.sleep(SparkConstant.sparkStreamingDelRate)
    sc.stop()
    }



}
