package com.suning.spark.streaming.kafka

import java.util.{Random, HashMap}

import com.suning.spark.streaming.{SparkConstant, ApplicationConstant}
import com.suning.spark.streaming.ppsc.KafkaProducerSingleton
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 14070345 on 2015/12/4 0004.
 */
object KafkaMessageProducer {
  val head="{\"cookieId\":\"cookieId"
  val tail = "\",\"memberId\":\"memberId123\",\"imei\":\"862594020474101\",\"userType\":\"4\",\"appInfo\":\"3.7.2|ANDROID|5670987e5f904694\",\"sublist\":[{\"flag\":\"11\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.2370\",\"simGdsId\":\"104022929\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.2258\",\"simGdsId\":\"103592303\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401002\",\"score\":\"0.1682\",\"simGdsId\":\"104141259\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.1082\",\"simGdsId\":\"103282872\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0888\",\"simGdsId\":\"102569364\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0719\",\"simGdsId\":\"104600390\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0689\",\"simGdsId\":\"103592304\"}]}"
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaMessageProducer")
    val random = new Random(16)
    val sc = new SparkContext(sparkConf)
    while(true){
      val rdd=sc.parallelize(1 to 16+random.nextInt(),16).mapPartitions { p => {
        val producer = KafkaProducerSingleton.getInstance
        val random = new Random(8)
        p.foreach { r => {
          val mess = head + random.nextInt() + tail
          val message = new ProducerRecord[String, String](ApplicationConstant.getTopics, "message", mess)
          producer.send(message)
        }
        }
        p
      }
      }
      rdd.collect()
      rdd.unpersist()
      //Thread.sleep(SparkConstant.sparkStreamingDelRate*100)
    }
    sc.stop()
    }
}
