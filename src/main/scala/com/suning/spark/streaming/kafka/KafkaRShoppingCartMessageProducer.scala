package com.suning.spark.streaming.kafka

import com.suning.spark.streaming.ApplicationConstant
import com.suning.spark.streaming.ppsc.KafkaProducerSingleton
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 14070345 on 2015/12/4 0004.
 */
object KafkaRShoppingCartMessageProducer {

  def main(args: Array[String]) {
    if(args.length< 4){
      println("usage:**jar handleInterval handleRax  usernum  // sleep handleRax sec ,default value is 100 sec")
      System.exit(-1)
    }
    System.setProperty("user.name",args(3))
    System.setProperty("HADOOP_USER_NAME",args(3))
    val sparkConf = new SparkConf().setAppName("KafkaMessageStandLoneProducer")
    sparkConf.set("driverproxyuser",args(3))
    val sc = new SparkContext(sparkConf)
    var handleInterval=100
    var handleRecode=1000
    var userNum=200
    if (args(0).toInt.isInstanceOf[Int]) {
      handleInterval=args(0).toInt
    }
    if (args(1).toInt.isInstanceOf[Int]) {
      handleRecode=args(1).toInt
    }
    if (args(2).toInt.isInstanceOf[Int]) {
      userNum=args(2).toInt
    }

    while(true){
      val num=handleRecode  // sa log
      val rdd=sc.parallelize(1 to num,num/100).mapPartitions { p => {
        val producer = KafkaProducerSingleton.getInstance
        p.foreach { r => {
          val visitorId="{\"visitor_id\":\""+scala.util.Random.nextInt(userNum)+"\","
          val generalCd= "\"general_cd\": \""+scala.util.Random.nextInt(userNum)+"\"}"
          val mess = visitorId+generalCd
          val message = new ProducerRecord[String, String]("spark-short-shoppingCart",visitorId, mess)
          producer.send(message)
        }
        }
        p
      }
      }
      rdd.collect()
      rdd.unpersist()
      Thread.sleep(handleInterval)
    }
    sc.stop()
    }
}
