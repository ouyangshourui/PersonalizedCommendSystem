package com.suning.spark.streaming.kafka

import java.util.Random

import com.suning.spark.streaming.ppsc.KafkaProducerSingleton
import com.suning.spark.streaming.{ApplicationConstant, SparkConstant}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 14070345 on 2015/12/4 0004.
 */
object KafkaMessageStandLoneProducer {
  val head="{\"cookieId\":\"cookieId"
  val tail = "\",\"memberId\":\"memberId123\",\"imei\":\"862594020474101\",\"userType\":\"4\",\"appInfo\":\"3.7.2|ANDROID|5670987e5f904694\",\"sublist\":[{\"flag\":\"11\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.2370\",\"simGdsId\":\"104022929\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.2258\",\"simGdsId\":\"103592303\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401002\",\"score\":\"0.1682\",\"simGdsId\":\"104141259\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.1082\",\"simGdsId\":\"103282872\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0888\",\"simGdsId\":\"102569364\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0719\",\"simGdsId\":\"104600390\"},{\"flag\":\"12\",\"gdsPrice\":\"\",\"l1GdsGroupDd\":\"\",\"l4GdsGroupDd\":\"R0401003\",\"score\":\"0.0689\",\"simGdsId\":\"103592304\"}]}"
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
      //val pnum=scala.util.Random.nextInt(5000)+5000
      val num=handleRecode  // sa log
      val rdd=sc.parallelize(1 to num,num/100).mapPartitions { p => {
        val producer = KafkaProducerSingleton.getInstance
        p.foreach { r => {
          val random = scala.util.Random.nextInt(userNum)  //user
          val mess = head + random + tail
          val message = new ProducerRecord[String, String](ApplicationConstant.getTopics, "message"+random, mess)
          producer.send(message)
        }
        }
        p
      }
      }
      rdd.collect()
      rdd.unpersist()
      Thread.sleep(handleInterval*1000)
    }
    sc.stop()
    }
}
