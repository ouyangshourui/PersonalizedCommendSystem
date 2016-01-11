package com.suning.spark.streaming.kafka

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 14070345 on 2015/12/7 0007.
 */
object UnionTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCountProducer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val rdd1=sc.parallelize(List(('a','c',1),('b','a',1),('b','d',8)))
    val rdd2=sc.parallelize(List(('a','c',2),('b','c',5),('b','d',6)))
    val rdd3=rdd1.union(rdd2)
    val rdd4=rdd3.map(x=>{
      (x._1+":"+x._2,x)
    }).groupByKey()
    rdd3.collect().map(x=>{
      println(x._1+":"+x._2+":"+x._3)
    })

    rdd4.collect().map(x=>{
      println(x._1+":"+x._2)
    })
  }

}
