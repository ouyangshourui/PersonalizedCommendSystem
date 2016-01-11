package com.suning.spark.streaming

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by 14070345 on 2015/12/11 0011.
 */
object RDDNULL {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveTest").setMaster("local")
    val initConf = SparkConstant.initConf(sparkConf)
    val sc = new SparkContext(initConf)
    val resultrdd= sc.parallelize(1 to 20 ,4).map{x=>{
      val arrayBuffer = new ArrayBuffer[Int]()
      if(x%2==0)
        arrayBuffer +=x
      else
        arrayBuffer
      arrayBuffer.toArray
    }
    }
    resultrdd.filter(_.length>0).collect().foreach(x=>println(x(0)))
  }

}
