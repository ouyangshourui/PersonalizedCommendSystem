package com.suning.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive._


/**
 * Created by 14070345 on 2015/12/9 0009.
 */
object HiveTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveTest")
    val initConf = SparkConstant.initConf(sparkConf)
    val sc = new SparkContext(initConf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    val dataDF=hiveContext.sql("select * from bi_dm.catgroup3Table ").cache
    dataDF.count()

    sc.parallelize(1 to 20,4).map(x=>{
      val result=dataDF.select("l4_gds_group_cd","catgroup3_id").limit(1).collect
      result(x-1).getString(0)
    }).collect()
  }
}
