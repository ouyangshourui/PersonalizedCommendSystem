package com.suning.spark.streaming.ppsc

import com.suning.spark.streaming.ApplicationConstant
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by 14070345 on 2015/12/10 0010.
 */
object Catgroup3TableSingleton {
  @transient private var instance: DataFrame = null
  def getInstance(sparkContext: SparkContext): DataFrame = synchronized{
    if (instance == null) {
      val sqlContext = new HiveContext(sparkContext)
       instance=sqlContext.sql(ApplicationConstant.cacheL3GroupTableSql)
    }
    instance
  }

}
