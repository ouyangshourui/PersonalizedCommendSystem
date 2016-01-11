package com.suning.spark.streaming.ppsc

import java.util

import com.suning.spark.streaming.ppcs.common.RedisUtils
import com.suning.spark.streaming.{SparkConstant, ApplicationConstant}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.collection.immutable.HashMap
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 14070345 on 2015/12/18 0018.
  */
object GdsCodeAggregation {

  /**
    * Recoverable for driver failure
    * @param checkpointDirectory
    * @return StreamingContext
    */
  def createContext(checkpointDirectory: String, args: Array[String]):
  StreamingContext = {
    val sparkConf = new SparkConf().setAppName("omsOrderFilter")
    sparkConf.set("driverproxyuser", args(2))
    // init conf
    if (args(0).toInt.isInstanceOf[Int]) {
      SparkConstant.setSparkStreamingReceiverMaxRate(args(0))
    }
    if (args(1).toInt.isInstanceOf[Int]) {
      SparkConstant.setSparkStreamingHandleRate(args(1).toInt)
    }
    val initConf = SparkConstant.initConf(sparkConf)
    val ssc = new StreamingContext(initConf, Seconds(SparkConstant.sparkStreamingHandleRate))
    //set checkpoint Directory
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  /**
    * select  memInCardNo
    * @param recode
    * @return (memInCardNo,generalGdsCode)
    */
  def ParserOMSJsonRecode(recode: String): (String, String) = {
    //println("come into ParserOMSJsonRecode function")
    var memInCardNo: String = null
    var recodeJson: JSONObject = null
    var generalGdsCode: String = null
    try {
      recodeJson = JSONObject.fromObject(recode)
      memInCardNo = recodeJson.getString(ApplicationConstant.memInCardNo)
      generalGdsCode = recodeJson.getString(ApplicationConstant.generalGdsCode)
      println("come into ParserOMSJsonRecode function memInCardNo " +memInCardNo)
      println("come into ParserOMSJsonRecode function generalGdsCode " +generalGdsCode)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    (memInCardNo, generalGdsCode)
  }


  /**
    * (memInCardNo,generalGdsCode)->(memInCardNo,generalGdsCode,三级目录ID)
    * @param rdd
    * @param  df   DataFrame
    */

  def SelectGeneralGdsCode(rdd: RDD[(String, String)], df: DataFrame):RDD[(String, String,String)] = {
    //println("come into SelectGeneralGdsCode function")
    val keymap = new util.HashMap[String, String]()
    rdd.map(x => x._2).collect().filter(!_.isEmpty).foreach(keymap.put(_, null))
    rdd.map(x=>println("x=------>" + x)).collect()
    val it = keymap.keySet().iterator()
    while (it.hasNext) {
      val key = it.next()
      val catGroup3_idRow = df.where(df("gds_cd") === key).select("catgroup3_id").limit(1)

      val catGroup3_id = catGroup3_idRow.first().getString(0)
      keymap.put(key, catGroup3_id)
    }
    println("!!!!:keymap.size() ::" + keymap.size())
    val keymapBC = rdd.sparkContext.broadcast(keymap)
    val resultRDD = rdd.map { recode => {
      println("SelectGeneralGdsCode ::: recode " + recode._1 + ":" +  recode._2 +":" +  keymapBC.value.get(recode._2))
      (recode._1, recode._2, keymapBC.value.get(recode._2))
    }
    }
    keymap.clear()
    keymapBC.unpersist()
    resultRDD
  }


  def recodeDistinct(r1:Int,r2:Int):Int={
     println("r1  @@@@@@@@@"  + r1)
    println("r1  @@@@@@@@@"  + r2)
    r1
  }


  def selRealMemCat3Data(rdd: RDD[(String, List[String])], df: DataFrame): RDD[(String,String)] = {
       // println("come into selRealMemCat3Data function")
        val keymap = new util.HashMap[String, String]()

        rdd.map(x=>println("selRealMemCat3Data  --> " + x)).collect()
        rdd.map(x => x._1).collect().filter(!_.isEmpty).foreach(x=>keymap.put(x,null))

        val it = keymap.keySet().iterator()
        while (it.hasNext) {
          val key = it.next()
          val catGroup3_idRow = df.where(df("member_id") === key).select("catentry3_id").limit(1)
          if(catGroup3_idRow.count() > 0) {
            val catGroup3_id = catGroup3_idRow.first().getString(0)
            keymap.put(key, catGroup3_id)
          }
        }

        val keymapBC = rdd.sparkContext.broadcast(keymap)
        val resultRDD = rdd.map { recode => {

          var cat3IdMemHis = RedisUtils.get("ppcs_oms_catentry3Id_" + recode._1)
          if (cat3IdMemHis == null) {
              cat3IdMemHis =  keymapBC.value.get(recode._1) //from hive
          }
          if(cat3IdMemHis == null){
              (recode._1,recode._2.mkString(","))
          }else{
              recode._2.foreach(x=>{
                   if(!cat3IdMemHis.contains(x)){
                       cat3IdMemHis = cat3IdMemHis.concat("," + x)
                   }
              })
              (recode._1,cat3IdMemHis)
          }
         }
        }
        keymap.clear()
        keymapBC.unpersist()
        resultRDD

  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("usage:*jar sparkStreamingReceiverMaxRate  sparkStreamingHandleRate driverproxyuser")
      System.exit(-1)
    }
   // System.setProperty("user.name", args(2))
   // System.setProperty("HADOOP_USER_NAME", args(2))

    println("main args:" + args(0) + ":" + args(1))
    val ssc = StreamingContext.getOrCreate(ApplicationConstant.getCheckpointDirectory
      , () => createContext(ApplicationConstant.checkpointDirectory, args))

    /**
      * load l3_group table in hive
      */

    // cache table
    val sqlContext = new HiveContext(ssc.sparkContext)
    // "select gds_cd,catgroup3_id from bi_dm.catgroup3Table sort by gds_cd"
    val catGroup3TableDF = sqlContext.sql(ApplicationConstant.cacheL3GroupTableSql).cache()
    catGroup3TableDF.count() //load to memeory

    val catGroup3MemTableDF = sqlContext.sql(ApplicationConstant.catgroup3_memberIdSql).cache()
    catGroup3MemTableDF.count() //load to memeory

    Thread.currentThread().setContextClassLoader(GdsCodeAggregation.getClass.getClassLoader())

    /**
      * set kafka receiver
      */

    val topicMap = ApplicationConstant.OMStopics.split(",")
      .map((_, ApplicationConstant.getNumThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc,
      ApplicationConstant.getZkQuorum,
      ApplicationConstant.getOmsordergroup,
      topicMap)


    /**
      * recode->(memInCardNo,generalGdsCode)
      */

    val parserOMSJsonRecode4DStream = lines.map(x =>
      ParserOMSJsonRecode(x._2)
    ).map(x => {
       println("xxxxxxxxxxxxxx::: " + x)
      (x, 1)
    })
      .reduceByKey(recodeDistinct(_, _)) //recodeDistinct
      .map(x => {
             println("reduce :::: x._1._1 " +  x._1._1 )
             println("reduce :::: x._1._2 " +  x._1._2 )
            (x._1._1, x._1._2)
      }
      )


    /**
      *
      * select dataframe catGroup3TableDF
      * (memInCardNo,generalGdsCode)->(memInCardNo,generalGdsCode,三级目录ID)
      *
      */
    val hiveJoinGeneralGdsCodeDStream = parserOMSJsonRecode4DStream.transform { rdd =>
      SelectGeneralGdsCode(rdd, catGroup3TableDF)
    }

 /*   /**
      * aggregation generalGdsCode with same memInCardNo
      * (memInCardNo,generalGdsCode,三级目录ID)->(memInCardNo,List(generalGdsCode),三级目录ID)
      *
      */

    val aggregationRedisGdsCodeDStream = hiveJoinGeneralGdsCodeDStream.mapPartitions { p => {
      val generalGdsCodeMap=new util.HashMap[(String,String),List[String]]()  //((memInCardNo,三级目录ID),generalGdsCode)
      val redisMap=new util.HashMap[String,List[String]]()  // redis result ,redis connect singleton can be used
      p.map{r => {
        generalGdsCodeMap.put((r._1, r._3), List(r._2) ::: redisMap.get(r._1))
        (r._1, List(r._2) ::: redisMap.get(r._1), r._3)
      }
      }
    }
    }*/
    /**
      * aggregation generalGdsCode with same memInCardNo
      * (memInCardNo,generalGdsCode,三级目录ID)->(memInCardNo,List(三级目录ID))
      *
      */
    val aggregationGdsCodeDStream = hiveJoinGeneralGdsCodeDStream.mapPartitions { recode => {
      //println("come into aggregationGdsCodeDStream function")
            var keymap = HashMap[String,List[(String)]]()
            //  keymap += ("3" -> List("3"))
            while(recode.hasNext){
              val r = recode.next()
              if(keymap.contains(r._1)){
                println("aggregationGdsCodeDStream r._1" + r._1 )
                println("aggregationGdsCodeDStream r._3" + r._3 )
                keymap += (r._1 -> keymap(r._1).::(r._3))
              }else {
                println("aggregationGdsCodeDStream r._1 else " + r._1 )
                println("aggregationGdsCodeDStream r._3 else" + r._3 )
                keymap += (r._1 -> List(r._3))
              }
            }
            keymap.iterator
      }
    }



    /**
      *
      * select dataframe catGroup3MemTableDF
      * (memInCardNo,(三级目录ID1,三级目录ID2）)
      *
      */
     val realMemCat3DataStream = aggregationGdsCodeDStream.transform { rdd =>
        selRealMemCat3Data(rdd, catGroup3MemTableDF)
     }

    //save redis
    realMemCat3DataStream.foreachRDD(rdd=>{
          //println("xxxxxxxxxxxxxxxxxxxxxxx")
          rdd.foreachPartition(p=> {
             // println("come into realMemCat3DataStream foreachRDD")

              p.foreach(x => {



                println("come into realMemCat3DataStream foreachRDD x._1 " + x._1)
                println("come into realMemCat3DataStream foreachRDD x._2 " + x._2)
                RedisUtils.set("ppcs_oms_catentry3Id_" + x._1, x._2, 1000)
            })

          })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}



