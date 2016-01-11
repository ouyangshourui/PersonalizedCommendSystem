package com.suning.spark.streaming.ppsc

import java.util
import java.util.HashMap

import com.suning.spark.streaming.ppcs.common.RedisUtils
import com.suning.spark.streaming.{ApplicationConstant, SparkConstant}
import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * Created by 14070345 on 2015/12/7 0007.
 */
object FixedAdvertising {

  case class TempTow(cookeId: String, simGdsId: String, score: String, l4GdsGroupDd: String)

  /**
   * Recoverable for driver failure
   * @param checkpointDirectory
   * @return StreamingContext
   */
  def createContext(checkpointDirectory: String, args: Array[String]):
  StreamingContext = {
    val sparkConf = new SparkConf().setAppName("FixedAdvertising")
    sparkConf.set("driverproxyuser",args(2))
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
   * select  (cookeId,simGdsId,score,l4GdsGroupDd)
   * @param recode
   * @return
   */
  // cookeid memid imei ->one
  def ParserJsonRecode(recode: String): Array[(String, String, String, String,String)] = {
    val arrayBuffer = new ArrayBuffer[(String, String, String, String,String)]()
    var recodeJson: JSONObject = null
    try {

      recodeJson = JSONObject.fromObject(recode)
      val cookieId = recodeJson.getString(ApplicationConstant.cookieId)
      val memberId = recodeJson.getString(ApplicationConstant.memberId)
      val imei = recodeJson.getString(ApplicationConstant.imei)
      val userType = recodeJson.getString(ApplicationConstant.userType)

      val jsonArray = recodeJson.getJSONArray(ApplicationConstant.sublist).toArray()
      jsonArray.foreach(
        recode => {
          val rJson = JSONObject.fromObject(recode)
          val simGdsId = rJson.getString(ApplicationConstant.simGdsId)
          val score = rJson.getString(ApplicationConstant.score)
          val l4_group = rJson.getString(ApplicationConstant.l4_group)
          var recKey = cookieId
          if(userType.equals("0")){
            recKey = memberId
          }else if(userType.equals("2")|| userType.equals("4")){
            recKey = imei
          }
          println("recKey,simGdsId,score,l4_group,userType:" + recKey + "," + simGdsId + "," + score + "," + l4_group + "," + userType)
          var t = (recKey, simGdsId, score, l4_group ,userType)
          arrayBuffer += t
        }
      )
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    arrayBuffer.toArray
  }

  /**
   *
   * @param r1
   * @param r2
   * @return select max score recode
   */

  def MaxScore(r1: ((String, String, String, String), Long),
               r2: ((String, String, String, String), Long))
  : ((String, String, String, String), Long) = {
    if (r1._1._1.toFloat > r2._1._1.toFloat) {
      r1
    } else
      r2
  }

  /*
    /**
     * select l3_group:(cookeId,simGdsId,score,l4GdsGroupDd)
     * ->(cookeId,simGdsId,l3GdsGroupDd),(score,l4GdsGroupDd)
     * @param rdd  RDD
     * @param metaDF  DataFrame
     * @return (cookeId,simGdsId,score,l4GdsGroupDd,l3GdsGroupDd,brandNum)
     */
    def recordJoinMeta(rdd: RDD[(String, String, String, String)], metadf: DataFrame): RDD[((String, String, String), (String, String))] = {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      Thread.currentThread().setContextClassLoader(FixedAdvertising.getClass.getClassLoader())
      val df = rdd.map(w => TempTow(w._1, w._2, w._3, w._4)).toDF
      println("recordJoinMeta")
      /**
       * (cookeId: String,simGdsId:String,score:String,l4GdsGroupDd:String,+
       * gds_cd String,l4_gds_group_cd String,l1_gds_group_cd String,brand_cd String ,catgroup3_id String)
       */
      // val resultDF= df.join(catgroup3TableDF,catgroup3TableDF("l4_gds_group_cd")<=>df("l4GdsGroupDd"))
      val resultDF = df.join(metadf, metadf("gds_cd") <=> df("simGdsId"))
        .select("cookeId", "simGdsId", "catgroup3_id", "score", "l4GdsGroupDd")
      //val resultRDD= sqlContext.sql(ApplicationConstant.catgroup3_idSql).rdd
      resultDF.rdd.map(row => ((row.getString(0), row.getString(1), row.getString(2)), (row.getString(3), row.getString(4))))
    }*/

  /**
   * (cookeId: String,simGdsId:String,score:String,l4GdsGroupDd:String,+
   * gds_cd String,l4_gds_group_cd String,l1_gds_group_cd String,brand_cd String ,catgroup3_id String)
   */
/*  def recordJoinMeta(rdd: RDD[(String, String, String, String)], metaDF: DataFrame): RDD[((String, String, String), (String, String))] = {
    rdd.map { recode => {
      Console.err.println("***************8gds_cd:" + recode._2)
      val catGroup3_idRow = metaDF.where(metaDF("gds_cd") === recode._2).select("catgroup3_id")
      val recode._2 = catGroup3_idRow.first().getString(0)
      ((recode._1, recode._2, recode._2), (recode._3, recode._4))
    }
    }
  }*/

 /* def orderFilter(recode: (String, String, String, String, String), memCat3GpDF: DataFrame): Boolean = {
      var isFilter = true
      //1. usertype = 1
      //2.get redis   hive --> redis
      //3. filter

      if(recode._5.equals("1")) {
        val memberId = recode._1
        val cat3GrpId = recode._2
        var catentry3IdFilter = RedisUtils.get("ppcs_oms_catentry3Id_") + memberId
        if(null==catentry3IdFilter) {
            val catGroup3_idRow = memCat3GpDF.where(memCat3GpDF("member_id") === memberId).select("catentry3_id")
            catentry3IdFilter = catGroup3_idRow.first().getString(0)
            Console.err.println("memberId,l3Catgorup,isFilter before" + memberId + ":" + catentry3IdFilter + ":" + isFilter)
            RedisUtils.set("ppcs_oms_catentry3Id_" + memberId,catentry3IdFilter,1000)
        }
        Console.err.println("memberId,l3Catgorup" + memberId + ":" + catentry3IdFilter)
        isFilter = !catentry3IdFilter.contains(cat3GrpId)
        Console.err.println("memberId,l3Catgorup,isFilter" + memberId + ":" + catentry3IdFilter + ":" + isFilter)
/*        def matchlCatGroup {
            jsonArray.foreach(
              recode => {
                val rJson = JSONObject.fromObject(recode)
                val l3Catgorup = rJson.getString(ApplicationConstant.l3Catgroup)
                Console.err.println("memberId,l3Catgorup" + memberId + ":" + l3Catgorup)
                if (cat3GrpId.equals(l3Catgorup)) {
                  isFilter = false
                }
              }
            )
        }
        matchlCatGroup*/
      }
      isFilter
  }
*/
  /**
   * (cookeId: String,simGdsId:String,score:String,l4GdsGroupDd:String,+
   * gds_cd String,l4_gds_group_cd String,l1_gds_group_cd String,brand_cd String ,catgroup3_id String)
   * ((recKey, simGdsId, score, l4_group ,userType)->
   * ((recKey,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd,userType))
   */
  def recordJoinMeta1(rdd: RDD[(String, String, String, String,String)], metaDF:DataFrame): RDD[((String, String, String), (String, String, String, String,String))] = {
    //collect recode._2 list
    val keymap = new util.HashMap[String, (String, String, String)]()
    rdd.map(r => r._2).collect().filter(!_.isEmpty).foreach(keymap.put(_, null))
    Console.err.println("#################:keymap.size() ::" + keymap.size())
    val it = keymap.keySet().iterator()
    while (it.hasNext) {
        val key = it.next()
        val catGroup3_idRow = metaDF.where(metaDF("gds_cd") === key).select("catgroup3_id", "l1_gds_group_cd", "brand_cd")
        if (catGroup3_idRow.count() > 0)
        {
            val catGroup3_id = catGroup3_idRow.first().getString(0)
            val l1_gds_group_cd = catGroup3_idRow.first().getString(1)
            val brand_cd = catGroup3_idRow.first().getString(2)
            keymap.put(key, (catGroup3_id, l1_gds_group_cd, brand_cd))
        }
    }
    val keymapBC = rdd.sparkContext.broadcast(keymap)
    val resultRDD = rdd.map { recode => {
      val catGroup3_id = keymapBC.value.get(recode._2)._1
      val l1_gds_group_cd = keymapBC.value.get(recode._2)._2
      val brand_cd = keymapBC.value.get(recode._2)._3
      ((recode._1, catGroup3_id, brand_cd), (recode._3, recode._4, recode._2, l1_gds_group_cd,recode._5))
    }
    }
    keymap.clear()
    keymapBC.unpersist()
    resultRDD
  }

  /**
    * ((recKey,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd,userType)) ->
    *
    * @param rdd
    * @param memCatGroup3IdTableDF
    * @return
    */
  def recordFilterMeta(rdd: RDD[((String, String, String), (String, String, String, String,String))], memCatGroup3IdTableDF: DataFrame): RDD[((String, String, String), (String, String, String, String))] = {

      val keymap = new util.HashMap[String,String]()

      rdd.filter(x=>x._2._5.equals("1")).map(r => r._1._1).collect().foreach(x=>keymap.put(x, null))
      println("***************:keymap.size() ::" + keymap.size())
      val it = keymap.keySet().iterator()
      while (it.hasNext) {
        val key = it.next()
        var catentry3IdFilter = RedisUtils.get("ppcs_oms_catentry3Id_" + key)
        println("***************:recordFilterMeta  redis:: catentry3IdFilter  " + catentry3IdFilter)
        if (null == catentry3IdFilter) {
          val catGroup3_idRow = memCatGroup3IdTableDF.where(memCatGroup3IdTableDF("member_id") === key).select("catentry3_id").limit(1)
          if (catGroup3_idRow.count() > 0) {
            catentry3IdFilter = catGroup3_idRow.first().getString(0)
            if (catentry3IdFilter != null) {
              RedisUtils.set("ppcs_oms_catentry3Id_" + key, catentry3IdFilter, 5000)
            }
          }
        }

        if (catentry3IdFilter != null) {
            keymap.put(key, catentry3IdFilter)
        }
    }
        val keymapBC = rdd.sparkContext.broadcast(keymap)
        val resultRDD = rdd.filter { recode => {
          var isFilter = true
          if(recode._2._5.equals("1"))
          {
            val memberId = recode._1._1
            val cat3GrpId = recode._1._2
            val catGroup3IdArr = keymapBC.value.get(memberId)
            if(catGroup3IdArr!=null)
               isFilter = !catGroup3IdArr.contains(cat3GrpId)
          }
            isFilter
        }
    }.map(recode=>((recode._1._1,recode._1._2,recode._1._3),(recode._2._1,recode._2._2,recode._2._3,recode._2._4)))
    keymap.clear()
    keymapBC.unpersist()
    resultRDD
  }


  /**
   * ./kafka-console-producer.sh --broker-list "10.27.25.164:9092,10.27.25.165:9093,10.27.25.166:9094"  --topic spark-short-action-output
   * ./kafka-console-consumer.sh --zookeeper 10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181  --topic spark-short-action-output-result --group spark-test
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("usage:*jar sparkStreamingReceiverMaxRate  sparkStreamingHandleRate")
    }
    println("main args:"+args(0)+":"+args(1))
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

    // "select member_cd,catgroup3_id from bi_dm.memOrderCatGroup3Table"
    val memCatGroup3IdTableDF = sqlContext.sql(ApplicationConstant.catgroup3_memberIdSql).cache()
    memCatGroup3IdTableDF.count() //load to memeory


    Thread.currentThread().setContextClassLoader(FixedAdvertising.getClass.getClassLoader())

    /**
     * set kafka receiver
     */

    val topicMap = ApplicationConstant.getTopics.split(",")
      .map((_, ApplicationConstant.getNumThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc,
      ApplicationConstant.getZkQuorum,
      ApplicationConstant.getGroup,
      topicMap)

    /**
     * json to (cookeId,simGdsId,score,l4GdsGroupDd)
     */
    val ParserJsonRecode4DStream = lines.flatMap(x => {
      ParserJsonRecode(x._2)
    })



    /**
     * (cookeId,simGdsId,score,l4GdsGroupDd)->((cookeId,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd))
     *
     */
    val  recordJoinMetaDStream = ParserJsonRecode4DStream.transform { rdd =>
      recordJoinMeta1(rdd,catGroup3TableDF)
    }

    /**
      * (cookeId,simGdsId,score,l4GdsGroupDd)->((cookeId,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd))
      *
      */
    val recordFilterJoinMetaDStream = recordJoinMetaDStream.transform { rdd =>
      recordFilterMeta(rdd,memCatGroup3IdTableDF)
    }



    //Compute BrandNum  using stream join
    /**
     * ((cookeId,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd))
     * ->((cookeId,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd)),brandNum))
     *
     */


    val recordJoinMetaKCountDStream = recordFilterJoinMetaDStream.map(r => r._1).countByValue()
    val L3GroupBrandNumDStream = recordFilterJoinMetaDStream.join(recordJoinMetaKCountDStream)


    /**
     * ((cookeId,l3GdsGroupDd,brand_cd),(score,l4GdsGroupDd,simGdsId,l1_gds_group_cd)),brandNum))
     * ->((cookeId,l3GdsGroupDd,brand_cd),((max(score),l4GdsGroupDd,simGdsId,l1_gds_group_cd)),brandNum))
     *
     */

    val L3GroupBrandNumDStreamMaxScore = L3GroupBrandNumDStream.reduceByKey(MaxScore(_, _))



    /**
     * ((cookeId,l3GdsGroupDd,brand_cd),((score,l4GdsGroupDd,simGdsId),brandNum))
     * ->(cookeId,brand_cd ,l3GdsGroupDd,userBrandCatScore,l4GdsGroupDd,l3GdsGroupDd)
     *
     * user_brandcat_score=score*(1+aConstant*brand_num)  aConstant=0.12
     */
    val L3GroupComputeUserBrandcatScoreDStream = L3GroupBrandNumDStreamMaxScore.map(r => {
      val key = r._1 //(cookeId,simGdsId,score,l4GdsGroupDd,l3GdsGroupDd,brandNum)
      val value = r._2 //(score,l4GdsGroupDd),brandNum)
      println("L3GroupComputeUserBrandcatScoreDStream ::" + value._1._1  + ":" )
      val brandCatScore = (value._1._1).toFloat * (1 + ApplicationConstant.aConstant * r._2._2)
      val userBrandCatScore = if (brandCatScore > 1) 1 else brandCatScore
      //(key._1, key._2, value._1._1, value._1._2, value._1._3, value._2, userBrandCatScore, key._3)
      (key._1,(key._3,key._2,userBrandCatScore,value._1._4,value._1._2))
    })



    val recUserInfoDStream = L3GroupComputeUserBrandcatScoreDStream.groupByKey()

    recUserInfoDStream.foreachRDD { rdd => {
      rdd.foreachPartition { p => {
        val producer = KafkaProducerSingleton.getInstance
        p.foreach { r => {

             val sb = new StringBuilder()
             val iter = r._2.iterator
             while(iter.hasNext){
                val recode = iter.next()
                sb.append("," + String.format(ApplicationConstant.brandCat3Format,recode._1,recode._2,recode._3.toString,recode._4,recode._5))
             }

            val outPut = String.format(ApplicationConstant.recbrandCat3Format, r._1,sb.substring(1))

            println("L3GroupComputeUserBrandcatScoreDStream:" + outPut)
            val message = new ProducerRecord[String, String](KafkaProducerSingleton.getTopic, "L3GroupComputeUserBrandcatScoreDStream", outPut)
            producer.send(message)
        }
        }
      }
      }
    }
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
