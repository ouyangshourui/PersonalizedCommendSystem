package com.suning.spark.streaming.ppsc

import com.suning.spark.streaming.{ApplicationConstant, SparkConstant}
import net.sf.json.JSONObject
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{HashPartitioner, Logging, SparkConf}

/**
 * Created by 14070345 on 2015/12/29 0029.
 */
object RecommendBasedShoppingCart extends Logging {

  def init(args: Array[String]): SparkConf = {
    System.setProperty("user.name", args(2))
    System.setProperty("HADOOP_USER_NAME", args(2))
    ApplicationConstant.setCheckpointDirectory(args(4))
    val sparkConf = new SparkConf().setAppName(args(3))
    sparkConf.set("driverproxyuser", args(2))
    // init conf
    if (args(0).toInt.isInstanceOf[Int]) {
      SparkConstant.setSparkStreamingReceiverMaxRate(args(0))
    }
    if (args(1).toInt.isInstanceOf[Int]) {
      SparkConstant.setSparkStreamingHandleRate(args(1).toInt)
    }

    ApplicationConstant.setTopics("spark-short-shoppingCart")
    ApplicationConstant.setZkQuorum("10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181")
    ApplicationConstant.setGroup("shoppingCart")
    SparkConstant.initConf(sparkConf)
  }

  def handle(conf: SparkConf) = {

    val ssc = StreamingContext.getOrCreate(ApplicationConstant.getCheckpointDirectory
      , () => RecoverableUtil.createContext(conf, ApplicationConstant.checkpointDirectory,
        SparkConstant.getSparkStreamingHandleRate
      ))
    val topicMap = ApplicationConstant.getTopics.split(",")
      .map((_, ApplicationConstant.getNumThreads.toInt)).toMap


    val lines = KafkaUtils.createStream(ssc,
      ApplicationConstant.getZkQuorum,
      ApplicationConstant.getGroup,
      topicMap)

    /**
     * json  -----------------> (visitor_id,general_cd)
     */
    val ParserJsonRecodeDStream = lines.map(x => {
      ParserJsonRecode(x._2)
    }).filter(!_._1.equals("error"))

    val stateDStream = ParserJsonRecodeDStream.updateStateByKey[Seq[String]](newUpdateFunc, new HashPartitioner (ssc.sparkContext.defaultParallelism),true)

    stateDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }



  /**
   * Parser  (visitor_id,general_cd))
   * @param recode
   * @return
   */
  // cookeid memid imei ->one
  def ParserJsonRecode(recode: String): (String, String) = {
    var recodeJson: JSONObject = null
    var visitorId: String = null
    var generalCd: String = null
    try {
      recodeJson = JSONObject.fromObject(recode)
      visitorId = recodeJson.getString(ApplicationConstant.visitor_id)
      generalCd = recodeJson.getString(ApplicationConstant.general_cd)
    }
    catch {
      case e: Exception =>{
        logInfo("e.printStackTrace()")
        e.printStackTrace()
        visitorId ="error"
        generalCd ="error"
      }
    }
    ( visitorId, generalCd)
  }


  val updateFunc = (values: Seq[String], state: Option[Seq[String]]) => {
    val reverseValues = values.reverse
    if(reverseValues.length>3){
      Some(Seq(reverseValues(0),reverseValues(1),reverseValues(2)))
    }else {
      var nState=if(state.isEmpty) reverseValues else reverseValues++state.get
      if(nState.length>3){
        nState=Seq(nState(0),nState(1),nState(2))
      }
      Some(nState)
    }
  }
  val newUpdateFunc = (iterator: Iterator[(String, Seq[String], Option[Seq[String]])]) => {
    iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
  }

  /**
   * ./kafka-topics.sh --zookeeper 10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181 --create --topic spark-short-shoppingCart --partitions 10 --replication-factor 2
   * ./kafka-console-producer.sh --broker-list "10.27.25.164:9092,10.27.25.165:9093,10.27.25.166:9094"  --topic spark-short-shoppingCart
   * ./kafka-console-consumer.sh --zookeeper 10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181  --topic spark-short-shoppingCart-result --group spark-short-shoppingCart
   * @param args
   */
  def main(args: Array[String]) {
    logInfo("main args:" + args)
    if (args.length < 5) {
      logInfo("usage:*jar sparkStreamingReceiverMaxRate  sparkStreamingHandleRate/minSeconds  driverproxyuser  appname checkpointDirectory")
      System.exit(-1)
    }

    /**
     * init all para
     */
    val conf = init(args)

    /**
     * hanle
     */
    handle(conf)
  }
}
