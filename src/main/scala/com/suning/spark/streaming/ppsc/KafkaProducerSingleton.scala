package com.suning.spark.streaming.ppsc

import java.util.HashMap

import com.suning.spark.streaming.ApplicationConstant
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.beans.BeanProperty

/**
 * Created by 14070345 on 2015/12/11 0011.
 */
object KafkaProducerSingleton {

  private val brokers = ApplicationConstant.getBrokers
  @BeanProperty val topic = ApplicationConstant.getResulttopics
  private val props = new HashMap[String, Object]()
  @transient private var instance: KafkaProducer[String, String] = null
  def getInstance: KafkaProducer[String, String] = synchronized {
    if (instance == null) {
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      instance = new KafkaProducer[String, String](props)
    }
    instance
  }


}
