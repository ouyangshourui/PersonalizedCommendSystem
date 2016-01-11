# PersonalizedCommendSystem
## 1.compile ##
mvn assembly:assembly
## 2.kafka+Spark unstateful Streaming + hive  ##
<pre>
<code>
appjars=ppcs.streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
hadoop fs -rm -r /user/spark/sparkstreamingcheckpoint/BrandCat/*
hadoop fs -rm hdfs:///user/spark/$appjars
hadoop fs -put $appjars 
FixedAdvertisingCores=8
maxRate=1000
producerRate=1000
handleRate=1
handleInterval=1
userNum=1000
driverproxyuser=spark
#*jar sparkStreamingReceiverMaxRate  sparkStreamingDelRate 
spark-submit --deploy-mode cluster  --total-executor-cores  $FixedAdvertisingCores --class com.suning.spark.streaming.ppsc.FixedAdvertising hdfs:///user/spark/$appjars  $maxRate $handleRate  $driverproxyuser
#kafka producer
//#usage:**jar handleInterval handleRax  usernum
spark-submit  --deploy-mode cluster   --total-executor-cores  2 --class com.suning.spark.streaming.kafka.KafkaMessageStandLoneProducer  hdfs:///user/spark/$appjars $handleInterval $producerRate  $userNum  $driverproxyuser
#spark-submit --deploy-mode cluster  --total-executor-cores  4 --class com.suning.spark.streaming.kafka.KafkaMessageProducer  hdfs:///user/spark/$appjars
</code>
</pre>

## 3.kafka+Spark stateful Streaming + hive  ##
<pre>
<code>
appjars=ppcs.streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
appname=RecommendBasedShoppingCart
checkpointDir=/user/spark/$appname
hadoop fs -rm -r  $checkpointDir
hadoop fs -rm hdfs:///user/spark/$appjars
hadoop fs -put $appjars 
Cores=8
maxRate=15000
batch=1000
driverproxyuser=spark
main1=com.suning.spark.streaming.ppsc.RecommendBasedShoppingCart
spark-submit --deploy-mode cluster  \
             --total-executor-cores  $Cores  \
             --class $main1   \
              hdfs:///user/spark/$appjars  $maxRate $batch  $driverproxyuser  $appname  $checkpointDir



#handleInterval handleRax  usernum driverproxyuser

#millisecond
handleInterval=200
handleRax=2000
usernum=100
main2=com.suning.spark.streaming.kafka.KafkaRShoppingCartMessageProducer
spark-submit --deploy-mode cluster  \
             --total-executor-cores  4  \
             --class $main2   \
            hdfs:///user/spark/$appjars $handleInterval $handleRax $usernum $driverproxyuser
</code>
</pre>
