package com.liangxl.gmall.realtime.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.{lang, util}

import com.liangxl.gmall.realtime.sdb.C3p0DbPool

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

/** * @Author: liangx
 *
 * @Date: 2021-04-01-17:23
 * @Description: 获取当前的偏移量
 **/
object StreamingMysqlOffset {

  private def getOffsetFromState(parmas: StreamingUtils.SparkParmeters,
                                 resetOffset: Boolean
                                 ,offsetDbProp:String): mutable.Map[TopicPartition,Long] ={

    var dbPool: C3p0DbPool = null
    var conn: Connection = null
    var partitionToLong = new util.HashMap[TopicPartition, Long]()
    var resultSet: ResultSet = null
    var stmt: PreparedStatement = null
    try {
       dbPool = C3p0DbPool(offsetDbProp)
       conn = dbPool.getConnection()
      val insertsql = "select partition_id,topic,offset from tb_kafka_offset_record where group_id = '" + parmas.group_id + "'"
       stmt = conn.prepareStatement(insertsql)
      val resultSet: ResultSet = stmt.executeQuery()
      while (resultSet.next()) {
        val topic: String = resultSet.getString("topic")
        val partition_id = resultSet.getInt("partition_id")
        val offset: Long = resultSet.getLong("offset")
        partitionToLong.put(new TopicPartition(topic, partition_id), offset + 1)
      }
    }
    catch {
      case e:Throwable=> {
        val msg: String = parmas.group_id + "获取offset程序将挂掉#" + ExceptionUtils.getStackTrace (e)
        System.exit (- 1)
      }
    }
    finally {
      if(resultSet!=null){
        resultSet.close()
      }
      if(conn!=null){
        conn.close()
      }
      if(stmt!=null){
        stmt.close()
      }
    }
    val map: mutable.Map[TopicPartition, Long] = partitionToLong.asScala
    val set: collection.Set[TopicPartition] = map.keySet
    if(set.size > 0){
      val mutableSet = mutable.Set[TopicPartition]()
      set.foreach(mutableSet+=(_))
      setCurrentTopicPartitionSet(mutableSet)
    }
    map
  }
 /*
 更新分区
  */

/**
 * 启动前 将没有写入的topic或者分区加入进来
 * @return
 */
def checkAndEnhanceOffsets(kafkaParams: util.Map[String, Object]
                           , parmas: StreamingUtils.SparkParmeters
                           , offsets: mutable.Map[TopicPartition, Long]):mutable.Map[TopicPartition, Long] ={

  //("auto.offset.reset")
  val consumer = new KafkaConsumer[String,Object](kafkaParams)

  import java.util.Arrays
  val isEarilistOffset = if(kafkaParams.get("auto.offset.reset") == "earliest") true else false

  parmas.topics.split(",").foreach(topic=> {
    val partitionInfoes: util.List[PartitionInfo] = consumer.partitionsFor(topic)

    val iter: util.Iterator[PartitionInfo] = partitionInfoes.iterator()

    import scala.collection.JavaConverters._
    if (iter.hasNext) {
      val topicPartitionToLongStart: util.Map[TopicPartition, lang.Long]
      = consumer.beginningOffsets(partitionInfoes.asScala.map(x=>new TopicPartition(x.topic(),x.partition())).asJava)

      val topicPartitionToLongEnd: util.Map[TopicPartition, lang.Long] = if (isEarilistOffset) {
        new util.HashMap[TopicPartition, lang.Long]
      } else {
        consumer.endOffsets(partitionInfoes.asScala.map(x=>new TopicPartition(x.topic(),x.partition())).asJava)
      }


      val partition = new TopicPartition(topic, iter.next().partition())
      if(offsets.contains(partition)){
        if(offsets(partition) < topicPartitionToLongStart.get(partition)){
          val updateV = if(isEarilistOffset) topicPartitionToLongStart.get(partition) else topicPartitionToLongEnd.get(partition)
          println(s"$partition offset 小于了最新值将其更新 ${offsets(partition)} -> $updateV")
          offsets(partition) = updateV
        }

      }
      else{
        val updateV = if(isEarilistOffset)topicPartitionToLongStart.get(partition) else topicPartitionToLongEnd.get(partition)
        offsets += (partition->updateV)
      }


    }
  } )
  consumer.close()
  offsets
}




  var topicPartitionSet:mutable.Set[TopicPartition] = mutable.Set[TopicPartition]()
  def setCurrentTopicPartitionSet(tps:mutable.Set[TopicPartition]){
    topicPartitionSet.synchronized{
      topicPartitionSet = tps
    }
  }


  def getCurrentTopicPartitionSet() ={
    topicPartitionSet.synchronized{
      topicPartitionSet
    }
  }


  /**
   * 从offest中订阅topic
   * @return: InputDStream[ConsumerRecord[K,V]]
   */
def subscribeToTopicWithState[K,V](streamingContext:StreamingContext,
                                   parmas: StreamingUtils.SparkParmeters,
                                   kafkaParams1:Map[String,Object]=null,
                                   offsetDbProp:String="mysql.properties",
                                   resetOffset:Boolean=false) = {

  var kafkaParams = Map[String, Object](
    "bootstrap.servers" -> parmas.bootStrap, //用于初始化链接到集群的地址
    "key.deserializer" -> ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "value.deserializer" -> ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> parmas.group_id,
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest"
    ,
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  if (kafkaParams1!=null)
    {
      kafkaParams=kafkaParams1
    }

  //获取当前偏移量
  val partitionToLong: mutable.Map[TopicPartition, Long] = getOffsetFromState(parmas, resetOffset, offsetDbProp)
  val subscribed: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    streamingContext
    , LocationStrategies.PreferConsistent
    , ConsumerStrategies.Subscribe[String, String](Array(parmas.topics), kafkaParams,partitionToLong))
  subscribed
}
}