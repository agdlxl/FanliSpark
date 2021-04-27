package com.liangxl.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/** * @Author: liangx
 *
 * @Date: 2021-03-31-17:01 
 * @Description:
 **/
object MyKafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  private val parmeters = new StreamingUtils.SparkParmeters
  // kafka 消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2020_group",
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //// 创建 DStream，返回接收到的输入数据
 def getKafkaStream(topic:String,ssc:StreamingContext):InputDStream[ConsumerRecord[String, String]]={
   val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
     ssc,
     LocationStrategies.PreferConsistent,
     ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
   )
   kafkaStream
 }
 /*
    指定偏移量位置读取数据
 */
  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String)
  : InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    println("获取流数据")
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))
    dStream
  }

  def getKafkaStream(topic: String,ssc:StreamingContext,groupId:String)
  : InputDStream[ConsumerRecord[String,String]]={
    println("获取流数据2")
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
    dStream
  }

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val inputKafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("gmall_event_0523", ssc)

    inputKafkaStream.map(_.value()).foreachRDD(rdd=>{
      rdd.foreachPartition(par=>{
        par.foreach(v=>{
          println("v1##"+v)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def main1(args: Array[String]): Unit = {

  }

}
