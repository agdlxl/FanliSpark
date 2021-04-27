package com.liangxl.gmall.realtime.app

import com.liangxl.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/** * @Author: liangx
 *
 * @Date: 2021-04-21-11:57 
 * @Description:
 **/
object test {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val groupId = "gmall2020_group_1"
    val topic = "gmall2020"
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]]= null
    //获取mysql内存的偏移量
    val partitionToLong: mutable.Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    //获取当前实际消费到的偏移量

    //获取kafka消费数据
    if(partitionToLong!=null && partitionToLong.size>0){
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, partitionToLong.toMap, groupId)
    }
    else
    {
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    kafkaDStream.foreachRDD{
      record=>{
         record.foreach{
           rdd=>{
             val str: String = rdd.value()
             println(str)

           }
         }      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
