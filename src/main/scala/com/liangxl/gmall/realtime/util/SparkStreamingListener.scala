package com.liangxl.gmall.realtime.util

import java.sql.Timestamp

import com.liangxl.gmall.realtime.sdb.C3p0DbPool
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.scheduler.StreamInputInfo

/** * @Author: liangx
 *
 * @Date: 2021-04-01-16:53 
 * @Description: 默认把offest存到mysql
 **/
class  SparkStreamingListener(parmas: StreamingUtils.SparkParmeters,offsetDbProp:String="mysql.properties") {

  val updateSql =
    """
      |UPDATE tb_kafka_offset_record set offset=?,offset_ts=?,batch_size=?,update_time=now(),acc_batch_size=acc_batch_size+?
      | where group_id=? and topic = ? and partition_id=?
    """.stripMargin



  def writeTopicOffsetsToMysql(stream: Map[TopicPartition,Long],milliseconds:Long,desc:String="")={

    val topicPartitions = StreamingMysqlOffset.getCurrentTopicPartitionSet()

    val offestFiliter: Map[TopicPartition, Long] = stream.filter(offests => offests._2 > 0)

    val containsOffset: Map[TopicPartition, Long] = offestFiliter.filter(offestRange => {
      topicPartitions(offestRange._1)
    })

    var dBManager = C3p0DbPool(offsetDbProp)
    var conn =  dBManager.getConnection(false)

    if(containsOffset.size>0){
      var stmt = conn.prepareStatement(updateSql)
      containsOffset.foreach(
        offsetRange=>{
          val maxOffset: Long = offsetRange._2
          stmt.setLong(1,maxOffset)
          stmt.setTimestamp(2,new Timestamp(milliseconds))
          stmt.setLong(3,1)
          stmt.setLong(4,offsetRange._2)
          stmt.setString(6,offsetRange._1.topic())
          stmt.setInt(7,offsetRange._1.partition())
          stmt.addBatch()
        }

      )

    }


  }
}
