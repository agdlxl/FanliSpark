package com.liangxl.gmall.realtime.util

import java.sql.{Connection, Date, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat

import com.liangxl.gmall.realtime.sdb.{C3p0DbPool, DruidPool}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.scheduler.{StreamInputInfo, StreamingListener, StreamingListenerBatchCompleted}

import scala.collection.mutable

/** * @Author: liangx
 *
 * @Date: 2021-04-23-19:41 
 * @Description:
 **/
class FanliSparkStreamingListener(parmas: StreamingUtils.SparkParmeters, offsetDbProp:String="mysql.properties")
extends StreamingListener  with Logging {

  val sql =
    s"""INSERT INTO tb_kafka_offset_record(group_id,topic,partition_id,offset,offset_ts,batch_size)
       |  values (?,?,?,?,?,?)
       |  ON DUPLICATE KEY UPDATE
       | offset=?,offset_ts=?,batch_size=?,acc_batch_size=acc_batch_size+?
           """.stripMargin

  val insertSql =
    """
      |INSERT INTO tb_kafka_offset_record(group_id,topic,partition_id,offset,offset_ts,batch_size,acc_batch_size)
      |  values (?,?,?,?,?,?,?)
    """.stripMargin
  val updateSql =
    """
      |UPDATE tb_kafka_offset_record set offset=?,offset_ts=?,batch_size=?,acc_batch_size=acc_batch_size+?
      | where group_id=? and topic = ? and partition_id=?
    """.stripMargin


  def writeTopicOffsetsToMysql(stream: (Int, StreamInputInfo), milliseconds: Long): Unit = {
    //获取需要更新
    val topicPartitions: mutable.Set[TopicPartition] = StreamingMysqlOffset.getCurrentTopicPartitionSet()
    //获取mysql连接

    var DruidPool: DruidPool = null
    var connection: Connection = null
    var statement: PreparedStatement = null
    try { ///获取偏移量

      DruidPool = new  DruidPool(offsetDbProp)
      connection = DruidPool.getConnection(false)

      val ranges: List[OffsetRange] = stream._2.metadata("offsets").asInstanceOf[List[OffsetRange]]

      val offsetRanges: List[OffsetRange] = ranges.filter(_.count() > 0)
      //这个是已经有记录的，主要用以更新操作
      val containOffsetrange: List[OffsetRange]
      = offsetRanges.filter(offsetrange => topicPartitions(offsetrange.topicPartition()))

      //这个是需要新插入的
      val unContainRange: List[OffsetRange] = offsetRanges.diff(containOffsetrange)

      if (offsetRanges.size > containOffsetrange.size) {
        statement = connection.prepareStatement(insertSql)
        unContainRange.foreach {
          offsetRanges => {
            val maxOffset: Long = offsetRanges.untilOffset - 1
            statement.setString(1, parmas.group_id)
            statement.setString(2, offsetRanges.topic)
            statement.setInt(3, offsetRanges.partition)
            statement.setLong(4, maxOffset)
            statement.setTimestamp(5, new Timestamp(milliseconds))
            statement.setLong(6, offsetRanges.count())
            statement.setLong(7, offsetRanges.count())
            statement.addBatch()
            val msg: String = s"""#add partition# ${offsetRanges.topic} partition: from ${offsetRanges.fromOffset} until ${offsetRanges.untilOffset}"""
            log.info(msg)
            topicPartitions +=(offsetRanges.topicPartition())
          }
        }
        statement.executeBatch()
        connection.commit()
        StreamingMysqlOffset.setCurrentTopicPartitionSet(topicPartitions)
        statement.clearBatch()
      }
      if (containOffsetrange.size > 0) {
        statement = connection.prepareStatement(updateSql)
        containOffsetrange.foreach {
          offsetRanges => {
            val maxOffset: Long = offsetRanges.untilOffset - 1

            statement.setLong(1, maxOffset)
            statement.setTimestamp(2, new Timestamp(milliseconds))
            statement.setLong(3, offsetRanges.count())
            statement.setLong(4, offsetRanges.count())
            statement.setString(5, parmas.group_id)
            statement.setString(6, offsetRanges.topic)
            statement.setInt(7, offsetRanges.partition)
            statement.addBatch()
            val msg: String = s"""#update partition# ${offsetRanges.topic} partition: from ${offsetRanges.fromOffset} until ${offsetRanges.untilOffset}"""
           log.info(msg)
          }
        }
        statement.executeBatch()
        connection.commit()
        statement.clearBatch()
      }
    }
    catch {
      case e:Throwable=> {
        val msg: String =  parmas.group_id+ "#保存offset#"+ExceptionUtils.getStackTrace(e)
        log.error(msg)
        System.exit(-1)
      }
    } finally {
      if(statement!=null){
        statement.close()
      }
      if(connection != null){
        connection.close()
      }
    }
  }

  //写入偏移量
  def writeBatchOffsetAndCounts(batchCompleted: StreamingListenerBatchCompleted) = {
    ///获取当前批次执行时间
    val milliseconds = batchCompleted.batchInfo.batchTime.milliseconds

    val records: Long = batchCompleted.batchInfo.numRecords
    if ( records > 0){
      logInfo(s"""batchTime=${new Date(milliseconds)},records=${records}""")
      batchCompleted.batchInfo.streamIdToInputInfo.foreach(stream=>{
        writeTopicOffsetsToMysql(stream,milliseconds)
      })
    }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
  {
    writeBatchOffsetAndCounts(batchCompleted)
  }

}


object FanliSparkStreamingListener{
  /**
   *
   * @param parmas
   * @param offsetDbProp
   * @return
   */
  def apply(parmas: StreamingUtils.SparkParmeters, offsetDbProp: String = "mysql.properties"): FanliSparkStreamingListener
  = new FanliSparkStreamingListener(parmas, offsetDbProp)

}