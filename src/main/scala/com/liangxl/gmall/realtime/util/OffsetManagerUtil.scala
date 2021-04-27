package com.liangxl.gmall.realtime.util

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import java.util.{Date, Properties}
import java.{lang, util}

import com.liangxl.gmall.realtime.sdb.C3p0DbPool
import com.liangxl.gmall.realtime.util.MyKafkaUtil.{parmeters, properties}
import com.liangxl.gmall.realtime.util.StreamingUtils.SparkParmeters
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** * @Author: liangx
 *
 * @Date: 2021-04-01-16:44 
 * @Description:  偏移量管理类，用于读取和保存偏移量
 **/
object OffsetManagerUtil extends Logging {

  /**o7lti
   * 从 Redis 中读取偏移量
   * Reids 格式：type=>Hash [key=>offset:topic:groupId field=>partitionId
value=>偏移量值] expire 不需要指定
   * topicName 主题名称
   * groupId 消费者组
   * @return 当前消费者组中，消费的主题对应的分区的偏移量信息
   * KafkaUtils.createDirectStream 在读取数据的时候封装了
Map[TopicPartition,Long]
   */
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  private val parmas = new StreamingUtils.SparkParmeters(group_id = "gmall2020_group",topics =  "gmall_event_0523")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaParam =Map[String,Object](
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> parmas.group_id,
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> "false"
  )

  /*
  连接mysql,读取当前偏移量
   */
  val selectsql =
  "select partition_id,topic,offset from tb_kafka_offset_record where group_id = ? and topic= ? "


  val updateSql =
    """
      |UPDATE tb_kafka_offset_record set offset=?,offset_ts=?,batch_size=?,acc_batch_size=acc_batch_size+?
      | where group_id=? and topic = ? and partition_id=?
    """.stripMargin

  val insertSql =
    """
      |INSERT INTO tb_kafka_offset_record(group_id,topic,partition_id,offset,offset_ts,batch_size,acc_batch_size)
      |  values (?,?,?,?,?,?,?)
    """.stripMargin

  def getOffset(topicName:String,groupId:String,mysqlProp:String="mysql.properties"):mutable.Map[TopicPartition,Long]={
    val pool: C3p0DbPool = C3p0DbPool(mysqlProp)
    val conn: Connection = pool.getConnection(false)
    val statement: PreparedStatement = conn.prepareStatement(selectsql)
    val partitionToLong = mutable.Map[TopicPartition,Long]()
    for(topics <- topicName.split(",")){
      statement.setString(1,groupId)
      statement.setString(2,topics)
      val resultSet: ResultSet = statement.executeQuery()
      while(resultSet.next()){
        val partition_id: String = resultSet.getString("partition_id")
        val topics: String = resultSet.getString("topic")
        val offset: String = resultSet.getString("offset")
        println("获取的偏移量"+partition_id + " " + topics + " " + offset)
        partitionToLong.put(new TopicPartition(topics, partition_id.toInt), offset.toLong)
      }
    }
    conn.commit()
    statement.close()
    conn.close()


    if(!partitionToLong.isEmpty){
      checkAndEnhanceOffsets(kafkaParam,parmas,partitionToLong)
      partitionToLong.foreach(tp=>{
        println(s"#START-OFFSET#${tp._1.topic()}-${tp._1.partition()}-${tp._2}")
      })
    }

    partitionToLong
  }

  def saveOffset(topicName:String,groupId:String,milliseconds:Long,mysqlProp:String,offsetRanges:Array[OffsetRange])={
    println("save")
    val pool: C3p0DbPool = C3p0DbPool(mysqlProp)
    val conn: Connection = pool.getConnection()
    var statement: PreparedStatement=null
    val partitionToLong:mutable.Map[TopicPartition, Long]
         = OffsetManagerUtil.getOffset(topicName, groupId, mysqlProp)

    if (partitionToLong.size>0){
      statement = conn.prepareStatement(updateSql)
      for(offsets <- offsetRanges ){
        val partition: Int = offsets.partition
        val untilOffset: Long = offsets.untilOffset
        statement.setLong(1,untilOffset)
        statement.setTimestamp(2,new Timestamp(milliseconds))
        statement.setLong(3,offsets.count())
        statement.setLong(4,offsets.count())
        statement.setString(5,groupId)
        statement.setString(6,topicName)
        statement.setLong(7,partition)
        statement.addBatch()
      }
    }
    else
    {
      statement = conn.prepareStatement(insertSql)
      for(offsets <- offsetRanges ){
        val partition: Int = offsets.partition
        val untilOffset: Long = offsets.untilOffset
        statement.setString(1,groupId)
        statement.setString(2,topicName)
        statement.setLong(3,partition)
        statement.setLong(4,untilOffset)
        statement.setTimestamp(5,new Timestamp(milliseconds))
        statement.setLong(6,offsets.count())
        statement.setLong(7,offsets.count())
        statement.addBatch()
        val msg = s"""#add partition# ${offsets.topic} partition: ${offsets.partition} from ${offsets.fromOffset} until ${offsets.untilOffset}"""
        println(msg)
      }
    }
    statement.executeBatch()
    conn.commit()
    statement.clearBatch()
    conn.close()

  }

  def checkAndEnhanceOffsets(kafkaParams: Map[String, Object]
                             , parmas: StreamingUtils.SparkParmeters
                             , offsets: mutable.Map[TopicPartition, Long]):mutable.Map[TopicPartition, Long] ={

    import scala.collection.JavaConverters._
    val consumer = new KafkaConsumer[String,Object](kafkaParams.asJava)

    import java.util.Arrays
    val isEarilistOffset = if(kafkaParams.get("auto.offset.reset") == "earliest") true else false

    parmas.topics.split(",").foreach(topic=> {
      val partitionInfoes: util.List[PartitionInfo] = consumer.partitionsFor(topic)

       //获取每一个分区对应的偏移量
        val topicPartitionToLongStart: util.Map[TopicPartition, lang.Long]
        = consumer.beginningOffsets(partitionInfoes.asScala.map(x=>new TopicPartition(x.topic(),x.partition())).asJava)

        val topicPartitionToLongEnd: util.Map[TopicPartition, lang.Long] = if (isEarilistOffset) {
          new util.HashMap[TopicPartition, lang.Long]
        } else {
          consumer.endOffsets(partitionInfoes.asScala.map(x=>new TopicPartition(x.topic(),x.partition())).asJava)
        }

       //更细每一个分区的位置
      partitionInfoes.asScala.foreach(partitions=> {
        val partition = new TopicPartition(partitions.topic(), partitions.partition())

        if (offsets.contains(partition)) {
          if (offsets(partition) > topicPartitionToLongStart.get(partition)) {
            val updateV = if (isEarilistOffset) topicPartitionToLongStart.get(partition) else topicPartitionToLongEnd.get(partition)
            println(s"$partition offset 小于了最新值将其更新 ${offsets(partition)} -> $updateV")
            offsets(partition) = updateV
          }
        }

          else {
            val updateV = if (isEarilistOffset) topicPartitionToLongStart.get(partition) else topicPartitionToLongEnd.get(partition)
            offsets += (partition -> updateV)
          }
      })
    })
      consumer.close()
    offsets
  }


//  def main(args: Array[String]): Unit = {
//
//
//    val partition = mutable.HashMap[TopicPartition,Long]((new TopicPartition("gmall_event_0523", 0),0L),(new TopicPartition("gmall_event_0523", 1),2L))
//
//
//   OffsetManagerUtil.checkAndEnhanceOffsets(kafkaParam,parmas,partition)
//  }



}
