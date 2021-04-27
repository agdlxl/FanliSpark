package com.liangxl.gmall.realtime.app

import java.lang
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.liangxl.gmall.realtime.bean.DauInfo
import com.liangxl.gmall.realtime.sdb.{C3p0DbPool, DruidPool}
import com.liangxl.gmall.realtime.util.{FanliSparkStreamingListener, MyKafkaUtil, OffsetManagerUtil, SparkStreamingListener, StreamingUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import org.apache.spark.streaming.scheduler._

/** * @Author: liangx
 *
 * @Date: 2021-04-01-9:51 
 * @Description:
 **/
object DauApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val parmas: StreamingUtils.SparkParmeters = StreamingUtils.SparkParmeters(group_id = "gmall2020_group", topics = "gmall_event_0523")
    ssc.addStreamingListener(FanliSparkStreamingListener(parmas,offsetDbProp = "mysql.properties"))

    val groupId = "gmall2020_group"
    val topic = "gmall_event_0523"
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
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

///获取当前消费点
    val kafkaStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println("当前消费分区"+offsetRanges(0).partition+"消费量"+offsetRanges(0).fromOffset)
        rdd
      }
    }

    //预处理
    val jsonStream: DStream[JSONObject] = kafkaStream.map {
      record => {
        val jsonstr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonstr)
        val ts: lang.Long = jsonObject.getLong("ts")
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHours: String = sdf.format(new Date(ts))
        val dateHour: Array[String] = dateHours.split(" ")
        jsonObject.put("dt", dateHour(0))
        jsonObject.put("hr", dateHour(1))
        jsonObject
      }
    }

    jsonStream.foreachRDD {
     rdd=>{
       rdd.foreachPartition{
         jsonstr=>{
           val iterJson: Iterator[DauInfo] = jsonstr.map {
             jsonObj => {
               val commonObject: JSONObject = jsonObj.getJSONObject("common")
               DauInfo(
                 commonObject.getString("mid"),
                 commonObject.getString("uid"),
                 commonObject.getString("ar"),
                 commonObject.getString("ch"),
                 commonObject.getString("vc"),
                 jsonObj.getString("dt"),
                 jsonObj.getString("hr"),
                 "00", //分钟我们前面没有转换，默认 00
                 jsonObj.getLong("ts")
               )
             }
           }
           //OffsetManagerUtil.saveOffset(topic,groupId,new Date().getTime,"mysql.properties",offsetRanges)
           bulkInsert(iterJson)
         }
       }
     }
    }

    def bulkInsert(iterJson: Iterator[DauInfo]){
      val pool = DruidPool("mysql.properties")
      val conn: Connection = pool.getConnection()
      val insertsql = "insert into tb_dau_detail(mid,uid,ar,ch,vc,dt,hr,mi,ts) values (?,?,?,?,?,?,?,?,?)"
      val stmt: PreparedStatement = conn.prepareStatement(insertsql)
      iterJson.foreach{
        rows=>{
          stmt.setString(1, rows.mid)
          stmt.setString(2, rows.uid)
          stmt.setString(3, rows.ar)
          stmt.setString(4, rows.ch)
          stmt.setString(5, rows.vc)
          stmt.setString(6, rows.dt)
          stmt.setString(7, rows.hr)

          stmt.setString(8, rows.mi)
          stmt.setLong(9, rows.ts)
          stmt.addBatch()
        }
      }

      val ints: Array[Int] = stmt.executeBatch()
      println("已插入mysql"+ints.sum)
      conn.commit()
      conn.close()

    }

    ssc.start()
    ssc.awaitTermination()
  }


  def mainX(args: Array[String]): Unit = {
    val str="""{
              |	"actions": [{
              |		"action_id": "cart_minus_num",
              |		"item": "6",
              |		"item_type": "sku_id",
              |		"ts": 1617152774827
              |	}, {
              |		"action_id": "cart_remove",
              |		"item": "6",
              |		"item_type": "sku_id",
              |		"ts": 1617152778922
              |	}],
              |	"common": {
              |		"ar": "110000",
              |		"ba": "vivo",
              |		"ch": "wandoujia",
              |		"md": "vivo iqoo3",
              |		"mid": "mid_27",
              |		"os": "Android 11.0",
              |		"uid": "369",
              |		"vc": "v2.1.134"
              |	},
              |	"page": {
              |		"during_time": 12285,
              |		"last_page_id": "good_detail",
              |		"page_id": "cart"
              |	},
              |	"ts": 1617152770732
              |}""".stripMargin

    val jsonobject: JSONObject = JSON.parseObject(str)
    val ts: lang.Long = jsonobject.getLong("ts")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val dateHourString: String = sdf.format(new Date(ts))
    val dateHour: Array[String] = dateHourString.split(" ")
    println(dateHour(1))
  }

  def main0(args: Array[String]): Unit = {
    //val listener = new SparkStreamingListener()
  }

}