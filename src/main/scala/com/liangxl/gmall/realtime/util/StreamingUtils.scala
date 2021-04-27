package com.liangxl.gmall.realtime.util

/** * @Author: liangx
 *
 * @Date: 2021-04-01-11:15 
 * @Description:
 **/
object StreamingUtils {

  case class SparkParmeters(
                             var bootStrap:String="",
                             var topics:String = "",
                             var checkpointDir:String = "",
                             var checkpointDuration:String = "",
                             var batch:String = "",
                             var group_id:String = "" ,
                             var redis_host:String = "",
                             var redis_port:String = "",
                             var interval:String = "",
                             var dbProperty:String = "",
                             var autoOffsetReset:String = "",
                             var monitorRedisHost:String="t",
                             var monitorRedisPort:Int=6381,
                             var ifMointor:Boolean=true,
                             var clickhouse_host:String = "",
                             var clickhouse_port:String = "",
                             var clickhouse_username:String = "",
                             var clickhouse_passwd:String = "",
                             var clickhouse_db:String = "",
                             var window_length:Int = 0,
                             var window_slide:Int = 0
                           )extends Serializable {
    override def toString: String =
      s"""
         |bootStrap=$bootStrap
         |topics=$topics
         |checkpointDir=$checkpointDir
         |checkpointDuration=$checkpointDuration
         |batch=$batch
         |group_id=$group_id
         |redis_host=$redis_host
         |redis_port=$redis_port
         |interval=$interval
         |dbProperty=$dbProperty
         |monitorRedisHost=$monitorRedisHost
         |monitorRedisPort=$monitorRedisPort
         |ifMointor=$ifMointor
         |autoOffsetReset=$autoOffsetReset
       """.stripMargin
  }
}
