package com.liangxl.gmall.realtime.sdb

import java.sql.{Connection, Statement}
import java.util.Properties

import com.alibaba.druid.pool.DruidPooledConnection

import scala.collection.mutable

/** * @Author: liangx
 *
 * @Date: 2021-04-27-13:33 
 * @Description:
 **/
class DruidPool(val proFileName: String, val host: String, val port: String, val db: String) {


  if(proFileName == null) "mysql.properties" else proFileName
  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(proFileName))

  if(properties.getProperty("url")==null) {
    val url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?characterEncoding=UTF8&autoReconnect=true&rewriteBatchedStatements=true"
    properties.setProperty("url", url)
  }

  import com.alibaba.druid.pool.DruidDataSource

  private val dataSource = new DruidDataSource(true)

  def this(proFileName: String ) = this( proFileName,  "", "",  "")
  dataSource.setUrl(properties.getProperty("url").toString())
  dataSource.setDriverClassName(properties.getProperty("driverClassName").toString())
  dataSource.setUsername(properties.getProperty("username").toString())
  dataSource.setPassword(properties.getProperty("password").toString())

  def getDatasoure:DruidDataSource=dataSource

  def getConnection(isAutoCommit:Boolean=false) = {
    synchronized {
      val connection: DruidPooledConnection = dataSource.getConnection
      connection.setAutoCommit(isAutoCommit)
      connection
    }
  }

  def closeConn(conn:DruidPooledConnection,stat:Statement){
    if(stat != null){
      stat.close()
    }
    if(conn != null && !conn.isClosed){
      conn.setAutoCommit(true)
      conn.close()
    }
  }

}


object  DruidPool {
  //新建一个map，存连接池
  val map: mutable.Map[String, DruidPool] = scala.collection.mutable.Map()

  def apply(proFileName: String, host: String, port: String, db: String): DruidPool = {
    if (!map.contains(proFileName)) {
      synchronized {
        if (!map.contains(proFileName)) {
          val pool = new DruidPool(proFileName, host, port, db)
          map += (proFileName -> pool)
        }
      }
    }
    map(proFileName)
  }


  def apply(proFileName: String): DruidPool = {
    if (!map.contains(proFileName)) {
      synchronized {
        if (!map.contains(proFileName)) {
          val pool = new DruidPool(proFileName)
          map += (proFileName -> pool)
        }
      }
    }
    map(proFileName)
  }

//  def main(args: Array[String]): Unit = {
//        var pool: DruidPool= DruidPool("mysql.properties")
//        var conn:Connection = pool.getConnection()
//    println(conn)
//      }
}

