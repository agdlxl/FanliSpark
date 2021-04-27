package com.liangxl.gmall.realtime.sdb

import java.sql.{Connection, SQLException, Statement}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

/** * @Author: liangx
 *
 * @Date: 2021-04-01-11:32 
 * @Description: 新建一个连接池
 **/
class C3p0DbPool(val proFileName: String, val host: String, val port: String, val db: String) {
  if(proFileName==null) "mysql.properties" else proFileName
  private val prop = new Properties()
  prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(proFileName))
  if(prop.getProperty("url")==null) {
    val url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?characterEncoding=UTF8&autoReconnect=true&rewriteBatchedStatements=true"
    prop.setProperty("url", url)
  }

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)


  def this(proFileName: String ) = this( proFileName,  "", "",  "")
  cpds.setJdbcUrl(prop.getProperty("url").toString());
  cpds.setDriverClass(prop.getProperty("driverClassName").toString());
  cpds.setUser(prop.getProperty("username").toString());
  cpds.setPassword(prop.getProperty("password").toString());

  def getDataSource:ComboPooledDataSource=cpds

  def getConnection(autoCommit:Boolean=false):Connection={
    synchronized {
      val connection: Connection = cpds.getConnection
      connection.setAutoCommit(autoCommit)
      connection
    }
  }

  def closeConn(conn: Connection,st:Statement): Unit = {
    if (st != null) {
      st.close()
    }
    if (conn != null && !conn.isClosed) {
      conn.setAutoCommit(true)
      conn.close()
    }
  }

}

object C3p0DbPool
{
  //存新建的數據庫連接池
  var mdbManager:scala.collection.mutable.Map[String,C3p0DbPool]= scala.collection.mutable.Map()

  def apply(proFileName: String, host: String, port: String, db: String):C3p0DbPool={
    if( !mdbManager.contains(proFileName)){
      synchronized{
        if( !mdbManager.contains(proFileName)){
          val dbPool = new C3p0DbPool(proFileName, host, port, db)
          mdbManager += (proFileName->dbPool)
        }
      }
    }
    mdbManager(proFileName)
  }

  def apply(proFileName: String):C3p0DbPool={
    if( !mdbManager.contains(proFileName)){
      synchronized{
        if( !mdbManager.contains(proFileName)){
            val dbPool = new C3p0DbPool(proFileName)
          mdbManager += (proFileName->dbPool)
        }
      }
    }
    mdbManager(proFileName)
  }

}
