package com.liangxl.gmall.realtime.util

import java.util.Properties
import java.io.{BufferedInputStream, File, FileInputStream, InputStream, InputStreamReader}

/** * @Author: liangx
 *
 * @Date: 2021-03-31-16:36 
 * @Description: 读取 properties 配置文件的工具类
 **/
object MyPropertiesUtil {

  def load(properties:String)={
    val prop = new Properties()

    val input = new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(properties))
    prop.load(input)
    prop
  }

  def main0(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))

  }

}
