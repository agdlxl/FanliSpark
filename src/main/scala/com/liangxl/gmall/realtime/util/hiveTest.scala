package com.liangxl.gmall.realtime.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/** * @Author: liangx
 *
 * @Date: 2021-04-06-14:17 
 * @Description:
 **/
object hiveTest {
  def main(args: Array[String]): Unit = {
    val conf = SparkSession.builder().master("local[2]").appName("test2").enableHiveSupport().getOrCreate()

//
//    val conf = new SparkConf()
//      .setAppName("test")
//      .setMaster("local[2]")
//    val sc = SparkContext.getOrCreate(conf)
//
//    val sqlContext = new HiveContext(sc)
    conf.sql("show databases").show(100,false)

  }

}



