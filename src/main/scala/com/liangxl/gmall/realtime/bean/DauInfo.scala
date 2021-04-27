package com.liangxl.gmall.realtime.bean

/** * @Author: liangx
 *
 * @Date: 2021-04-01-10:46 
 * @Description:
 **/
case class DauInfo(
                    mid:String,//设备 id
                    uid:String,//用户 id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时间戳
                  ) {}
