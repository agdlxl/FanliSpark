package com.liangxl.gmall.realtime.app;

import java.io.UnsupportedEncodingException;

/**
 * @Author: liangx
 * @Date: 2021-04-26-9:53
 * @Description:
 */
public class test2 {

    public static void main(String[] args) {
        String s = "谧时驹鲋滴";
        if(!(java.nio.charset.Charset.forName("GBK").newEncoder().canEncode(s))){
            System.out.println("乱码");
        }
        else
        {
            System.out.println("GBK");
        }
    }

}
