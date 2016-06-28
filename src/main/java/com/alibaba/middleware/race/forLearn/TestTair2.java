package com.alibaba.middleware.race.forLearn;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;

/**
 * Created by xiyuanbupt on 6/11/16.
 */
public class TestTair2 {
    public static void main(String[] args){
        DataEntry entry = null;
        TairOperatorImpl tairOperator = TairOperatorImpl.getInstance();
        entry  = tairOperator.get("foo");
        System.out.println(entry.toString());
        System.out.println("这已经是在获得foo 参数之后");
        entry = tairOperator.get("ttt");
        System.out.println(entry.toString());

        tairOperator.write("foo","bar1098");
        entry = tairOperator.get("foo");
        System.out.println(entry.toString());

        try{
            Thread.sleep(500);
        }catch (Exception e){
        }
        entry = tairOperator.get("foo");
        System.out.println(entry.toString());
        entry = tairOperator.get("hello");
        tairOperator.write("hello",(double)100);
        entry = tairOperator.get("hello");
        System.out.println(entry.toString());
        try{
            Thread.sleep(1000);
        }catch (Exception e){

        }
        entry = tairOperator.get("hello");
        System.out.println(entry.toString());
    }
}
