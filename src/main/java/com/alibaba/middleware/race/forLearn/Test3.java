package com.alibaba.middleware.race.forLearn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/9/16.
 * 用于学习ConcurrentHashMap 复制到 HashMap 的代码
 */
public class Test3 {

    public static ConcurrentHashMap<Integer,String> map = new ConcurrentHashMap<Integer, String>();

    public static void main(String[] args){
        map.put(1,"wang");
        HashMap<Integer,String> forTest = new HashMap<Integer, String>(map);
        System.out.println(map.hashCode());
        System.out.println(forTest.hashCode());
        ConcurrentHashMap<Integer,String> bar = new ConcurrentHashMap<Integer, String>(map);
        System.out.println(bar.hashCode());
        bar.put(2,"xi");
        for(Map.Entry<Integer,String> entry:map.entrySet()){
            System.out.println("key: " + entry.getKey());
            System.out.println("value: " + entry.getValue());
        }
        for(Map.Entry<Integer,String> entry : bar.entrySet()){
            System.out.println("key " + entry.getKey());
            System.out.println("value "  + entry.getValue() );
        }
        System.out.println(bar.hashCode());
        System.out.println(map.hashCode());
        forTest.put(3,"hahhaha");
        for(Map.Entry<Integer,String> entry:forTest.entrySet()){
            System.out.println("key:  " + entry.getKey());
            System.out.println("value:  " + entry.getValue());
        }
        System.out.println(forTest.hashCode());
    }
}
