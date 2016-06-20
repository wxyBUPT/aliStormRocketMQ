package com.alibaba.middleware.race.forLearn;

import java.util.HashMap;

/**
 * Created by xiyuanbupt on 6/20/16.
 */
public class useHashMap {

    public static void main(String[] args){
        HashMap<Long,Double>  hashMap = new HashMap<Long, Double>();
        hashMap.put(1L,0.1);
        hashMap.put(2L,0.2);
        for(long key: hashMap.keySet()){
            System.out.println(key);
        }
        boolean flag =  hashMap.get(3L)==null;
        System.out.println(flag);
    }
}
