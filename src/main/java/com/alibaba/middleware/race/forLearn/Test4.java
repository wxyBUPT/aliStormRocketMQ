package com.alibaba.middleware.race.forLearn;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/9/16.
 */
public class Test4 {
    public  static  List<Map.Entry<Long,Double>> getLadderSumFromMap(Map map){
        List<Map.Entry<Long,Double>> forSoutSumList = new ArrayList<Map.Entry<Long, Double>>(
                map.entrySet()
        );
        Collections.sort(forSoutSumList, new Comparator<Map.Entry<Long, Double>>() {
            @Override
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                return (o1.getKey().compareTo(o2.getKey()));
            }
        });
        //将结果相加
        for(int i = 1;i<forSoutSumList.size();i++){
            Map.Entry<Long,Double> preEntry = forSoutSumList.get(i-1);
            Map.Entry<Long,Double> curEntry = forSoutSumList.get(i);
            curEntry.setValue(preEntry.getValue() + curEntry.getValue());
        }
        return forSoutSumList;
    }

    public static void main(String[] args){
        ConcurrentHashMap<Long,Double> concurrentHashMap = new ConcurrentHashMap<Long, Double>();
        concurrentHashMap.put((long)1000000,3.0);
        concurrentHashMap.put((long)3,3.0);
        concurrentHashMap.put((long)1,1.0);
        concurrentHashMap.put((long)2,2.0);
        for(Map.Entry<Long,Double> entry:concurrentHashMap.entrySet()){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        List<Map.Entry<Long,Double>> list = getLadderSumFromMap(concurrentHashMap);
        concurrentHashMap.clear();
        System.out.println("我要获得当前最大的时间戳,尝试是否能够获得");

        long currentMaxTime = list.get(list.size()-1).getKey();
        System.out.println("当前最大的时间戳是 : " + currentMaxTime);
        for(Map.Entry entry:list){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        System.out.println("下面是清楚数据之后的concurrentHashMap");
        for(Map.Entry entry:concurrentHashMap.entrySet()){
            System.out.println("key : " + entry.getKey());
            System.out.println("value : " + entry.getValue());
        }
        Map<String, String> m0 = new HashMap<String, String>();
        m0.put("a", "a");
        m0.put("b", "b");
        Map<String, String> m1 = new HashMap<String, String>();
        m1.put("c", "c");
        m1.put("b", "b");
        Set<String> s = new HashSet<String>(m0.keySet());
        s.addAll(m1.keySet());
        System.out.println(s);
    }
}
